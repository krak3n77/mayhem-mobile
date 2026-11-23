#!/usr/bin/env python3
"""
Remote HackRF WebSocket Server

A minimal server that connects to a locally attached HackRF and streams
raw IQ samples to a remote browser via WebSocket.

Control protocol (client -> server):
  JSON messages with the following fields:
  {
    "freqHz": number,       # Frequency in Hz
    "rateHz": number,       # Sample rate in Hz
    "lna": number,          # LNA gain in dB (0-40, multiples of 8)
    "vga": number,          # VGA gain in dB (0-62, multiples of 2)
    "amp": boolean,         # Front-end amplifier enable
    "biasTee": boolean,     # Bias tee enable (not implemented)
    "bbFilterHz": number    # Baseband filter bandwidth in Hz
  }

Lifecycle messages:
  {"type": "start"} - Start streaming IQ
  {"type": "stop"}  - Stop streaming IQ

IQ format (server -> client):
  Binary WebSocket frames containing raw signed 8-bit IQ samples
  (interleaved I, Q bytes)
"""

import asyncio
import json
import sys
import os
import struct
import argparse
import ssl
from typing import Optional, Dict, Any

try:
    import websockets
    from websockets.server import WebSocketServerProtocol
except ImportError:
    print("Error: websockets library not found. Install with: pip install websockets")
    sys.exit(1)

try:
    import hackrf
except ImportError:
    print("Error: hackrf library not found. Install with: pip install hackrf")
    sys.exit(1)

# HackRF parameters
HACKRF_MAX_LNA = 40
HACKRF_MAX_VGA = 62
HACKRF_MAX_RATE = 4_000_000   # Match RemoteHackRF's advertised maximum
HACKRF_MIN_RATE = 1_000_000   # Minimum sample rate in Hz
HACKRF_BB_FILTERS = [1750000, 2500000, 3500000, 5000000, 5500000, 6000000, 7000000, 8000000, 9000000, 10000000,
                    12000000, 14000000, 15000000, 20000000, 24000000, 28000000]

# IQ buffer size (4 MB)
IQ_BUFFER_SIZE = 4 * 1024 * 1024

class HackRFController:
    """Controls the HackRF hardware."""
    
    def __init__(self):
        self.device: Optional[hackrf.HackRF] = None
        self.is_streaming = False
        self.config: Dict[str, Any] = {
            'freqHz': 145_000_000,
            'rateHz': 2_000_000,
            'lna': 16,
            'vga': 20,
            'amp': False,
            'biasTee': False,
            'bbFilterHz': 2_000_000,
        }
    
    def connect(self) -> bool:
        """Open the HackRF device."""
        try:
            self.device = hackrf.HackRF()
            return True
        except Exception as e:
            print(f"Failed to open HackRF: {e}")
            return False
    
    def configure(self, config: Dict[str, Any]) -> bool:
        """Apply a complete RemoteHackRF JSON configuration packet."""
        if not self.device:
            print("Error: HackRF not connected")
            return False
        try:
            required = ("freqHz", "rateHz", "lna", "vga", "amp", "biasTee", "bbFilterHz")
            missing = [key for key in required if key not in config]
            if missing:
                raise ValueError(f"Missing configuration fields: {', '.join(missing)}")

            freq = int(config["freqHz"])
            rate = int(config["rateHz"])
            lna = int(config["lna"])
            vga = int(config["vga"])
            amp = config["amp"]
            bias_tee = config["biasTee"]
            bb_filter_hz = int(config["bbFilterHz"])

            if not 1_000_000 <= freq <= 6_000_000_000:
                raise ValueError("Frequency must be between 1 MHz and 6 GHz")
            if not 1_000_000 <= rate <= 4_000_000:
                raise ValueError("Sample rate must be between 1 and 4 Msps")
            if lna < 0 or lna > 40 or lna % 8:
                raise ValueError("LNA gain must be 0–40 dB in 8 dB steps")
            if vga < 0 or vga > 62 or vga % 2:
                raise ValueError("VGA gain must be 0–62 dB in 2 dB steps")
            if not isinstance(amp, bool) or not isinstance(bias_tee, bool):
                raise ValueError("amp and biasTee must be boolean")
            if bb_filter_hz <= 0:
                raise ValueError("bbFilterHz must be positive")

            was_streaming = self.is_streaming
            if was_streaming:
                self.stop_streaming()

            self.device.set_freq(freq)
            self.device.set_sample_rate(rate)
            self.device.set_baseband_filter_bandwidth(bb_filter_hz)
            self.device.set_lna_gain(lna)
            self.device.set_vga_gain(vga)
            self.device.set_amp(amp)
            self.device.set_antenna_enable(bias_tee)

            self.config = {
                "freqHz": freq, "rateHz": rate, "lna": lna, "vga": vga,
                "amp": amp, "biasTee": bias_tee, "bbFilterHz": bb_filter_hz,
            }
            print(f"Configured: {freq / 1_000_000:.3f} MHz, {rate / 1_000_000:.1f} Msps")
            if was_streaming:
                self.start_streaming()
            return True
        except Exception as e:
            print(f"Configuration error: {e}")
            return False

    def start_streaming(self) -> bool:
        """Start IQ streaming."""
        if not self.device:
            print("Error: HackRF not connected")
            return False
        
        if self.is_streaming:
            print("Warning: Already streaming")
            return True
        
        try:
            self.device.start_rx_mode()
            self.is_streaming = True
            print("Streaming started")
            return True
        except Exception as e:
            print(f"Failed to start streaming: {e}")
            return False
    
    def stop_streaming(self) -> bool:
        """Stop IQ streaming."""
        if not self.is_streaming:
            return True
        
        try:
            self.device.stop_rx_mode()
            self.is_streaming = False
            print("Streaming stopped")
            return True
        except Exception as e:
            print(f"Failed to stop streaming: {e}")
            return False
    
    def close(self):
        """Close the HackRF device."""
        self.stop_streaming()
        if self.device:
            self.device.close()
            self.device = None
            print("HackRF disconnected")


class RemoteHackRFServer:
    """WebSocket server for remote HackRF control."""
    
    def __init__(self, host: str, port: int, ssl_context: Optional[ssl.SSLContext] = None):
        self.host = host
        self.port = port
        self.ssl_context = ssl_context
        self.controller = HackRFController()
        self.client: Optional[WebSocketServerProtocol] = None
        self.iq_task: Optional[asyncio.Task] = None
    
    async def handle_client(self, websocket: WebSocketServerProtocol, path: Optional[str] = None):
        """Handle a new WebSocket client connection."""
        print(f"Client connected from {websocket.remote_address}")
        
        # Reject if another client is connected
        if self.client is not None:
            print("Connection rejected: another client is connected")
            await websocket.close(1013, "Server busy - another client connected")
            return
        
        self.client = websocket
        
        try:
            async for message in websocket:
                if isinstance(message, bytes):
                    # Ignore binary data (shouldn't receive any from client)
                    continue
                
                # Parse JSON message
                try:
                    data = json.loads(message)
                    await self.handle_message(websocket, data)
                except json.JSONDecodeError as e:
                    print(f"Invalid JSON: {e}")
                    await websocket.send(json.dumps({"error": "Invalid JSON"}))
        
        except websockets.exceptions.ConnectionClosed:
            print("Client disconnected")
        finally:
            if self.client == websocket:
                self.client = None
            if self.iq_task is not None:
                self.iq_task.cancel()
                try:
                    await self.iq_task
                except asyncio.CancelledError:
                    pass
                self.iq_task = None
            self.controller.stop_streaming()
    
    async def handle_message(self, websocket: WebSocketServerProtocol, data: Dict[str, Any]):
        """Handle a received message."""
        msg_type = data.get('type', '')
        
        if msg_type == 'start':
            if self.controller.start_streaming():
                # Start IQ streaming task
                if self.iq_task is None:
                    self.iq_task = asyncio.create_task(self.stream_iq())
            else:
                await websocket.send(json.dumps({"error": "Failed to start streaming"}))
        
        elif msg_type == 'stop':
            if self.iq_task:
                self.iq_task.cancel()
                try:
                    await self.iq_task
                except asyncio.CancelledError:
                    pass
                self.iq_task = None
            self.controller.stop_streaming()
        
        else:
            # Configuration update
            if self.controller.configure(data):
                await websocket.send(json.dumps({"status": "configured"}))
            else:
                await websocket.send(json.dumps({"error": "Configuration failed"}))
    
    async def stream_iq(self):
        """Stream IQ samples to the connected client."""
        if not self.controller.device:
            return
        
        print(f"Starting IQ stream to client")
        
        try:
            while True:
                # Read IQ data from HackRF
                # The HackRF returns bytes, interleaved I/Q (signed 8-bit)
                data = self.controller.device.read_rx()
                
                if data:
                    # Send as binary frame
                    if self.client and (getattr(getattr(self.client, 'state', None), 'name', None) == 'OPEN' or getattr(self.client, 'open', False)):
                        await self.client.send(data)
                    else:
                        break
                
                # Small delay to prevent busy-waiting
                await asyncio.sleep(0.001)
        
        except asyncio.CancelledError:
            print("IQ stream cancelled")
        except Exception as e:
            print(f"IQ stream error: {e}")
        finally:
            self.controller.stop_streaming()
            if self.iq_task is asyncio.current_task():
                self.iq_task = None
    
    async def run(self):
        """Run the WebSocket server."""
        # Connect to HackRF
        if not self.controller.connect():
            print("Failed to connect to HackRF. Server will start but won't be able to stream.")
        
        async with websockets.serve(
            self.handle_client,
            self.host,
            self.port,
            ping_interval=20,
            ping_timeout=20,
            ssl=self.ssl_context,
        ):
            scheme = "wss" if self.ssl_context else "ws"
            print(f"Remote HackRF Server listening on {scheme}://{self.host}:{self.port}")
            print("Waiting for client connection...")
            await asyncio.Future()  # Run forever


def main():
    parser = argparse.ArgumentParser(
        description="Remote HackRF WebSocket Server",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Example usage:
  # WSS endpoint (the browser RemoteHackRF client requires wss://)
  python server.py --host 0.0.0.0 --port 8443 --cert fullchain.pem --key privkey.pem

  # Plain WS is for local testing only
  python server.py --host 127.0.0.1 --port 8443

Security notes:
  - Use TLS certificates trusted by the browser.
  - This endpoint has no authentication; keep it behind a trusted network or VPN.
"""
    )
    parser.add_argument('--host', default='0.0.0.0', help='Host to bind to (default: 0.0.0.0)')
    parser.add_argument('--port', type=int, default=8443, help='Port to listen on (default: 8443)')
    parser.add_argument('--cert', help='TLS certificate file (enables WSS; requires --key)')
    parser.add_argument('--key', help='TLS private key file (requires --cert)')
    
    args = parser.parse_args()

    if bool(args.cert) != bool(args.key):
        parser.error('--cert and --key must be provided together to enable WSS')

    ssl_context = None
    if args.cert and args.key:
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        try:
            ssl_context.load_cert_chain(certfile=args.cert, keyfile=args.key)
        except (OSError, ssl.SSLError) as e:
            parser.error(f'Could not load TLS certificate/key: {e}')
    
    # Check for root/sudo privileges (needed for HackRF access on Linux)
    if os.name != 'nt' and os.geteuid() != 0:
        print("Warning: Running without root privileges. HackRF access may fail.")
        print("Consider running with sudo or setting up udev rules.")
    
    server = RemoteHackRFServer(args.host, args.port, ssl_context=ssl_context)
    
    try:
        asyncio.run(server.run())
    except KeyboardInterrupt:
        print("\nShutting down...")
        server.controller.close()
        print("Done.")


if __name__ == '__main__':
    main()