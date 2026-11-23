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
HACKRF_MAX_RATE = 20_000_000  # Maximum sample rate in Hz
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
        """Apply radio configuration."""
        if not self.device:
            print("Error: HackRF not connected")
            return False
        
        try:
            # Stop streaming if currently running
            if self.is_streaming:
                self.stop_streaming()
            
            # Apply configuration in the correct order
            # 1. Set frequency
            freq = config.get('freqHz', 145_000_000)
            self.device.set_freq(freq)
            print(f"Frequency: {freq / 1_000_000:.3f} MHz")
            
            # 2. Set sample rate
            rate = config.get('rateHz', 2_000_000)
            self.device.set_sample_rate(rate)
            self.device.set_baseband_filter_bandwidth(min(rate, max(HACKRF_BB_FILTERS)))
            print(f"Sample rate: {rate / 1_000_000:.1f} Msps")
            
            # 3. Set gains
            lna = min(HACKRF_MAX_LNA, max(0, config.get('lna', 16)))
            vga = min(HACKRF_MAX_VGA, max(0, config.get('vga', 20)))
            self.device.set_lna_gain(lna)
            self.device.set_vga_gain(vga)
            print(f"Gains: LNA {lna} dB, VGA {vga} dB")
            
            # 4. Set amplifier
            amp = config.get('amp', False)
            self.device.set_amp(amp)
            print(f"Amplifier: {'ON' if amp else 'OFF'}")
            
            # 5. Set antenna power (bias tee)
            bias_tee = config.get('biasTee', False)
            self.device.set_antenna_enable(bias_tee)
            print(f"Bias Tee: {'ON' if bias_tee else 'OFF'}")
            
            # Update config
            self.config.update(config)
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
    
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.controller = HackRFController()
        self.client: Optional[WebSocketServerProtocol] = None
        self.iq_task: Optional[asyncio.Task] = None
    
    async def handle_client(self, websocket: WebSocketServerProtocol, path: str):
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
        
        elif msg_type in ('freqHz', 'rateHz', 'lna', 'vga', 'amp', 'biasTee', 'bbFilterHz'):
            # Legacy single-field messages - not used by current client
            pass
        
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
                    if self.client and self.client.open:
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
        ):
            print(f"Remote HackRF Server listening on ws://{self.host}:{self.port}")
            print("Waiting for client connection...")
            await asyncio.Future()  # Run forever


def main():
    parser = argparse.ArgumentParser(
        description="Remote HackRF WebSocket Server",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Example usage:
  # Listen on all interfaces, port 8443
  python server.py --host 0.0.0.0 --port 8443

  # Listen on localhost only (more secure)
  python server.py --host 127.0.0.1 --port 8443

Security notes:
  - For production use, place this server behind a reverse proxy with TLS
  - Consider binding to 127.0.0.1 and using SSH tunneling
  - Never expose this server directly to untrusted networks
"""
    )
    parser.add_argument('--host', default='0.0.0.0', help='Host to bind to (default: 0.0.0.0)')
    parser.add_argument('--port', type=int, default=8443, help='Port to listen on (default: 8443)')
    
    args = parser.parse_args()
    
    # Check for root/sudo privileges (needed for HackRF access on Linux)
    if os.name != 'nt' and os.geteuid() != 0:
        print("Warning: Running without root privileges. HackRF access may fail.")
        print("Consider running with sudo or setting up udev rules.")
    
    server = RemoteHackRFServer(args.host, args.port)
    
    try:
        asyncio.run(server.run())
    except KeyboardInterrupt:
        print("\nShutting down...")
        server.controller.close()
        print("Done.")


if __name__ == '__main__':
    main()
