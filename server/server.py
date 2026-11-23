
    
  
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

