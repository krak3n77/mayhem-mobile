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

