# Remote HackRF WebSocket Server

A minimal reference implementation of a WebSocket server that bridges a locally-attached
HackRF to remote browser clients. All DSP (waterfall, spectrum, demodulation, decoding)
remains in the browser; the server only handles hardware access and IQ transport.

## Requirements

### Hardware
- HackRF One, HackRF Pro, HackRF Jawbreaker, or rad1o (any device supported by `libhackrf`)
- USB port accessible to the server process

### Software
- Python 3.8 or later
- `websockets` library: `pip install websockets`
- `hackrf` library: `pip install hackrf` (uses `pyusb` and `libhackrf`)

### System Dependencies (Linux)
```bash
# Install libhackrf and its dependencies
sudo apt-get install hackrf libhackrf-dev libusb-1.0-0-dev

# Set up udev rules (if not provided by the package)
# Add /etc/udev/rules.d/53-hackrf.rules:
SUBSYSTEM=="usb", ATTRS{idVendor}=="1d50", ATTRS{idProduct}=="6089", MODE="0664", GROUP="plugdev"
SUBSYSTEM=="usb", ATTRS{idVendor}=="1d50", ATTRS{idProduct}=="604b", MODE="0664", GROUP="plugdev"
SUBSYSTEM=="usb", ATTRS{idVendor}=="1d50", ATTRS{idProduct}=="cc15", MODE="0664", GROUP="plugdev"

# Add your user to the plugdev group
sudo usermod -a -G plugdev $USER

# Re-plug the HackRF after adding rules
```

## Installation

```bash
# Install Python dependencies
pip install websockets hackrf

# (Optional) For system-wide installation
sudo pip install websockets hackrf
```

## Usage

### Basic Start

```bash
python server.py
```

This starts the server on all interfaces at port 8443.

### Command-Line Options

```
usage: server.py [-h] [--host HOST] [--port PORT]

Remote HackRF WebSocket Server

optional arguments:
  -h, --help     Show this help message
  --host HOST    Host to bind to (default: 0.0.0.0)
  --port PORT    Port to listen on (default: 8443)
```

### Examples

```bash
# Listen on all interfaces (less secure)
python server.py --host 0.0.0.0 --port 8443

# Listen on localhost only (more secure)
python server.py --host 127.0.0.1 --port 8443

# Use the standard HTTPS port with wss
python server.py --host 0.0.0.0 --port 443
```

## WebSocket Protocol

### Control Messages (Client → Server)

**Configuration Message:**
```json
{
  "freqHz": 145000000,
  "rateHz": 2000000,
  "lna": 16,
  "vga": 20,
  "amp": false,
  "biasTee": false,
  "bbFilterHz": 2000000
}
```

**Lifecycle Messages:**
```json
{"type": "start"}   # Begin streaming IQ
{"type": "stop"}    # Stop streaming IQ
```

### IQ Data (Server → Client)

Binary WebSocket frames containing raw signed 8-bit IQ samples:
- Interleaved: I, Q, I, Q, ...
- Range: -128 to +127
- Sample order: Same as local HackRF driver

## Configuration

### Browser Client Settings

In the Mayhem Mobile web app:

1. Open Settings
2. Scroll to "RADIO SOURCE"
3. Select **REMOTE**
4. Enter the WebSocket URL (e.g., `wss://radio.example.com:8443`)
5. Click **CONNECT**

### URL Schemes

- `ws://` - Unencrypted WebSocket (suitable for local development)
- `wss://` - Encrypted WebSocket (required for HTTPS pages)

**Important:** If your app is served over HTTPS (e.g., GitHub Pages), browsers will
block insecure `ws://` connections. Use `wss://` with a TLS terminator.

## Security Considerations

### Warning

This server provides **full control of your HackRF hardware** to any connected client.
Improper deployment can:

- Allow unauthorized access to your radio hardware
- Permit transmission on any frequency (if TX support is added)
- Expose your system to network-based attacks

### Recommended Practices

1. **Never expose directly to the internet** - Use SSH tunneling or a reverse proxy
2. **Bind to localhost** for local development: `--host 127.0.0.1`
3. **Use TLS termination** - Place behind nginx/Apache with Let's Encrypt
4. **Limit network access** - Use firewall rules if binding to 0.0.0.0
5. **Consider authentication** - Extend the server to add auth if needed

### Example: SSH Tunnel

```bash
# On your remote server
python server.py --host 127.0.0.1 --port 8443

# On your local machine
ssh -L 8443:localhost:8443 user@remote-server

# In Mayhem Mobile, connect to: ws://localhost:8443
```

### Example: nginx Reverse Proxy

```nginx
server {
    listen 443 ssl http2;
    server_name radio.example.com;

    ssl_certificate /etc/ssl/certs/radio.crt;
    ssl_certificate_key /etc/ssl/private/radio.key;

    location / {
        proxy_pass http://127.0.0.1:8443;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host $host;
        proxy_read_timeout 86400;
    }
}
```

## Supported Parameters

| Parameter | Type | Range | Notes |
|-----------|------|-------|-------|
| `freqHz` | number | 1_000_000 - 6_000_000_000 | Frequency in Hz |
| `rateHz` | number | 1_000_000 - 20_000_000 | Sample rate in Hz |
| `lna` | number | 0 - 40, multiples of 8 | LNA gain in dB |
| `vga` | number | 0 - 62, multiples of 2 | VGA gain in dB |
| `amp` | boolean | true/false | Front-end amplifier |
| `biasTee` | boolean | true/false | Antenna bias power |
| `bbFilterHz` | number | See BB_FILTER_BW | Baseband filter bandwidth |

## Troubleshooting

### "HackRF not found" Error

1. Check device connection: `lsusb | grep 1d50`
2. Verify permissions: `ls -l /dev/bus/usb/*/`
3. Check udev rules (Linux): `/etc/udev/rules.d/53-hackrf.rules`
4. Re-plug the device after modifying rules

### "Port already in use" Error

Another service may be using the port:

```bash
# Find what's using the port
lsof -i :8443
# or
netstat -tlnp | grep :8443
```

### WebSocket Connection Failed

1. Verify server is running: `ps aux | grep server.py`
2. Check firewall rules
3. Test with `wscat` or similar tool
4. For browser connection, verify URL scheme (ws vs wss)

### No IQ Data Received

1. Check server logs for streaming messages
2. Verify client sent `{"type": "start"}`
3. Check HackRF LED (should be on when streaming)
4. Verify sample rate and frequency are within device limits

### Transmission Not Working

TX is explicitly disabled in the remote driver. The server does not support
transmission - it only handles receive mode.

## Known Limitations

1. **Single client only** - Multiple simultaneous connections are rejected
2. **No authentication** - Any network-accessible client can control the radio
3. **No TX support** - Only receive mode is implemented
4. **IQ buffer size fixed** - 4 MB buffer may need adjustment for high-rate streams

## License

Part of Mayhem Mobile - same license as the main project.
