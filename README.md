# Bluesky Linux Monitor

A high-performance Rust application that monitors the Bluesky public firehose for mentions of Linux and popular Linux distributions, displaying them in a real-time web interface.

## Features

- **Real-time monitoring**: Connects to Bluesky's public firehose via WebSocket
- **Smart filtering**: Detects mentions of "Linux" and 20+ popular distributions (Ubuntu, Debian, Fedora, Arch, etc.)
- **Persistent storage**: Saves the last 250 posts to disk
- **Auto-refreshing web UI**: Clean, dark-themed interface on port 2287
- **Post details**: Displays text, images, links, and direct links to Bluesky
- **Automatic reconnection**: Handles network interruptions gracefully

## Tracked Keywords

The monitor searches for mentions of:
- Linux (general)
- Ubuntu, Debian, Fedora, Arch, Manjaro, Mint
- OpenSUSE, Gentoo, Red Hat, CentOS, Rocky, AlmaLinux
- Kali, Parrot, Elementary, Zorin
- Pop!_OS, EndeavourOS, NixOS

All matching is case-insensitive.

## Requirements

- Rust 1.70 or later
- Linux system (tested on standard distributions)
- Internet connection for Bluesky firehose

## Building

```bash
cargo build --release
```

The compiled binary will be at `target/release/bluesky-linux-monitor`

## Running

```bash
cargo run --release
```

Or run the compiled binary directly:

```bash
./target/release/bluesky-linux-monitor
```

The application will:
1. Load any previously saved posts from `posts.json`
2. Connect to the Bluesky firehose
3. Start the web server on http://localhost:2287

## Usage

1. Start the application
2. Open your browser to http://localhost:2287
3. The page will auto-refresh every 10 seconds
4. Posts appear newest first, with up to 250 posts stored

## Architecture

- **WebSocket client**: Connects to Bluesky's Jetstream firehose
- **Async runtime**: Powered by Tokio for high performance
- **Web server**: Axum-based HTTP server
- **Storage**: JSON file persistence
- **Thread-safe**: Shared state using Arc<RwLock>

## Data Storage

Posts are stored in `posts.json` in the current directory. The file is automatically created and updated as new posts arrive. A maximum of 250 posts are kept.

## Network

The application binds to `0.0.0.0:2287`, making it accessible from:
- http://localhost:2287
- http://127.0.0.1:2287
- Your machine's IP address on port 2287

## Note on Authentication

This application monitors the **public** Bluesky firehose, which does not require authentication. While Bluesky supports authenticated operations via app passwords, they are not needed for this read-only monitoring use case.

## License

Open source - use freely
