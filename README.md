# GoAsync Downloader

GoAsync Downloader is a command-line file downloader written in Go. It downloads files in chunks, processes chunks in parallel, keeps resume state on disk, and shows progress bars for every file.

## Features

- Parallel chunk downloading with a fixed worker pool.
- Resume support via `.progress` files.
- Retry logic for temporary network failures.
- Multi-file downloads in one run.
- Multi-progress terminal output with file name, percentage, downloaded size, total size, and speed.
- Graceful shutdown on `Ctrl+C`: no new chunks are started, active chunks finish, and state is saved.

## Requirements

- Go 1.25 or newer.

## Build

```bash
go build -o goasync .
```

## Usage

Download one file:

```bash
./goasync ./downloads https://example.com/data.zip
```

Download several files:

```bash
./goasync ./downloads \
  https://example.com/data.zip \
  https://example.com/report.csv \
  https://example.com/images.tar
```

The first argument is the output directory. Every following argument is a file URL.

## Resume Downloads

The downloader stores state in `*.progress` files next to downloaded files. If a download is interrupted, run the same command again and already completed chunks will be skipped.

```bash
./goasync ./downloads https://example.com/data.zip
```

## Graceful Shutdown

Press `Ctrl+C` to stop safely. The program stops scheduling new chunks, waits for active chunks to finish, saves the current state, and exits.

## Development

Format and check the project:

```bash
gofmt -w .
go test ./...
```
