# Kafka File Sink

This repository contains a simple .NET console application that consumes messages from a Kafka topic and writes selected messages to a file.

## Configuration

All settings are stored in `appsettings.json`.

- `Kafka:BootstrapServers` – Kafka broker list
- `Kafka:Topic` – topic to subscribe to
- `Kafka:GroupId` – consumer group id
- `Output:FilePath` – path to the output file where accepted messages are appended

## Build and Run

Ensure the .NET SDK is installed.

```bash
dotnet build
dotnet run
```
