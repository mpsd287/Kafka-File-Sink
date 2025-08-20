using System;
using System.IO;
using Confluent.Kafka;
using Microsoft.Extensions.Configuration;

var configuration = new ConfigurationBuilder()
    .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
    .Build();

var kafkaConfig = new ConsumerConfig
{
    BootstrapServers = configuration["Kafka:BootstrapServers"],
    GroupId = configuration["Kafka:GroupId"],
    AutoOffsetReset = AutoOffsetReset.Earliest
};

var topic = configuration["Kafka:Topic"] ?? throw new InvalidOperationException("Topic not configured");
var outputFile = configuration["Output:FilePath"] ?? "output.txt";

var outputDir = Path.GetDirectoryName(outputFile);
if (!string.IsNullOrEmpty(outputDir))
{
    Directory.CreateDirectory(outputDir);
}

IMessageHandler handler = new MessageHandler();

using var consumer = new ConsumerBuilder<string, string>(kafkaConfig).Build();
consumer.Subscribe(topic);

var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    cts.Cancel();
};

Console.WriteLine("Listening for messages. Press Ctrl+C to exit.");

try
{
    while (!cts.IsCancellationRequested)
    {
        var result = consumer.Consume(cts.Token);
        if (handler.ShouldProcess(result.Message.Value))
        {
StreamWriter? writer = null;
try
{
    writer = new StreamWriter(outputFile, append: true);
    while (!cts.IsCancellationRequested)
    {
        var result = consumer.Consume(cts.Token);
        if (handler.ShouldProcess(result.Message.Value))
        {
            writer.WriteLine(result.Message.Value);
            writer.Flush();
            Console.WriteLine($"Processed message: {result.Message.Value}");
        }
        else
        {
            Console.WriteLine("Ignored message");
        }
    }
}
catch (OperationCanceledException)
{
    // graceful shutdown
}
finally
{
    consumer.Close();
}
