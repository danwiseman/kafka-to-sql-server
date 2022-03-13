namespace ScryfallToSQLConsumer;

using Confluent.Kafka;
using System;
using System.Threading;
using Microsoft.Extensions.Configuration;

public class ScryfallToSQLConsumer
{
    static void Main(string[] args)
    {
        if (args.Length != 1)
        {
            Console.WriteLine("Please provide configuration file");
        }

        IConfiguration configuration = new ConfigurationBuilder()
            .AddIniFile(args[0])
            .Build();

        configuration["group.id"] = "test-scryfall-consumer";
        configuration["auto.offset.reset"] = "earliest";

        const string topic = "scryfall-cards";

        CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true;
            cancellationTokenSource.Cancel();
        };

        using (var consumer = new ConsumerBuilder<string, string>(
                   configuration.AsEnumerable()).Build())
        {
            consumer.Subscribe(topic);
            try
            {
                while (true)
                {
                    var cr = consumer.Consume(cancellationTokenSource.Token);
                    Console.WriteLine(
                        $"Consumed event from topic {topic} with key {cr.Message.Key,-10} and value {cr.Message.Value}");
                }
            }
            catch (OperationCanceledException)
            {

            }
            finally
            {
                consumer.Close();
            }
        }
    }
    
}