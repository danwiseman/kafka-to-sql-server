using System.Text.Json;

namespace ScryfallToSQLConsumer;

using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Newtonsoft.Json;
using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;

public class ScryfallToSQLConsumer
{

    public static void Main(string[] args)
    {
        if (args.Length != 1)
        {
            Console.WriteLine("Usage: .. configuration-file");
            return;
        }
        
        IConfiguration configuration = new ConfigurationBuilder()
            .AddIniFile(args[0])
            .Build();

        var brokerList = configuration["bootstrap.servers"];
        var topics  = configuration["topic.name"];
        var schemaRegistryUrl = configuration["schema.registry.url"];
        
        Console.WriteLine($"Started consumer, Ctrl-C to stop consuming");

        CancellationTokenSource cts = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) => {
            e.Cancel = true; // prevent the process from terminating.
            cts.Cancel();
        };
        
        Run_Consume(brokerList, topics, cts.Token, schemaRegistryUrl);
        
        // var consumeTask = Task.Run(() =>
        // {
        //     using (var consumer =
        //         new ConsumerBuilder<Null, Card>(consumerConfig)
        //             .SetValueDeserializer(new JsonDeserializer<Card>().AsSyncOverAsync())
        //             .SetErrorHandler((_, e) => Console.WriteLine($"Error: {e.Reason}"))
        //             .Build())
        //     {
        //         consumer.Subscribe(configuration["topic.name"]);
        //
        //         try
        //         {
        //             while (true)
        //             {
        //                 try
        //                 {
        //                     var cr = consumer.Consume(cts.Token);
        //                     ConsumeScryfallRecord(cr.Message.Value);
        //                 }
        //                 catch (ConsumeException e)
        //                 {
        //                     Console.WriteLine($"Consume error: {e.Error.Reason}");
        //                 }
        //             }
        //         }
        //         catch (OperationCanceledException)
        //         {
        //             consumer.Close();
        //         }
        //     }
        // });
        
        cts.Cancel();
    }

    /// <summary>
    ///     Modified from the Confluent Kafka examples
    ///     https://github.com/confluentinc/confluent-kafka-dotnet/blob/master/examples/Consumer/Program.cs
    /// </summary>
    /// <param name="brokerList"></param>
    /// <param name="topics"></param>
    /// <param name="cancellationToken"></param>
    public static void Run_Consume(string brokerList, string topic, CancellationToken cancellationToken, string schemaRegistryUrl)
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = brokerList,
                GroupId = "csharp-consumer",
                EnableAutoCommit = false,
                StatisticsIntervalMs = 5000,
                SessionTimeoutMs = 6000,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnablePartitionEof = true,
                PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky
            };
            
            var schemaRegistryConfig = new SchemaRegistryConfig
            {
                // Note: you can specify more than one schema registry url using the
                // schema.registry.url property for redundancy (comma separated list). 
                // The property name is not plural to follow the convention set by
                // the Java implementation.
                Url = schemaRegistryUrl
            };

            const int commitPeriod = 5;
            
            // var jsonOptions = new JsonSerializerOptions { WriteIndented = true };

            
            using (var consumer = new ConsumerBuilder<Ignore, string>(config)
                // Note: All handlers are called on the main .Consume thread.
                .SetErrorHandler((_, e) => Console.WriteLine($"Error: {e.Reason}"))
                .SetStatisticsHandler((_, json) => Console.WriteLine($"Statistics: {json}"))
                .SetPartitionsAssignedHandler((c, partitions) =>
                {
                    // Since a cooperative assignor (CooperativeSticky) has been configured, the
                    // partition assignment is incremental (adds partitions to any existing assignment).
                    Console.WriteLine(
                        "Partitions incrementally assigned: [" +
                        string.Join(',', partitions.Select(p => p.Partition.Value)) +
                        "], all: [" +
                        string.Join(',', c.Assignment.Concat(partitions).Select(p => p.Partition.Value)) +
                        "]");

                    // Possibly manually specify start offsets by returning a list of topic/partition/offsets
                    // to assign to, e.g.:
                    // return partitions.Select(tp => new TopicPartitionOffset(tp, externalOffsets[tp]));
                })
                .SetPartitionsRevokedHandler((c, partitions) =>
                {
                    // Since a cooperative assignor (CooperativeSticky) has been configured, the revoked
                    // assignment is incremental (may remove only some partitions of the current assignment).
                    var remaining = c.Assignment.Where(atp => partitions.Where(rtp => rtp.TopicPartition == atp).Count() == 0);
                    Console.WriteLine(
                        "Partitions incrementally revoked: [" +
                        string.Join(',', partitions.Select(p => p.Partition.Value)) +
                        "], remaining: [" +
                        string.Join(',', remaining.Select(p => p.Partition.Value)) +
                        "]");
                })
                .SetPartitionsLostHandler((c, partitions) =>
                {
                    // The lost partitions handler is called when the consumer detects that it has lost ownership
                    // of its assignment (fallen out of the group).
                    Console.WriteLine($"Partitions were lost: [{string.Join(", ", partitions)}]");
                })
                .Build())
            {
                consumer.Subscribe(topic);

                try
                {
                    while (true)
                    {
                        try
                        {
                            var consumeResult = consumer.Consume(cancellationToken);

                            if (consumeResult.IsPartitionEOF)
                            {
                                Console.WriteLine(
                                    $"Reached end of topic {consumeResult.Topic}, partition {consumeResult.Partition}, offset {consumeResult.Offset}.");

                                continue;
                            }

                            ConsumeScryfallRecord(consumeResult.Message.Value);
                            //Console.WriteLine($"{consumeResult.Message.Value}");
                            
                            if (consumeResult.Offset % commitPeriod == 0)
                            {
                                // The Commit method sends a "commit offsets" request to the Kafka
                                // cluster and synchronously waits for the response. This is very
                                // slow compared to the rate at which the consumer is capable of
                                // consuming messages. A high performance application will typically
                                // commit offsets relatively infrequently and be designed handle
                                // duplicate messages in the event of failure.
                                try
                                {
                                    consumer.Commit(consumeResult);
                                }
                                catch (KafkaException e)
                                {
                                    Console.WriteLine($"Commit error: {e.Error.Reason}");
                                }
                            }
                        }
                        catch (ConsumeException e)
                        {
                            Console.WriteLine($"Consume error: {e.Error.Reason}");
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    Console.WriteLine("Closing consumer.");
                    consumer.Close();
                }
            }
            
           
        }
    
    /// <summary>
    /// Takes a Card and Inserts into a SQL database
    /// </summary>
    /// <param name="consumedCard"></param>
    private static void ConsumeScryfallRecord(string consumedValue)
    {
        Card consumedCard = Card.FromJson(consumedValue);

        Console.WriteLine($"{consumedCard.Name} consumed");

    }
}