using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;

class Program
{
    static readonly JsonSerializerConfig jsonSerializerConfig = new JsonSerializerConfig
    {
        BufferBytes = 100,
    };

    static readonly CachedSchemaRegistryClient schemaRegistryClient = new(
        new SchemaRegistryConfig
        {
            Url = "http://localhost:8081",
        });
    
    static readonly string bootstrapServers = "192.168.0.246:19092";
    static readonly string topic = "test-topic";

    public static async Task Main(string[] args)
    {
        await produce();
        consume();
    }

    private static async Task produce()
    {
        var config = new ProducerConfig { BootstrapServers = bootstrapServers };

        // If serializers are not specified, default serializers from
        // `Confluent.Kafka.Serializers` will be automatically used where
        // available. Note: by default strings are encoded as UTF8.
        using (var p = new ProducerBuilder<string, StructuredMessage>(config)
            .SetValueSerializer(new JsonSerializer<StructuredMessage>(
                schemaRegistryClient, jsonSerializerConfig))
            .Build())
        {
            try
            {
                var thing = new StructuredMessage
                {
                    Id = 1,
                    Text = "Hello Kafka!",
                    Timestamp = DateTime.UtcNow,
                };

                var message = new Message<string, StructuredMessage> { Key = "todo", Value = thing };

                var dr = await p.ProduceAsync(topic, message);

                Console.WriteLine($"Delivered '{dr.Value}' to '{dr.TopicPartitionOffset}'");
            }
            catch (ProduceException<Null, string> e)
            {
                Console.WriteLine($"Delivery failed: {e.Error.Reason}");
            }
        }
    }

    private static void consume()
    {
        var conf = new ConsumerConfig
        {
            GroupId = "test-consumer-group",
            BootstrapServers = bootstrapServers,
            // Note: The AutoOffsetReset property determines the start offset in the event
            // there are no offsets saved for the consumer group for the topic/partitions of interest.
            // Earliest: automatically reset the offset to the earliest offset
            // Latest: automatically reset the offset to the latest offset
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        using (var c = new ConsumerBuilder<string, StructuredMessage>(conf)
            .SetKeyDeserializer(Deserializers.Utf8)
            .SetValueDeserializer(new JsonDeserializer<StructuredMessage>().AsSyncOverAsync())
            .Build())
        {
            c.Subscribe(topic);

            CancellationTokenSource cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) => {
                e.Cancel = true; // prevent the process from terminating.
                cts.Cancel();
            };

            try
            {
                while (true)
                {
                    try
                    {
                        var cr = c.Consume(cts.Token);
                        // Console.WriteLine($"Consumed message '{cr.Message.Value}' at: '{cr.TopicPartitionOffset}'.");
                        Console.WriteLine($"Consumed message '{cr.Message.Value.Id} | {cr.Message.Value.Text} | {cr.Message.Value.Timestamp}' at: '{cr.TopicPartitionOffset}'");
                    }
                    catch (ConsumeException e)
                    {
                        Console.WriteLine($"Error occured: {e.Error.Reason}");
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // Ctrl+C was pressed.
            }
            finally
            {
                c.Close();
            }
        }
    }
}