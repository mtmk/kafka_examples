using System;
using System.Collections.Generic;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;
using Google.Protobuf;
using Grpc.Core;

namespace KafkaSync
{
    class Program
    {
        static void Main(string[] args)
        {
            if (Environment.CommandLine.Contains("-server"))
            {
                Server();
            }
            else if (Environment.CommandLine.Contains("-client"))
            {
                Client();
            }
            else if (Environment.CommandLine.Contains("-consumer"))
            {
                var cancel = new CancellationTokenSource();
                var cancelToken = cancel.Token;


                var t1 = Task.Run(()=>ConsumerCmd("example1", cancelToken));
                var t2 = Task.Run(()=>ConsumerCmd("example1", cancelToken));
                var t3 = Task.Run(()=>ConsumerCmd("example1", cancelToken));
                var t4 = Task.Run(()=>ConsumerCmd("example2", cancelToken));
                var t5 = Task.Run(()=>ConsumerCmd("example2", cancelToken));
                var t6 = Task.Run(()=>ConsumerCmd("example2", cancelToken));
                var t7 = Task.Run(()=>ConsumerCmd("example3", cancelToken));
                var t8 = Task.Run(()=>ConsumerCmd("example3", cancelToken));
                var t9 = Task.Run(()=>ConsumerCmd("example3", cancelToken));

                Console.WriteLine("Press any key to exit...");
                Console.ReadKey();

                Console.WriteLine("Cancelling..");
                cancel.Cancel();

                Task.WaitAll(t1, t2, t3, t4, t5, t6, t7, t8, t9);

                Console.WriteLine("Exit.");
            }
            else if (Environment.CommandLine.Contains("-producer"))
            {
                ProducerCmd();
            }
        }

        private static void ProducerCmd()
        {
            string brokerList = "localhost:9092";
            string topicName = "test2";

            var config = new Dictionary<string, object>
            {
                { "bootstrap.servers", brokerList },
                { "socket.blocking.max.ms", 1 },
                { "linger.ms", 0 },
                { "socket.nagle.disable", true },
            };

            var producer = new Producer<string, string>(config, new StringSerializer(Encoding.UTF8),
                new StringSerializer(Encoding.UTF8));

            while (true)
            {
                //Console.ReadLine();
                var deliveryReport = producer.ProduceAsync(topicName, Guid.NewGuid().ToString(),
                    Guid.NewGuid().ToString() + Guid.NewGuid() + Guid.NewGuid() + Guid.NewGuid() + Guid.NewGuid() + Guid.NewGuid() + Guid.NewGuid() + Guid.NewGuid()
                    ).Result;
                if (deliveryReport.Error.Code != ErrorCode.NoError)
                    Console.WriteLine($"failed to deliver message: {deliveryReport.Error.Reason}");
            }
        }

        static int p = 0;

        private static void ConsumerCmd(string topic, CancellationToken cancelToken)
        {
            string brokes = "localhost:9092";
            //string topic = "test2";

            Channel channel = new Channel("10.154.0.2:55001", ChannelCredentials.Insecure);

            var client = new Replica.ReplicaClient(channel);

            //Console.WriteLine("Copied: " + reply.Message);


            var config = new Dictionary<string, object>
            {
                { "bootstrap.servers", brokes },
                { "group.id", "csharp-consumer" },
                { "enable.auto.commit", true },  // this is the default
                { "auto.commit.interval.ms", 5000 },
                //{ "statistics.interval.ms", 60000 },
                { "session.timeout.ms", 6000 },
                { "auto.offset.reset", "smallest" }
            };

            var consumer = new Consumer<byte[], byte[]>(config, new ByteArrayDeserializer(), new ByteArrayDeserializer());
            consumer.OnMessage += (_, msg) =>
            {
                while (true)
                {
                    try
                    {
                        var reply = client.CopyData(new CopyRequest
                        {
                            Topic = msg.Topic,
                            Key = ByteString.CopyFrom(msg.Key ?? new byte[0]),
                            Value = ByteString.CopyFrom(msg.Value ?? new byte[0]),
                            Partition = msg.Partition,
                            Offset = msg.Offset,
                        });

                        if (!reply.Success) throw new Exception(reply.Message);
                        break;
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e);
                        Thread.Sleep(3000);
                    }
                }
                // Console.WriteLine($"Topic: {msg.Topic} Partition: {msg.Partition} Offset: {msg.Offset} {msg.Value}");
            };

            consumer.OnPartitionEOF += (_, end)
                => Console.WriteLine($"Reached end of topic {end.Topic} partition {end.Partition}, next message will be at offset {end.Offset}");

            consumer.OnError += (_, error)
                => Console.WriteLine($"Error: {error}");

            // Raised on deserialization errors or when a consumed message has an error != NoError.
            consumer.OnConsumeError += (_, msg)
                => Console.WriteLine($"Error consuming from topic/partition/offset {msg.Topic}/{msg.Partition}/{msg.Offset}: {msg.Error}");

//            consumer.OnOffsetsCommitted += (_, commit)
//                =>
//            {
//                Console.WriteLine(
//                    commit.Error
//                        ? $"Failed to commit offsets: {commit.Error}"
//                        : $"Successfully committed offsets:]");
//                foreach (var offset in commit.Offsets)
//                {
//                    Console.WriteLine($"  {offset}");
//                }
//            };

            //var s = new object();
            // Raised when the consumer is assigned a new set of partitions.
            consumer.OnPartitionsAssigned += (_, partitions) =>
            {
                //lock (s)
                {
                    var x = Interlocked.Increment(ref p);
                    Console.WriteLine($"Assigned partitions({x}): [{string.Join(", ", partitions)}], member id: {consumer.MemberId}");
                }
                // If you don't add a handler to the OnPartitionsAssigned event,
                // the below .Assign call happens automatically. If you do, you
                // must call .Assign explicitly in order for the consumer to 
                // start consuming messages.
                consumer.Assign(partitions);
            };
            consumer.OnPartitionsRevoked += (_, partitions) =>
            {
                //lock (s)
                {
                    var x = Interlocked.Decrement(ref p);
                    Console.WriteLine($"Revoked partitions({x}): [{string.Join(", ", partitions)}]");
                }
                // If you don't add a handler to the OnPartitionsRevoked event,
                // the below .Unassign call happens automatically. If you do, 
                // you must call .Unassign explicitly in order for the consumer
                // to stop consuming messages from it's previously assigned 
                // partitions.
                consumer.Unassign();
            };

            // consumer.OnStatistics += (_, json) => Console.WriteLine($"Statistics: {json}");

            consumer.Subscribe(topic);

            Console.WriteLine($"Subscribed to: [{string.Join(", ", consumer.Subscription)}]");

            Task.Run(() =>
            {
                while (!cancelToken.IsCancellationRequested)
                {
                    consumer.Poll(TimeSpan.FromMilliseconds(100));
                }
                Console.WriteLine("Unsub..");
                consumer.Unsubscribe();
                Console.WriteLine("Unsub..done");
            });

            cancelToken.WaitHandle.WaitOne();

            Console.WriteLine("Spin..");

            var spin = SpinWait.SpinUntil(() => Interlocked.CompareExchange(ref p, 0, 0) == 0, TimeSpan.FromSeconds(300));

            if (!spin)
            {
                Console.WriteLine("Warning: Did not unassigned in time!");
            }
            else
            {
                Console.WriteLine("All unassigned in time.");
            }

            channel.ShutdownAsync().Wait();
        }

        private static void Client()
        {
            Channel channel = new Channel("10.154.0.2:55001", ChannelCredentials.Insecure);

            var client = new Replica.ReplicaClient(channel);

            var reply = client.CopyData(new CopyRequest { Topic = "test1" });
            Console.WriteLine("Copied: " + reply.Message);

            channel.ShutdownAsync().Wait();
            Console.WriteLine("Press any key to exit...");
            Console.ReadKey();
        }

        private static void Server()
        {
            var port = 55001;
            Server server = new Server
            {
                Services = { Replica.BindService(new KafkaSyncImpl()) },
                Ports = { new ServerPort("0.0.0.0", port, ServerCredentials.Insecure) }
            };
            server.Start();

            Console.WriteLine("Greeter server listening on port " + port);
            Console.WriteLine("Press any key to stop the server...");
            Console.ReadKey();

            server.ShutdownAsync().Wait();
        }
    }

    class KafkaSyncImpl : Replica.ReplicaBase
    {
        private readonly Producer<byte[], byte[]> _producer;

        public KafkaSyncImpl()
        {
            string brokerList = "localhost:9092";

            var config = new Dictionary<string, object>
            {
                { "bootstrap.servers", brokerList },
                { "socket.blocking.max.ms", 1 },
                //{ "linger.ms", 0 },
                //{ "socket.nagle.disable", true },
            };

            _producer = new Producer<byte[], byte[]>(config, new ByteArraySerializer(), new ByteArraySerializer());
        }

        public override async Task<CopyReply> CopyData(CopyRequest request, ServerCallContext context)
        {
            //Console.WriteLine($"Got request: {request.Topic} ({context.Peer})");

            try
            {
                var report = await _producer.ProduceAsync(request.Topic, request.Key.ToByteArray(), request.Value.ToByteArray(), request.Partition);

                return new CopyReply
                {
                    Success = report.Error.Code == ErrorCode.NoError,
                    Message = report.Error.Code == ErrorCode.NoError ? "OK" : report.Error.Reason,
                };
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }
        }
    }
}
