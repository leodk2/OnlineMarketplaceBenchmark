using System.Buffers;
using System.ComponentModel;
using Common.Services;
using Common.Streaming;
using Common.Workload;
using Common.Workload.Metrics;
using Confluent.Kafka;
using MessagePack;

namespace Styx.Workload;

public class TransactionMarkDeserializer : IDeserializer<TransactionMark>
{
    public TransactionMark Deserialize(ReadOnlySpan<byte> data,
        bool isNull,
        SerializationContext context)
    {
        if (isNull)
            return null!;
        ReadOnlySequence<byte> bytes = new ReadOnlySequence<byte>([.. data]);
        var reader = new MessagePackReader(bytes);
        if (reader.NextMessagePackType != MessagePackType.Map)
        {
            Console.WriteLine("Styx abort on {0}: {1}", context.Topic, reader.ReadString());
            return null!;
        }

        try
        {
            return MessagePackSerializer.Deserialize<TransactionMark>(bytes);
        }
        catch (MessagePackSerializationException e)
        {
            return null!;
        }
    }
}

public class StyxTransactionMarkConsumer
{
    private readonly string kakfaUrl;
    private readonly int kafkaPort;
    private readonly string kafkaTopic;
    private readonly ISellerService sellerService;
    private readonly ICustomerService customerService;
    private readonly IDeliveryService deliveryService;
    private readonly IConsumer<byte[], TransactionMark> consumer;

    public StyxTransactionMarkConsumer(string kafkaUrl,
        int kafkaPort,
        string kafkaTopic,
        ISellerService sellerService,
        ICustomerService customerService,
        IDeliveryService deliveryService)
    {
        this.kakfaUrl = kafkaUrl;
        this.kafkaPort = kafkaPort;
        this.kafkaTopic = kafkaTopic;
        this.sellerService = sellerService;
        this.customerService = customerService;
        this.deliveryService = deliveryService;
        ConsumerConfig config = new ConsumerConfig()
        {
            BootstrapServers = $"{this.kakfaUrl}:{this.kafkaPort}",
            AutoOffsetReset = AutoOffsetReset.Latest,
            GroupId = $"driver-{Guid.NewGuid()}",
            EnableAutoOffsetStore = true,
            EnableAutoCommit = false
        };
        using var admin = new AdminClientBuilder(new AdminClientConfig() { BootstrapServers = config.BootstrapServers })
            .Build();
        var partitions = admin.GetMetadata(this.kafkaTopic, TimeSpan.FromSeconds(10)).Topics[0].Partitions;
        ConsumerBuilder<byte[], TransactionMark> builder = new ConsumerBuilder<byte[], TransactionMark>(config)
            .SetKeyDeserializer(Deserializers.ByteArray)
            .SetValueDeserializer(new TransactionMarkDeserializer());
        this.consumer = builder.Build();
        this.consumer.Assign(partitions.Select(p =>
        {
            var tp = new TopicPartition(this.kafkaTopic, p.PartitionId);
            return new TopicPartitionOffset(tp, this.consumer.QueryWatermarkOffsets(tp, TimeSpan.FromSeconds(5)).High);
        }));
    }

    public async Task Run(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {

                ConsumeResult<byte[], TransactionMark> cr = this.consumer.Consume(cancellationToken);
                TransactionMark transactionMark = cr.Message.Value;
                await Shared.ResultQueue.Writer.WriteAsync(Shared.ITEM, CancellationToken.None);

                if (transactionMark is null)
                    continue;


                TransactionOutput transactionOutput = new TransactionOutput(transactionMark.tid,
                    cr.Message.Timestamp.UtcDateTime);
                int actorId = transactionMark.actorId;

                switch (transactionMark.type)
                {
                    case TransactionType.CUSTOMER_SESSION:
                        this.customerService.AddFinishedTransaction(transactionMark.actorId,
                            transactionOutput);
                        if (transactionMark.status == MarkStatus.SUCCESS)
                            await Shared.CheckoutOutputs.Writer.WriteAsync(transactionOutput,
                                CancellationToken.None);
                        else
                            await Shared.PoisonCheckoutOutputs.Writer.WriteAsync(transactionMark,
                                CancellationToken.None);
                        break;
                    case TransactionType.PRICE_UPDATE:
                        this.sellerService.AddFinishedTransaction(actorId,
                            transactionOutput);
                        if (transactionMark.status == MarkStatus.SUCCESS)
                            await Shared.PriceUpdateOutputs.Writer.WriteAsync(transactionOutput,
                                CancellationToken.None);
                        else
                            await Shared.PoisonPriceUpdateOutputs.Writer.WriteAsync(transactionMark,
                                CancellationToken.None);
                        break;
                    case TransactionType.UPDATE_PRODUCT:
                        this.sellerService.AddFinishedTransaction(actorId,
                            transactionOutput);
                        if (transactionMark.status == MarkStatus.SUCCESS)
                            await Shared.ProductUpdateOutputs.Writer.WriteAsync(transactionOutput,
                                CancellationToken.None);
                        else
                            await Shared.PoisonProductUpdateOutputs.Writer.WriteAsync(transactionMark,
                                CancellationToken.None);
                        break;
                    case TransactionType.QUERY_DASHBOARD:
                        this.sellerService.AddFinishedTransaction(actorId,
                            transactionOutput);
                        if (transactionMark.status == MarkStatus.SUCCESS)
                            await Shared.DashboardQueryOutputs.Writer.WriteAsync(transactionOutput,
                                CancellationToken.None);
                        else
                            await Shared.PoisonDashboardQueryOutputs.Writer.WriteAsync(transactionMark,
                                CancellationToken.None);
                        break;
                    case TransactionType.UPDATE_DELIVERY:
                        this.deliveryService.AddFinishedTransaction(transactionOutput);
                        if (transactionMark.status == MarkStatus.SUCCESS)
                            await Shared.DeliveryUpdateOutputs.Writer.WriteAsync(transactionOutput,
                                CancellationToken.None);
                        else
                            await Shared.PoisonDeliveryUpdateOutputs.Writer.WriteAsync(transactionMark,
                                CancellationToken.None);
                        break;
                    case TransactionType.NONE:
                    default:
                        throw new Exception("Unknown transaction type: " + transactionMark.type);
                }
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch
            {
                // ignored
            }
        }
    }
}
