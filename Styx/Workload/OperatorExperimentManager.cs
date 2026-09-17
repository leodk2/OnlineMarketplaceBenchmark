using Common.Experiment;
using Common.Metric;
using Common.Services;
using Common.Workers.Customer;
using Common.Workers.Delivery;
using Common.Workload;
using DuckDB.NET.Data;
using Styx.Workers;

namespace Styx.Workload;

public class OperatorExperimentManager : AbstractExperimentManager
{
    private CancellationTokenSource source;
    private readonly List<StyxTransactionMarkConsumer> styxTransactionMarkConsumers;

    private static List<string> styxOutputTopics =
    [
        "order--OUT",
        "product--OUT",
        "stock--OUT",
        "shipment---OUT",
        "seller--OUT"
    ];

    public static OperatorExperimentManager BuildOperatorExperimentManager(IHttpClientFactory httpClientFactory,
        ExperimentConfig config,
        DuckDBConnection connection)
    {
        return new OperatorExperimentManager(httpClientFactory,
            OperatorSellerWorker.BuildSellerWorker,
            OperatorCustomerWorker.BuildCustomerWorker,
            DefaultDeliveryWorker.BuildDeliveryWorker,
            config,
            connection);
    }

    private OperatorExperimentManager(IHttpClientFactory httpClientFactory,
        SellerService.BuildSellerWorkerDelegate sellerWorkerDelegate,
        CustomerService.BuildCustomerWorkerDelegate customerWorkerDelegate,
        DeliveryService.BuildDeliveryWorkerDelegate deliveryWorkerDelegate,
        ExperimentConfig config,
        DuckDBConnection connection) : base(httpClientFactory,
        config.concurrencyType == ConcurrencyType.CONTROL
            ? OperatorWorkloadManager.BuildWorkloadManager
            : WorkloadManager.BuildWorkloadManager,
        MetricManager.BuildMetricManager,
        sellerWorkerDelegate,
        customerWorkerDelegate,
        deliveryWorkerDelegate,
        config,
        connection)
    {
        this.styxTransactionMarkConsumers = new List<StyxTransactionMarkConsumer>();
        this.source = new CancellationTokenSource();
    }

    protected override void PreExperiment()
    {
        base.PreExperiment();
        foreach (string topic in styxOutputTopics)
        {
            this.styxTransactionMarkConsumers.Add(new StyxTransactionMarkConsumer(this.config.streamingConfig.host,
                this.config.streamingConfig.port,
                topic,
                this.sellerService,
                this.customerService,
                this.deliveryService));
        }

        foreach (StyxTransactionMarkConsumer consumer in this.styxTransactionMarkConsumers)
        {
            Task.Factory.StartNew(() => consumer.Run(this.source.Token));
        }
        Console.WriteLine("=== Starting receipt pulling thread ===");
    }

    public override void PostExperiment()
    {
        base.PostExperiment();
        this.source.Cancel();
        this.styxTransactionMarkConsumers.Clear();
    }
}