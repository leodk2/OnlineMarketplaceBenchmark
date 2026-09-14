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
    public static OperatorExperimentManager BuildOperatorExperimentManager(IHttpClientFactory httpClientFactory, ExperimentConfig config, DuckDBConnection connection)
    {
        return new OperatorExperimentManager(httpClientFactory, OperatorSellerWorker.BuildSellerWorker,
            OperatorCustomerWorker.BuildCustomerWorker, 
            DefaultDeliveryWorker.BuildDeliveryWorker, config, connection);
    }

    private OperatorExperimentManager(IHttpClientFactory httpClientFactory,
        SellerService.BuildSellerWorkerDelegate sellerWorkerDelegate,
        CustomerService.BuildCustomerWorkerDelegate customerWorkerDelegate,
        DeliveryService.BuildDeliveryWorkerDelegate deliveryWorkerDelegate,
        ExperimentConfig config,
        DuckDBConnection connection) :
        base(httpClientFactory,
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
    }

}
