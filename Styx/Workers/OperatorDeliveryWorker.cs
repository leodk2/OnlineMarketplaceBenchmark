using Common.Http;
using Common.Infra;
using Common.Streaming;
using Common.Workers.Delivery;
using Common.Workload;
using Common.Workload.Delivery;
using Common.Workload.Metrics;
using Microsoft.Extensions.Logging;

namespace Styx.Workers;

public sealed class OperatorDeliveryWorker : DefaultDeliveryWorker
{
    private OperatorDeliveryWorker(DeliveryWorkerConfig config, IHttpClientFactory httpClientFactory, ILogger logger)
        : base(config, httpClientFactory, logger)
    {
    }

    public static new OperatorDeliveryWorker BuildDeliveryWorker(IHttpClientFactory httpClientFactory,
        DeliveryWorkerConfig config)
    {
        var logger = LoggerProxy.GetInstance("Delivery");
        return new OperatorDeliveryWorker(config, httpClientFactory, logger);
    }

    public override void Run(string tid)
    {
        var req = new HttpRequestMessage(HttpMethod.Patch, $"{this.config.shipmentUrl}/{tid}");
        var res = HttpUtils.HTTP_CLIENT.Send(req);
        if (res.IsSuccessStatusCode)
        {
            this.submittedTransactions.Add(new TransactionIdentifier(tid, TransactionType.UPDATE_DELIVERY, DateTime.UtcNow));
        }
        else
        {
            this.abortedTransactions.Add(new TransactionMark(tid, TransactionType.UPDATE_DELIVERY, 1, MarkStatus.ABORT, "shipment"));
            
        }

    }
}
