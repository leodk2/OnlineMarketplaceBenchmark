using System.Net.Http.Headers;
using System.Net.Http.Json;
using Common.Entities;
using Common.Http;
using Common.Infra;
using Common.Requests;
using Common.Streaming;
using Common.Workers.Seller;
using Common.Workload;
using Common.Workload.Metrics;
using Common.Workload.Seller;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace  Styx.Workers;

public sealed class OperatorSellerWorker : DefaultSellerWorker
{

    private OperatorSellerWorker(int sellerId, IHttpClientFactory httpClientFactory, SellerWorkerConfig workerConfig, ILogger logger) : base(sellerId, httpClientFactory, workerConfig, logger)
    { }

    public new static OperatorSellerWorker BuildSellerWorker(int sellerId, IHttpClientFactory httpClientFactory, SellerWorkerConfig workerConfig)
    {
        var logger = LoggerProxy.GetInstance("Seller_"+ sellerId);
        return new OperatorSellerWorker(sellerId, httpClientFactory, workerConfig, logger);
    }

    protected override void DoAfterSuccessUpdate(string tid, TransactionType transactionType)
    {
        this.finishedTransactions.Add(new TransactionOutput(tid, DateTime.UtcNow));
    }
    protected override void SendProductUpdateRequest(Product product, string tid)
    {
        var payLoad = JsonConvert.SerializeObject(product);

        var req = new HttpRequestMessage(HttpMethod.Put, this.config.productUrl)
        {
            Content = new StringContent(payLoad)
        };

        var resp = HttpUtils.HTTP_CLIENT.Send(req);

        var now = DateTime.UtcNow;

        if (resp.IsSuccessStatusCode)
        {
            this.submittedTransactions.Add(new TransactionIdentifier(tid, TransactionType.UPDATE_PRODUCT, now));
        }
        else
        {
            this.abortedTransactions.Add(new TransactionMark(tid, TransactionType.UPDATE_PRODUCT, this.sellerId, MarkStatus.ABORT, "product"));
            this.logger.LogError("Seller {0} failed to update product {1} version: {2}", this.sellerId, product.product_id, resp.ReasonPhrase);
        }

    }

}
