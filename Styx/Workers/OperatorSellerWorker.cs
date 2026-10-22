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

namespace Styx.Workers;

public sealed class OperatorSellerWorker : DefaultSellerWorker
{

    private OperatorSellerWorker(int sellerId, IHttpClientFactory httpClientFactory, SellerWorkerConfig workerConfig, ILogger logger) : base(sellerId, httpClientFactory, workerConfig, logger)
    { }

    public new static OperatorSellerWorker BuildSellerWorker(int sellerId, IHttpClientFactory httpClientFactory, SellerWorkerConfig workerConfig)
    {
        var logger = LoggerProxy.GetInstance("Seller_" + sellerId);
        return new OperatorSellerWorker(sellerId, httpClientFactory, workerConfig, logger);
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
    public override void BrowseDashboard(string tid)
    {
        try
        {
            HttpRequestMessage message = new(HttpMethod.Get, config.sellerUrl + "/dashboard/" + this.sellerId + "/" + tid);
            message.Headers.Accept.Add(new System.Net.Http.Headers.MediaTypeWithQualityHeaderValue("application/json"));
            var startTs = DateTime.UtcNow;
            var response = HttpUtils.HTTP_CLIENT.Send(message);
            var endTs = DateTime.UtcNow;
            if (response.IsSuccessStatusCode)
            {
                this.finishedTransactions.Add(new TransactionOutput(tid, endTs));
                this.submittedTransactions.Add(new TransactionIdentifier(tid, TransactionType.QUERY_DASHBOARD, startTs));
            }
            else
            {
                this.abortedTransactions.Add(new TransactionMark(tid, TransactionType.QUERY_DASHBOARD, this.sellerId, MarkStatus.ABORT, "seller"));
                this.logger.LogWarning("Seller {0} - {1} - Dashboard retrieval failed: {2}", this.sellerId, startTs, response.ReasonPhrase);
            }
        }
        catch (Exception e)
        {
            this.abortedTransactions.Add(new TransactionMark(tid, TransactionType.QUERY_DASHBOARD, this.sellerId, MarkStatus.ABORT, "seller"));
            this.logger.LogError("Seller {0}: Dashboard could not be retrieved: {1}", this.sellerId, e);
        }
    }
    protected override void DoAfterSuccessUpdate(string tid, TransactionType transactionType)
    {
        this.finishedTransactions.Add(new TransactionOutput(tid, DateTime.UtcNow));
    }

}
