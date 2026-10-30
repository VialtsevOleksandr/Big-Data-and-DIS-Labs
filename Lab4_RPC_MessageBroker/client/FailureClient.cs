using System.Globalization;
using System.Text;
using System.Text.Json;
using Google.Protobuf;
using Grpc.Core;
#if SCHEMA_V2
using Dis.Orders.V2;
#elif SCHEMA_BREAKING
using Dis.Orders.V2Breaking;
#else
using Dis.Orders.V1;
#endif

static class FailureClient
{
    static string Value(Dictionary<string, List<string>> values, string key, string? fallback = null) =>
        values.TryGetValue(key, out var found) ? found.Single() : fallback ?? throw new ArgumentException($"Missing --{key}");

    public static async Task RetryAsync(OrderService.OrderServiceClient client, CreateOrderRequest request,
        Dictionary<string, List<string>> values, double firstTimeout)
    {
        var retryTimeout = double.Parse(Value(values, "retry-timeout", "10"), CultureInfo.InvariantCulture);
        if (!double.IsFinite(retryTimeout) || retryTimeout <= 0 || retryTimeout > 3600)
            throw new ArgumentException("Invalid --retry-timeout");
        // Warm the actual channel so the first short deadline tests slow processing, not connection setup.
        var before = await client.ListOrdersByBuyerAsync(new ListOrdersByBuyerRequest { BuyerId = request.BuyerId },
            deadline: DateTime.UtcNow.AddSeconds(15));
        string firstStatus = "OK", firstDetail = "", retryStatus = "OK", retryDetail = "";
        string? firstId = null, retryId = null;
        try
        {
            firstId = (await client.CreateOrderAsync(request, deadline: DateTime.UtcNow.AddSeconds(firstTimeout))).OrderId;
        }
        catch (RpcException error) { firstStatus = error.StatusCode.ToString(); firstDetail = error.Status.Detail; }
        try
        {
            // Same request object and same request_id, with a longer deadline.
            retryId = (await client.CreateOrderAsync(request, deadline: DateTime.UtcNow.AddSeconds(retryTimeout))).OrderId;
        }
        catch (RpcException error) { retryStatus = error.StatusCode.ToString(); retryDetail = error.Status.Detail; }
        var after = await client.ListOrdersByBuyerAsync(new ListOrdersByBuyerRequest { BuyerId = request.BuyerId },
            deadline: DateTime.UtcNow.AddSeconds(15));
        Console.WriteLine(JsonSerializer.Serialize(new
        {
            request_id = request.RequestId, buyer_id = request.BuyerId,
            first_timeout_seconds = firstTimeout, retry_timeout_seconds = retryTimeout,
            first_status = firstStatus, first_detail = firstDetail, first_order_id = firstId,
            retry_status = retryStatus, retry_detail = retryDetail, retry_order_id = retryId,
            before_order_ids = before.Orders.Select(order => order.OrderId).ToArray(),
            after_order_ids = after.Orders.Select(order => order.OrderId).ToArray(),
            actual_orders_after_retry = after.Orders.Count
        }));
    }

    public static async Task LoadAsync(OrderService.OrderServiceClient client,
        Dictionary<string, List<string>> values, double timeout)
    {
        var count = int.Parse(Value(values, "count", "10000"), CultureInfo.InvariantCulture);
        if (count < 10000) throw new ArgumentException("At least 10000 messages are required.");
        var dataset = Value(values, "dataset");
        var orders = File.ReadLines(dataset).Where(line => !string.IsNullOrWhiteSpace(line))
            .Take(count).Select(line => JsonParser.Default.Parse<Order>(line)).ToArray();
        if (orders.Length != count) throw new ArgumentException("Dataset is smaller than --count.");
        if (orders.Select(order => order.OrderId).Distinct().Count() != count)
            throw new ArgumentException("Dataset order IDs must be unique.");
        var buyer = Value(values, "buyer");
        var runId = Value(values, "run-id");
        var manifest = Path.GetFullPath(Value(values, "manifest"));
        Directory.CreateDirectory(Path.GetDirectoryName(manifest)!);
        var seen = new HashSet<string>();
        using (var stream = new StreamWriter(manifest, false, new UTF8Encoding(true)))
        {
            stream.WriteLine("index,source_order_id,request_id,order_id,response_status");
            for (var index = 0; index < orders.Length; index++)
            {
                var source = orders[index];
                var request = new CreateOrderRequest
                {
                    RequestId = $"failure-{runId}-{index}", BuyerId = buyer, ShippingAddress = source.ShippingAddress
                };
                request.Items.Add(source.Items);
                foreach (var name in new[] { "comment", "promo_code" })
                {
                    var field = Order.Descriptor.FindFieldByName(name);
                    var target = CreateOrderRequest.Descriptor.FindFieldByName(name);
                    if (field is not null && target is not null && field.Accessor.HasValue(source))
                        target.Accessor.SetValue(request, field.Accessor.GetValue(source));
                }
                var response = await client.CreateOrderAsync(request, deadline: DateTime.UtcNow.AddSeconds(timeout));
                if (!seen.Add(response.OrderId) || string.IsNullOrWhiteSpace(response.OrderId))
                    throw new ArgumentException("Empty or duplicate server order ID.");
                stream.WriteLine(FormattableString.Invariant($"{index},{source.OrderId},{request.RequestId},{response.OrderId},{(int)response.Status}"));
                if ((index + 1) % 1000 == 0) { stream.Flush(); Console.Error.WriteLine($"Created {index + 1}/{count}"); }
            }
        }
        Console.WriteLine(JsonSerializer.Serialize(new { count, buyer_id = buyer, manifest,
            distinct_order_ids = seen.Count, buyer_override = "A unique buyer isolates this run; item/address/comment data comes from the seeded dataset" }));
    }

    public static async Task StatusAsync(OrderService.OrderServiceClient client,
        Dictionary<string, List<string>> values, double timeout)
    {
        var buyer = Value(values, "buyer");
        var response = await client.ListOrdersByBuyerAsync(new ListOrdersByBuyerRequest { BuyerId = buyer },
            deadline: DateTime.UtcNow.AddSeconds(timeout));
        var path = Path.GetFullPath(Value(values, "status-file"));
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        using (var stream = new StreamWriter(path, false, new UTF8Encoding(true)))
        {
            stream.WriteLine("order_id,status");
            foreach (var order in response.Orders)
                stream.WriteLine(FormattableString.Invariant($"{order.OrderId},{(int)order.Status}"));
        }
        Console.WriteLine(JsonSerializer.Serialize(new { count = response.Orders.Count,
            pending = response.Orders.Count(order => (int)order.Status == 1),
            confirmed = response.Orders.Count(order => (int)order.Status == 2),
            rejected = response.Orders.Count(order => (int)order.Status == 3), status_file = path }));
    }
}
