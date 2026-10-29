using System.Diagnostics;
using System.Globalization;
using System.Text;
using System.Text.Json;
using Google.Protobuf;
#if SCHEMA_V2
using Dis.Orders.V2;
#elif SCHEMA_BREAKING
using Dis.Orders.V2Breaking;
#else
using Dis.Orders.V1;
#endif

// Explicit bench command only: normal client startup never runs measurements.
static class RpcBenchmark
{
    public static async Task RunAsync(OrderService.OrderServiceClient client,
        Dictionary<string, List<string>> options, double timeout)
    {
        string Value(string name, string fallback) => options.TryGetValue(name, out var a) ? a.Single() : fallback;
        var path = Value("dataset", "data/orders.jsonl");
        if (!File.Exists(path)) throw new ArgumentException("Dataset not found. Run 01-generate-data.ps1 first.");
        var orders = File.ReadLines(path).Where(line => !string.IsNullOrWhiteSpace(line))
            .Select(line => JsonParser.Default.Parse<Order>(line)).ToArray();
        if (orders.Length < 10000) throw new ArgumentException("At least 10000 orders are required.");
        if (orders.Select(o => o.OrderId).Distinct().Count() != orders.Length)
            throw new ArgumentException("Dataset contains duplicate order IDs.");
        var runs = int.Parse(Value("runs", "5"), CultureInfo.InvariantCulture);
        if (runs < 5) throw new ArgumentException("At least five measured runs are required.");
        var output = Path.GetFullPath(Value("output-dir", "results/rpc"));
        Directory.CreateDirectory(output);
        var runId = Value("run-id", Guid.NewGuid().ToString("N"));
        var statistics = new List<(double P50, double P95, double P99, double Mean)>();
        var rows = new List<object>();
        var seen = new HashSet<string>();
        using (var csv = new StreamWriter(Path.Combine(output, "rpc_samples.csv"), false, new UTF8Encoding(true)))
        {
            csv.WriteLine("run,call_index,request_id,order_id,elapsed_ms");
            for (var run = -1; run < runs; run++)
            {
                var elapsed = new double[orders.Length];
                var responseIds = new string[orders.Length];
                for (var index = 0; index < orders.Length; index++)
                {
                    var request = Request(orders[index], $"benchmark-{runId}-{run}-{index}");
                    var deadline = DateTime.UtcNow.AddSeconds(timeout);
                    var started = Stopwatch.GetTimestamp();
                    var response = await client.CreateOrderAsync(request, deadline: deadline);
                    elapsed[index] = Stopwatch.GetElapsedTime(started).TotalMilliseconds;
                    responseIds[index] = response.OrderId;
                    // Validation is outside the measured RPC interval.
                    if (string.IsNullOrWhiteSpace(response.OrderId) || !seen.Add(response.OrderId))
                        throw new ArgumentException("CreateOrder returned an empty or repeated order ID.");
                }
                if (run < 0)
                {
                    Console.WriteLine($"warmup: {orders.Length} sequential calls, excluded from statistics");
                    continue;
                }
                // Disk writes happen after the whole pass, never inside the RPC timer.
                for (var index = 0; index < elapsed.Length; index++)
                    csv.WriteLine(FormattableString.Invariant($"{run + 1},{index},benchmark-{runId}-{run}-{index},{responseIds[index]},{elapsed[index]:R}"));
                csv.Flush();
                var mean = elapsed.Average();
                Array.Sort(elapsed);
                var item = (P50: Quantile(elapsed, .50), P95: Quantile(elapsed, .95), P99: Quantile(elapsed, .99), Mean: mean);
                statistics.Add(item);
                rows.Add(new { run = run + 1, count = orders.Length, p50_ms = item.P50,
                    p95_ms = item.P95, p99_ms = item.P99, mean_ms = item.Mean });
                Console.WriteLine(FormattableString.Invariant($"run {run + 1}: n={orders.Length} p50={item.P50:F4} ms p95={item.P95:F4} ms p99={item.P99:F4} ms mean={item.Mean:F4} ms"));
            }
        }

        // Separate fixture microbenchmarks: these do NOT instrument the real gRPC marshaller.
        var fixtureRequests = orders.Select((order, i) => Request(order, $"benchmark-{runId}-profile-{i}")).ToArray();
        var responseBytes = orders.Select(order => order.ToByteArray()).ToArray();
        var encodeTimes = new List<double>();
        var decodeTimes = new List<double>();
        var codecRows = new List<object>();
        for (var run = -1; run < runs; run++)
        {
            var (encoded, encodeMs) = Measure(fixtureRequests, message => message.ToByteArray());
            var (decoded, decodeMs) = Measure(responseBytes, bytes => Order.Parser.ParseFrom(bytes));
            if (encoded.Length != orders.Length || decoded.Length != orders.Length)
                throw new ArgumentException("Incomplete codec profile.");
            GC.KeepAlive(encoded);
            GC.KeepAlive(decoded);
            if (run < 0) continue;
            encodeTimes.Add(encodeMs);
            decodeTimes.Add(decodeMs);
            codecRows.Add(new { run = run + 1, request_encode_ms = encodeMs, response_decode_ms = decodeMs });
        }
        var summary = new
        {
            client = "C#", transport = "gRPC", schema = Order.Descriptor.File.Name,
            count = orders.Length, warmup_runs = 1, measured_runs = runs,
            total_created_including_warmup = seen.Count,
            median_p50_ms = Median(statistics.Select(s => s.P50)),
            median_p95_ms = Median(statistics.Select(s => s.P95)),
            median_p99_ms = Median(statistics.Select(s => s.P99)),
            median_mean_ms = Median(statistics.Select(s => s.Mean)),
            percentile_method = "linear interpolation: position=(n-1)*p",
            calls = "sequential; one reused gRPC channel; distinct request_id per call; broker integration disabled",
            runs = rows,
            client_codec = new
            {
                request_encode_ns_per_call = Median(encodeTimes) * 1_000_000 / orders.Length,
                response_decode_ns_per_call = Median(decodeTimes) * 1_000_000 / orders.Length,
                runs = codecRows,
                method = "Separate generated-message microbenchmarks on dataset fixtures, not direct gRPC marshaller instrumentation"
            }
        };
        File.WriteAllText(Path.Combine(output, "rpc_client.json"),
            JsonSerializer.Serialize(summary, new JsonSerializerOptions { WriteIndented = true }));
        using var aggregate = new StreamWriter(Path.Combine(output, "rpc_runs.csv"), false, new UTF8Encoding(true));
        aggregate.WriteLine("run,count,p50_ms,p95_ms,p99_ms,mean_ms");
        for (var i = 0; i < statistics.Count; i++)
        {
            var s = statistics[i];
            aggregate.WriteLine(FormattableString.Invariant($"{i + 1},{orders.Length},{s.P50:R},{s.P95:R},{s.P99:R},{s.Mean:R}"));
        }
        Console.WriteLine("C# RPC passes complete; result files saved.");
    }

    static CreateOrderRequest Request(Order order, string id)
    {
        var request = new CreateOrderRequest { RequestId = id, BuyerId = order.BuyerId, ShippingAddress = order.ShippingAddress };
        request.Items.Add(order.Items);
        foreach (var name in new[] { "comment", "promo_code" })
        {
            var source = Order.Descriptor.FindFieldByName(name);
            var target = CreateOrderRequest.Descriptor.FindFieldByName(name);
            if (source is not null && target is not null && source.Accessor.HasValue(order))
                target.Accessor.SetValue(request, source.Accessor.GetValue(order));
        }
        return request;
    }

    static (TOut[] Values, double Milliseconds) Measure<TIn, TOut>(TIn[] input, Func<TIn, TOut> function)
    {
        var start = Stopwatch.GetTimestamp();
        var result = new TOut[input.Length];
        for (var i = 0; i < input.Length; i++) result[i] = function(input[i]);
        return (result, Stopwatch.GetElapsedTime(start).TotalMilliseconds);
    }

    static double Median(IEnumerable<double> values) => Quantile(values.Order().ToArray(), .5);

    static double Quantile(double[] sorted, double p)
    {
        var position = (sorted.Length - 1) * p;
        var lower = (int)Math.Floor(position);
        var upper = (int)Math.Ceiling(position);
        return sorted[lower] + (sorted[upper] - sorted[lower]) * (position - lower);
    }
}
