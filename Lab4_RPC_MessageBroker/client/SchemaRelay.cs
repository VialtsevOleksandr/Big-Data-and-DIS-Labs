using System.Text.Json;
using Google.Protobuf;
#if SCHEMA_V2
using Dis.Orders.V2;
#elif SCHEMA_BREAKING
using Dis.Orders.V2Breaking;
#else
using Dis.Orders.V1;
#endif

static class SchemaRelay
{
    public static void Run(Dictionary<string, List<string>> options)
    {
        string Required(string name) => options.TryGetValue(name, out var values)
            ? values.Single() : throw new ArgumentException($"Missing --{name}");
        var mode = options.TryGetValue("mode", out var modes) ? modes.Single() : "direct";
        var message = Order.Parser.ParseFrom(File.ReadAllBytes(Required("input")));
        var before = Describe(message);
        switch (mode)
        {
            case "direct": break;
            case "json":
                message = JsonParser.Default.Parse<Order>(JsonFormatter.Default.Format(message));
                break;
            case "fields":
                var copy = new Order { OrderId = message.OrderId, BuyerId = message.BuyerId, Status = message.Status };
                if (message.CreatedAt is not null) copy.CreatedAt = message.CreatedAt.Clone();
                if (message.ShippingAddress is not null) copy.ShippingAddress = message.ShippingAddress.Clone();
                foreach (var item in message.Items) copy.Items.Add(item.Clone());
                foreach (var name in new[] { "comment", "promo_code" })
                {
                    var field = Order.Descriptor.FindFieldByName(name);
                    if (field is not null && field.Accessor.HasValue(message))
                        field.Accessor.SetValue(copy, field.Accessor.GetValue(message));
                }
                message = copy;
                break;
            default: throw new ArgumentException("--mode must be direct, json or fields");
        }
        File.WriteAllBytes(Required("output"), message.ToByteArray());
        Console.WriteLine(JsonSerializer.Serialize(new { language = "C#", schema_namespace = typeof(Order).Namespace,
            mode, before, after = Describe(message) }));
    }

    static object Describe(Order order)
    {
        object State(string name)
        {
            var field = Order.Descriptor.FindFieldByName(name);
            return new { known = field is not null,
                present = field is null ? (bool?)null : field.Accessor.HasValue(order),
                value = field is null ? null : field.Accessor.GetValue(order) };
        }
        using var body = JsonDocument.Parse(JsonFormatter.Default.Format(order));
        return new { body = body.RootElement.Clone(), comment = State("comment"), promo_code = State("promo_code"),
            prices_minor = order.Items.Select(item => item.UnitPriceMinor).ToArray() };
    }
}
