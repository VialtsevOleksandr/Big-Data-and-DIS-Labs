using System.Globalization;
using System.Text;
#if SCHEMA_V2
using Dis.Orders.V2;
#elif SCHEMA_BREAKING
using Dis.Orders.V2Breaking;
#else
using Dis.Orders.V1;
#endif
using Google.Protobuf;
using Grpc.Core;
using Grpc.Net.Client;

Console.OutputEncoding = Encoding.UTF8;
Console.InputEncoding = Encoding.UTF8;
Console.SetError(new StreamWriter(Console.OpenStandardError(), new UTF8Encoding(false)) { AutoFlush = true });
return await RunAsync(args);

static async Task<int> RunAsync(string[] args)
{
    try
    {
        var options = Options.Parse(args);
        if (options.Command is "help" or "--help" or "-h")
        {
            PrintHelp();
            return 0;
        }
        if (options.Command == "schema-relay")
        {
            SchemaRelay.Run(options.Values);
            return 0;
        }
        using var channel = GrpcChannel.ForAddress(options.Address, new GrpcChannelOptions
        {
            MaxReceiveMessageSize = 32 * 1024 * 1024,
            // Retries are explicit so timeout experiments are reproducible.
            MaxRetryAttempts = 1
        });
        var client = new OrderService.OrderServiceClient(channel);
        if (options.Command == "interactive")
            await InteractiveAsync(client, options);
        else
            await ExecuteAsync(client, options);
        return 0;
    }
    catch (RpcException error)
    {
        Console.Error.WriteLine($"gRPC {error.StatusCode}: {error.Status.Detail}");
        if (error.StatusCode == StatusCode.Unavailable)
            Console.Error.WriteLine("Сервер недоступний. Перевірте його запуск та адресу.");
        if (error.StatusCode == StatusCode.DeadlineExceeded)
            Console.Error.WriteLine("Час очікування вичерпано. Замовлення могло вже створитися; повторіть той самий request_id.");
        return 2;
    }
    catch (Exception error) when (error is ArgumentException or FormatException or OverflowException)
    {
        Console.Error.WriteLine($"Помилка аргументів: {error.Message}");
        return 1;
    }
}

static DateTime Deadline(Options options) => DateTime.UtcNow.AddSeconds(options.TimeoutSeconds);

static async Task ExecuteAsync(OrderService.OrderServiceClient client, Options options)
{
    switch (options.Command)
    {
        case "retry-demo":
            await FailureClient.RetryAsync(client, CreateRequest(options), options.Values, options.TimeoutSeconds);
            break;
        case "load-orders":
            await FailureClient.LoadAsync(client, options.Values, options.TimeoutSeconds);
            break;
        case "order-statuses":
            await FailureClient.StatusAsync(client, options.Values, options.TimeoutSeconds);
            break;
        case "bench":
            await RpcBenchmark.RunAsync(client, options.Values, options.TimeoutSeconds);
            break;
        case "create":
            var request = CreateRequest(options);
            // Keep stdout machine-readable; the ID remains available even on timeout.
            Console.Error.WriteLine($"request_id={request.RequestId}");
            Print(await client.CreateOrderAsync(request, deadline: Deadline(options)));
            break;
        case "get":
            Print(await client.GetOrderAsync(new GetOrderRequest { OrderId = options.RequiredId() },
                deadline: Deadline(options)));
            break;
        case "list":
            Print(await client.ListOrdersByBuyerAsync(new ListOrdersByBuyerRequest { BuyerId = options.RequiredId() },
                deadline: Deadline(options)));
            break;
        case "demo":
            await DemoAsync(client, options);
            break;
        default:
            throw new ArgumentException($"Невідома команда: {options.Command}. Використайте help.");
    }
}

static CreateOrderRequest CreateRequest(Options options)
{
    var request = new CreateOrderRequest
    {
        RequestId = options.Single("request-id", Guid.NewGuid().ToString()),
        BuyerId = options.Single("buyer", "buyer-demo"),
        ShippingAddress = new Address
        {
            Country = options.Single("country", "UA"),
            City = options.Single("city", "Київ"),
            Street = options.Single("street", "Хрещатик, 1"),
            PostalCode = options.Single("postal-code", "01001")
        }
    };
    foreach (var name in new[] { "comment", "promo-code" })
        if (options.Values.TryGetValue(name, out var values))
            SetOptional(request, name.Replace('-', '_'), values.Single());
    var items = options.Values.GetValueOrDefault("item") ?? ["sku-001:2:15999"];
    foreach (var text in items)
    {
        var parts = text.Split(':');
        if (parts.Length != 3)
            throw new ArgumentException("--item має формат product_id:quantity:price_in_kopecks");
        request.Items.Add(new OrderItem
        {
            ProductId = parts[0],
            Quantity = uint.Parse(parts[1], NumberStyles.None, CultureInfo.InvariantCulture),
            UnitPriceMinor = long.Parse(parts[2], NumberStyles.Integer, CultureInfo.InvariantCulture)
        });
    }
    return request;
}

static void Print(IMessage message) => Console.WriteLine(JsonFormatter.Default.Format(message));

static void SetOptional(IMessage message, string name, string value)
{
    var field = message.Descriptor.FindFieldByName(name)
        ?? throw new ArgumentException($"Поле {name} відсутнє в цій версії схеми.");
    field.Accessor.SetValue(message, value);
}

static async Task DemoAsync(OrderService.OrderServiceClient client, Options options)
{
    var buyer = $"buyer-demo-{Guid.NewGuid():N}";
    var request = CreateRequest(new Options { Values = new() { ["buyer"] = [buyer] } });
    var optionalField = CreateOrderRequest.Descriptor.FindFieldByName("comment") ?? CreateOrderRequest.Descriptor.FindFieldByName("promo_code")!;
    optionalField.Accessor.SetValue(request, "Демонстраційне значення");
    var first = await client.CreateOrderAsync(request, deadline: Deadline(options));
    var retry = await client.CreateOrderAsync(request, deadline: Deadline(options));
    if (first.OrderId != retry.OrderId)
        throw new ArgumentException("Демонстрація очікує сервер з увімкненою ідемпотентністю.");
    request.RequestId = Guid.NewGuid().ToString();
    optionalField.Accessor.Clear(request);
    var second = await client.CreateOrderAsync(request, deadline: Deadline(options));
    var fetched = await client.GetOrderAsync(new GetOrderRequest { OrderId = first.OrderId }, deadline: Deadline(options));
    var listed = await client.ListOrdersByBuyerAsync(new ListOrdersByBuyerRequest { BuyerId = buyer }, deadline: Deadline(options));
    Console.WriteLine("Створено два замовлення: з коментарем і без. Повтор першого запиту повернув той самий ID.");
    Print(first);
    Print(second);
    Console.WriteLine($"Пошук за ID: {fetched.OrderId}; замовлень покупця: {listed.Orders.Count}.");
    Print(listed);
}

static async Task InteractiveAsync(OrderService.OrderServiceClient client, Options options)
{
    Console.WriteLine("DIS: клієнт замовлень. Дані зберігаються в пам’яті сервера.");
    while (true)
    {
        Console.WriteLine("\n1 — створити | 2 — знайти за ID | 3 — список покупця | 4 — демонстрація | 0 — вихід");
        Console.Write("> ");
        var choice = Console.ReadLine();
        if (choice is null or "0") return;
        try
        {
            var action = new Options { Address = options.Address, TimeoutSeconds = options.TimeoutSeconds };
            switch (choice)
            {
                case "1":
                    action.Command = "create";
                    action.Values["buyer"] = [Prompt("ID покупця", "buyer-demo")];
                    action.Values["country"] = [Prompt("Країна", "UA")];
                    action.Values["city"] = [Prompt("Місто", "Київ")];
                    action.Values["street"] = [Prompt("Вулиця і будинок", "Хрещатик, 1")];
                    action.Values["postal-code"] = [Prompt("Поштовий індекс", "01001")];
                    var count = int.Parse(Prompt("Кількість позицій (1–20)", "1"), CultureInfo.InvariantCulture);
                    if (count is < 1 or > 20) throw new ArgumentException("Потрібно від 1 до 20 позицій.");
                    action.Values["item"] = [];
                    for (var index = 0; index < count; index++)
                    {
                        var product = Prompt($"Товар {index + 1}", $"sku-{index + 1:000}");
                        var quantity = Prompt("Кількість одиниць", "1");
                        var price = Prompt("Ціна одиниці у копійках", "15999");
                        action.Values["item"].Add($"{product}:{quantity}:{price}");
                    }
                    Console.Write(CreateOrderRequest.Descriptor.FindFieldByName("comment") is null
                        ? "Промокод (Enter — відсутній): " : "Коментар (Enter — відсутній): ");
                    var comment = Console.ReadLine();
                    if (!string.IsNullOrEmpty(comment))
                        action.Values[CreateOrderRequest.Descriptor.FindFieldByName("comment") is null ? "promo-code" : "comment"] = [comment];
                    break;
                case "2":
                    action.Command = "get";
                    action.Id = Prompt("ID замовлення", "");
                    break;
                case "3":
                    action.Command = "list";
                    action.Id = Prompt("ID покупця", "buyer-demo");
                    break;
                case "4": action.Command = "demo"; break;
                default: Console.WriteLine("Оберіть пункт від 0 до 4."); continue;
            }
            await ExecuteAsync(client, action);
        }
        catch (RpcException error)
        {
            Console.Error.WriteLine($"gRPC {error.StatusCode}: {error.Status.Detail}");
        }
        catch (Exception error) when (error is ArgumentException or FormatException or OverflowException)
        {
            Console.Error.WriteLine($"Помилка: {error.Message}");
        }
    }
}

static string Prompt(string label, string fallback)
{
    Console.Write($"{label} [{fallback}]: ");
    var value = Console.ReadLine();
    return string.IsNullOrEmpty(value) ? fallback : value;
}

static void PrintHelp() => Console.WriteLine("""
    DIS — клієнт замовлень (C# / gRPC / Protocol Buffers)
    Без аргументів: інтерактивне меню.
    Команди:
      create [--buyer ID] [--request-id ID] [--comment TEXT | --promo-code TEXT]
             [--item SKU:QUANTITY:PRICE_IN_KOPECKS] (можна повторювати)
             [--country UA] [--city TEXT] [--street TEXT] [--postal-code TEXT]
      get ORDER_ID
      list BUYER_ID
      demo
      help
      bench --dataset PATH --output-dir PATH [--runs 5] [--run-id TEXT]
      schema-relay --input PATH --output PATH [--mode direct|json|fields]
      retry-demo --buyer ID --request-id ID --timeout 0.5 --retry-timeout 10
      load-orders --dataset PATH --manifest PATH --buyer ID --run-id TEXT [--count 10000]
      order-statuses --buyer ID --status-file PATH
    Спільні параметри: --address http://127.0.0.1:50051 --timeout 5
    Ціни — у копійках, валюта — гривня. Результати команд — Protobuf JSON.
    Коди виходу: 0 — успіх; 1 — помилка аргументів; 2 — помилка RPC.
    """);

sealed class Options
{
    public string Command { get; set; } = "interactive";
    public string? Id { get; set; }
    public string Address { get; set; } = "http://127.0.0.1:50051";
    public double TimeoutSeconds { get; set; } = 5;
    public Dictionary<string, List<string>> Values { get; set; } = new();
    public string Single(string name, string fallback) => Values.TryGetValue(name, out var values) ? values.Single() : fallback;
    public string RequiredId() => string.IsNullOrWhiteSpace(Id) ? throw new ArgumentException("Потрібен ID.") : Id;

    public static Options Parse(string[] args)
    {
        var result = new Options();
        var positionals = new List<string>();
        string[] allowed = ["address", "timeout", "buyer", "request-id", "comment", "promo-code", "item", "country", "city", "street", "postal-code", "dataset", "output-dir", "runs", "run-id", "input", "output", "mode", "retry-timeout", "manifest", "count", "status-file"];
        for (var index = 0; index < args.Length; index++)
        {
            if (args[index] is "--help" or "-h") { result.Command = "help"; return result; }
            if (!args[index].StartsWith("--", StringComparison.Ordinal))
            {
                positionals.Add(args[index]);
                continue;
            }
            var name = args[index][2..];
            if (!allowed.Contains(name)) throw new ArgumentException($"Невідомий параметр --{name}");
            if (++index >= args.Length) throw new ArgumentException($"Потрібне значення --{name}");
            if (!result.Values.TryGetValue(name, out var values)) result.Values[name] = values = [];
            if (name != "item" && values.Count != 0) throw new ArgumentException($"--{name} можна вказати лише один раз.");
            values.Add(args[index]);
        }
        if (positionals.Count > 0) result.Command = positionals[0];
        if (positionals.Count > 1) result.Id = positionals[1];
        if (positionals.Count > 2) throw new ArgumentException("Забагато позиційних аргументів.");
        if (result.Id is not null && result.Command is not ("get" or "list"))
            throw new ArgumentException("ID після команди підтримується лише для get/list.");
        result.Address = result.Single("address", result.Address);
        if (!Uri.TryCreate(result.Address, UriKind.Absolute, out var uri) || uri.Scheme is not ("http" or "https"))
            throw new ArgumentException("--address має бути URL http:// або https://");
        result.TimeoutSeconds = double.Parse(result.Single("timeout", "5"), CultureInfo.InvariantCulture);
        if (!double.IsFinite(result.TimeoutSeconds) || result.TimeoutSeconds <= 0 || result.TimeoutSeconds > 3600)
            throw new ArgumentException("--timeout має бути числом від 0 (не включно) до 3600 секунд.");
        return result;
    }
}
