using System.Diagnostics;
using System.Globalization;
using System.Net.Http.Json;

// Load generator for POST /transactions. Prints a summary (status codes, throughput, latency
// percentiles) used by the cost-benefit analysis in step 12. Exit code != 0 on any 5xx or
// transport error, so `make load` fails loudly when the core is unhealthy under load.

if (!LoadOptions.TryParse(args, out var options, out var error))
{
    Console.Error.WriteLine(error);
    Console.Error.WriteLine("usage: loadgen --count N --parallel P [--base-url http://core-api:8080]");
    return 2;
}

using var handler = new SocketsHttpHandler
{
    // One pooled connection per worker; recycled so DNS changes (container restarts) are picked up.
    MaxConnectionsPerServer = options.Parallel,
    PooledConnectionLifetime = TimeSpan.FromMinutes(2),
};
using var http = new HttpClient(handler) { BaseAddress = options.BaseUrl, Timeout = TimeSpan.FromSeconds(30) };
http.DefaultRequestHeaders.Add("X-Actor-Id", "loadgen");

// Per-request results in pre-sized arrays: each worker writes only its own index, so no locking.
var statusCodes = new int[options.Count]; // 0 = transport error (timeout, connection refused)
var latenciesMs = new double[options.Count];
var highAmounts = 0;

Console.WriteLine(Invariant($"loadgen: {options.Count} requests, parallel {options.Parallel}, target {options.BaseUrl}"));

var started = Stopwatch.GetTimestamp();
await Parallel.ForEachAsync(
    Enumerable.Range(0, options.Count),
    new ParallelOptions { MaxDegreeOfParallelism = options.Parallel },
    async (i, cancellationToken) =>
    {
        var request = TransferRequest.Random();
        if (request.Amount >= TransferRequest.HighAmountThreshold)
        {
            Interlocked.Increment(ref highAmounts);
        }

        var requestStarted = Stopwatch.GetTimestamp();
        try
        {
            using var response = await http.PostAsJsonAsync("transactions", request, cancellationToken);
            statusCodes[i] = (int)response.StatusCode;
        }
        catch (HttpRequestException)
        {
            statusCodes[i] = 0;
        }
        catch (TaskCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            statusCodes[i] = 0; // HttpClient timeout
        }

        latenciesMs[i] = Stopwatch.GetElapsedTime(requestStarted).TotalMilliseconds;
    });
var elapsed = Stopwatch.GetElapsedTime(started);

// Latency percentiles only over requests that got an HTTP response.
var answered = latenciesMs.Where((_, i) => statusCodes[i] != 0).Order().ToArray();

Console.WriteLine(Invariant($"total        {options.Count}"));
foreach (var group in statusCodes.GroupBy(code => code).OrderBy(g => g.Key))
{
    var label = group.Key == 0 ? "transport error" : Invariant($"HTTP {group.Key}");
    Console.WriteLine(Invariant($"  {label,-15} {group.Count()}"));
}

Console.WriteLine(Invariant($"high amount  {highAmounts} (>= {TransferRequest.HighAmountThreshold:N0} COP)"));
Console.WriteLine(Invariant($"elapsed      {elapsed.TotalSeconds:F2} s"));
Console.WriteLine(Invariant($"throughput   {options.Count / elapsed.TotalSeconds:F1} req/s"));
Console.WriteLine(answered.Length == 0
    ? "latency ms   n/a (no responses)"
    : Invariant($"latency ms   p50={Percentile(answered, 50):F1} p95={Percentile(answered, 95):F1} p99={Percentile(answered, 99):F1} max={answered[^1]:F1}"));

var failed = statusCodes.Count(code => code is 0 or >= 500);
if (failed > 0)
{
    Console.Error.WriteLine(Invariant($"FAILED: {failed} requests with 5xx or transport error"));
    return 1;
}

return 0;

// Nearest-rank percentile over an ascending-sorted array.
static double Percentile(double[] sorted, int percentile)
{
    var rank = (int)Math.Ceiling(percentile / 100.0 * sorted.Length);
    return sorted[Math.Clamp(rank, 1, sorted.Length) - 1];
}

static string Invariant(FormattableString value) => value.ToString(CultureInfo.InvariantCulture);

internal sealed record LoadOptions(int Count, int Parallel, Uri BaseUrl)
{
    public static bool TryParse(string[] args, out LoadOptions options, out string? error)
    {
        var count = 1000;
        var parallel = 10;
        var baseUrl = "http://core-api:8080";
        options = new LoadOptions(count, parallel, new Uri(baseUrl + "/"));
        error = null;

        for (var i = 0; i < args.Length; i++)
        {
            var value = i + 1 < args.Length ? args[i + 1] : null;
            switch (args[i])
            {
                case "--count" when int.TryParse(value, CultureInfo.InvariantCulture, out var c) && c > 0:
                    count = c;
                    i++;
                    break;
                case "--parallel" when int.TryParse(value, CultureInfo.InvariantCulture, out var p) && p > 0:
                    parallel = p;
                    i++;
                    break;
                case "--base-url" when Uri.TryCreate(value, UriKind.Absolute, out _):
                    baseUrl = value!;
                    i++;
                    break;
                default:
                    error = $"invalid or incomplete argument: {args[i]} {value}";
                    return false;
            }
        }

        // Trailing slash so the relative "transactions" path is appended, not substituted.
        options = new LoadOptions(count, parallel, new Uri(baseUrl.TrimEnd('/') + "/"));
        return true;
    }
}

/// <summary>Body of POST /transactions. Accounts A1..A50 / B1..B50 for t1..t5 are seeded by init.sql.</summary>
internal sealed record TransferRequest(string TenantId, string From, string To, decimal Amount, string Currency)
{
    /// <summary>Threshold of the gold "high-value transfers" report.</summary>
    public const decimal HighAmountThreshold = 10_000_000m;

    public static TransferRequest Random()
    {
        var rng = System.Random.Shared;
        // ~10% high-value transfers (10M..200M COP), the rest 1k..10M COP. Whole pesos.
        decimal amount = rng.NextDouble() < 0.10
            ? rng.NextInt64(10_000_000, 200_000_001)
            : rng.NextInt64(1_000, 10_000_000);

        return new TransferRequest(
            TenantId: $"t{rng.Next(1, 6)}",
            From: $"A{rng.Next(1, 51)}",
            To: $"B{rng.Next(1, 51)}",
            Amount: amount,
            Currency: "COP");
    }
}
