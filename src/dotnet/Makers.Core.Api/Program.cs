using Makers.Core.Api.Application;
using Makers.Core.Api.Domain;
using Makers.Core.Api.Infrastructure;

// Chiseled images have no shell or curl: the container healthcheck runs this same binary in
// "healthcheck" mode, which probes /health and returns an exit code.
if (args is ["healthcheck", ..])
{
    var port = Environment.GetEnvironmentVariable("ASPNETCORE_HTTP_PORTS")?.Split(';')[0] ?? "8080";
    using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(3) };
    try
    {
        using var response = await http.GetAsync(new Uri($"http://localhost:{port}/health"));
        return response.IsSuccessStatusCode ? 0 : 1;
    }
    catch (HttpRequestException)
    {
        return 1;
    }
    catch (TaskCanceledException)
    {
        return 1;
    }
}

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddCore(builder.Configuration);
builder.Services.AddProblemDetails();

var app = builder.Build();

app.MapHealthChecks("/health");

app.MapPost("/transactions", async (
    TransactionRequest request,
    HttpContext http,
    RegisterTransactionHandler handler,
    CancellationToken cancellationToken) =>
{
    var errors = request.Validate();
    if (errors.Count > 0)
    {
        return Results.ValidationProblem(errors);
    }

    var actorId = http.Request.Headers["X-Actor-Id"].FirstOrDefault() is { Length: > 0 } actor ? actor : "anonymous";
    var command = new RegisterTransactionCommand(
        request.TenantId!, request.From!, request.To!, request.Amount!.Value, request.Currency!.ToUpperInvariant(), actorId);

    try
    {
        var result = await handler.HandleAsync(command, cancellationToken);
        return Results.Created($"/transactions/{result.TransactionId}", result);
    }
    catch (AccountNotFoundException ex)
    {
        return Results.Problem(ex.Message, statusCode: StatusCodes.Status422UnprocessableEntity, title: "Unknown source account");
    }
    catch (DomainException ex)
    {
        return Results.Problem(ex.Message, statusCode: StatusCodes.Status400BadRequest);
    }
});

await app.RunAsync();
return 0;

internal sealed record TransactionRequest(string? TenantId, string? From, string? To, decimal? Amount, string? Currency)
{
    public Dictionary<string, string[]> Validate()
    {
        var errors = new Dictionary<string, string[]>();
        if (string.IsNullOrWhiteSpace(TenantId))
        {
            errors[nameof(TenantId)] = ["tenantId is required."];
        }

        if (string.IsNullOrWhiteSpace(From))
        {
            errors[nameof(From)] = ["from is required."];
        }

        if (string.IsNullOrWhiteSpace(To))
        {
            errors[nameof(To)] = ["to is required."];
        }

        if (Amount is null or <= 0)
        {
            errors[nameof(Amount)] = ["amount must be greater than zero."];
        }

        if (string.IsNullOrWhiteSpace(Currency))
        {
            errors[nameof(Currency)] = ["currency is required."];
        }

        return errors;
    }
}
