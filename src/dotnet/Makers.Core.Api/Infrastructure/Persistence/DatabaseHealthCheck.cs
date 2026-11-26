using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace Makers.Core.Api.Infrastructure.Persistence;

public sealed class DatabaseHealthCheck(CoreDbContext db) : IHealthCheck
{
    public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default) =>
        await db.Database.CanConnectAsync(cancellationToken)
            ? HealthCheckResult.Healthy()
            : HealthCheckResult.Unhealthy("SQL Server is not reachable.");
}
