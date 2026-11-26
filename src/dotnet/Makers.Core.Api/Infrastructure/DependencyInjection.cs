using Makers.Core.Api.Application;
using Makers.Core.Api.Application.Ports;
using Makers.Core.Api.Infrastructure.Outbox;
using Makers.Core.Api.Infrastructure.Persistence;
using Microsoft.EntityFrameworkCore;

namespace Makers.Core.Api.Infrastructure;

public static class DependencyInjection
{
    public static IServiceCollection AddCore(this IServiceCollection services, IConfiguration configuration)
    {
        var connectionString = configuration.GetConnectionString("Core")
            ?? throw new InvalidOperationException("ConnectionStrings:Core is not configured.");

        services.AddSingleton(TimeProvider.System);
        services.AddSingleton<OutboxInterceptor>();

        services.AddDbContext<CoreDbContext>((sp, options) =>
        {
            options.UseSqlServer(connectionString, sql => sql.UseCompatibilityLevel(160)); // SQL Server 2022
            options.AddInterceptors(sp.GetRequiredService<OutboxInterceptor>());
        });

        services.AddScoped<IAccountRepository, AccountRepository>();
        services.AddScoped<IUnitOfWork, EfUnitOfWork>();
        services.AddScoped<RegisterTransactionHandler>();

        services.AddHealthChecks().AddCheck<DatabaseHealthCheck>("sqlserver");

        return services;
    }
}
