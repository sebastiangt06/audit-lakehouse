using Makers.Core.Api.Application;
using Makers.Core.Api.Application.Ports;
using Makers.Core.Api.Infrastructure.Messaging;
using Makers.Core.Api.Infrastructure.Outbox;
using Makers.Core.Api.Infrastructure.Persistence;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using Polly;
using Polly.Retry;

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

    /// <summary>Role=Relay only: outbox → Kafka. The API process never registers this.</summary>
    public static IServiceCollection AddOutboxRelay(this IServiceCollection services, IConfiguration configuration)
    {
        services.AddOptions<KafkaOptions>()
            .Bind(configuration.GetSection(KafkaOptions.SectionName))
            .Validate(o => !string.IsNullOrWhiteSpace(o.BootstrapServers) && !string.IsNullOrWhiteSpace(o.AuditTopic),
                "Kafka:BootstrapServers and Kafka:AuditTopic are required.")
            .ValidateOnStart();

        services.AddOptions<OutboxRelayOptions>()
            .Bind(configuration.GetSection(OutboxRelayOptions.SectionName))
            .Validate(o => o.BatchSize > 0 && o.LeaseSeconds > 0 && o.PollIntervalMs > 0 && o.DeadLetterAttempts > 0,
                "OutboxRelay settings must be positive.")
            .Validate<IOptions<KafkaOptions>>((relay, kafka) => relay.LeaseSeconds * 1000 > kafka.Value.MessageTimeoutMs,
                "OutboxRelay:LeaseSeconds must exceed Kafka:MessageTimeoutMs, or leases expire with messages still in flight.")
            .ValidateOnStart();

        services.AddSingleton<IEventBusPublisher, KafkaEventBusPublisher>();

        // Exponential backoff + jitter, no attempt limit: while Kafka or SQL Server is down the relay
        // waits 1s, 2s, 4s ... up to 30s between tries, and resumes on its own when they come back.
        services.AddResiliencePipeline(OutboxRelay.PipelineName, (pipeline, context) =>
        {
            var logger = context.ServiceProvider.GetRequiredService<ILogger<OutboxRelay>>();
            pipeline.AddRetry(new RetryStrategyOptions
            {
                ShouldHandle = new PredicateBuilder().Handle<Exception>(ex => ex is not OperationCanceledException),
                BackoffType = DelayBackoffType.Exponential,
                UseJitter = true,
                Delay = TimeSpan.FromSeconds(1),
                MaxDelay = TimeSpan.FromSeconds(30),
                MaxRetryAttempts = int.MaxValue,
                OnRetry = args =>
                {
                    OutboxRelay.LogBackingOff(logger, args.AttemptNumber + 1, (long)args.RetryDelay.TotalMilliseconds, args.Outcome.Exception?.Message);
                    return default;
                },
            });
        });

        services.AddHostedService<OutboxRelay>();

        return services;
    }
}
