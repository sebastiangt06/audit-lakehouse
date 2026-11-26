using Makers.Core.Api.Application.Ports;
using Makers.Core.Api.Infrastructure.Persistence;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using Polly.Registry;

namespace Makers.Core.Api.Infrastructure.Outbox;

public sealed class OutboxRelayOptions
{
    public const string SectionName = "OutboxRelay";

    public int BatchSize { get; set; } = 200;

    public int LeaseSeconds { get; set; } = 30;

    public int PollIntervalMs { get; set; } = 1000;

    /// <summary>From this many attempts a failing row is logged as a dead-letter candidate.</summary>
    public int DeadLetterAttempts { get; set; } = 10;
}

/// <summary>
/// Outbox relay (ADR-002): claim → publish → ack, in three SHORT, independent statements.
/// <para>
/// Why no open transaction: the classic "BEGIN TRAN; SELECT ... WITH (UPDLOCK); publish; UPDATE;
/// COMMIT" keeps locks on the hottest table of the OLTP for as long as Kafka takes to answer. If Kafka
/// slows down or is unreachable, those locks pile up and the business transactions (the ones
/// inserting into the outbox) start waiting. Here the claim is a single autocommit UPDATE that sets a
/// lease (LeaseUntil = now + N s): its locks last milliseconds, the network I/O happens with NO
/// transaction and NO locks, and READPAST lets several relay instances claim disjoint batches.
/// </para>
/// <para>
/// Why delivery is at-least-once: Kafka's ack and our UPDATE ProcessedAt cannot be atomic (two
/// systems, no distributed transaction). If the relay dies — or SQL Server fails — after Kafka acked
/// but before ProcessedAt is written, the lease expires and the row is published again. We accept
/// that duplicate on purpose and resolve it downstream: bronze keeps it, silver dedupes by event_id
/// (ADR-005). The opposite choice (mark first, publish later) would be at-most-once: losing audit
/// events is not acceptable, duplicating them is.
/// </para>
/// </summary>
public sealed partial class OutboxRelay(
    IServiceScopeFactory scopeFactory,
    IEventBusPublisher publisher,
    ResiliencePipelineProvider<string> pipelineProvider,
    IOptions<OutboxRelayOptions> options,
    TimeProvider timeProvider,
    ILogger<OutboxRelay> logger) : BackgroundService
{
    public const string PipelineName = "outbox-relay";

    private const int LastErrorMaxLength = 2000; // outbox.OutboxMessages.LastError nvarchar(2000)

    private readonly OutboxRelayOptions _options = options.Value;
    private bool _eventBusDown;
    private int _consecutiveFailures;
    private DateTimeOffset _failingSince;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Exponential backoff with jitter, retrying forever: the relay never dies because Kafka or
        // SQL Server is down (pipeline registered in DependencyInjection.AddOutboxRelay).
        var pipeline = pipelineProvider.GetPipeline(PipelineName);
        var pollInterval = TimeSpan.FromMilliseconds(_options.PollIntervalMs);
        LogStarted(logger, _options.BatchSize, _options.LeaseSeconds, _options.PollIntervalMs);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                var claimed = await pipeline.ExecuteAsync(RelayOnceAsync, stoppingToken);

                // A full batch means there is backlog: keep draining without waiting.
                if (claimed < _options.BatchSize)
                {
                    await Task.Delay(pollInterval, timeProvider, stoppingToken);
                }
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // Shutdown. Rows claimed but not acked keep their lease and are retried after it expires.
        }
    }

    private async ValueTask<int> RelayOnceAsync(CancellationToken cancellationToken)
    {
        try
        {
            var claimed = await RelayBatchAsync(cancellationToken);
            if (_consecutiveFailures > 0)
            {
                LogRecovered(logger, _consecutiveFailures, (timeProvider.GetUtcNow() - _failingSince).TotalSeconds);
                _consecutiveFailures = 0;
            }

            return claimed;
        }
        catch (Exception) when (!cancellationToken.IsCancellationRequested)
        {
            if (_consecutiveFailures++ == 0)
            {
                _failingSince = timeProvider.GetUtcNow();
            }

            throw; // the resilience pipeline logs and backs off
        }
    }

    private async Task<int> RelayBatchAsync(CancellationToken cancellationToken)
    {
        // While the bus is known to be down, probe it instead of claiming rows: retries then test
        // Kafka for real (so the backoff grows) and Attempts is not inflated by an outage.
        if (_eventBusDown)
        {
            await publisher.EnsureAvailableAsync(cancellationToken);
            _eventBusDown = false;
        }

        await using var scope = scopeFactory.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<CoreDbContext>();

        // 1. CLAIM
        var batch = await ClaimBatchAsync(db, cancellationToken);
        if (batch.Count == 0)
        {
            return 0;
        }

        // 2. PUBLISH (no transaction, no locks held)
        var outcomes = await publisher.PublishAsync(batch.Select(ToOutboundEvent).ToList(), cancellationToken);

        // 3. ACK only what Kafka confirmed; failed rows keep their lease and are retried when it expires.
        var deliveredIds = new List<long>(batch.Count);
        var failures = new List<(OutboxMessage Message, string Error)>();
        for (var i = 0; i < batch.Count; i++)
        {
            if (outcomes[i].Delivered)
            {
                deliveredIds.Add(batch[i].Id);
            }
            else
            {
                failures.Add((batch[i], Truncate(outcomes[i].Error ?? "unknown error")));
            }
        }

        await AcknowledgeAsync(db, deliveredIds, cancellationToken);
        await RecordFailuresAsync(db, failures, cancellationToken);

        foreach (var (message, error) in failures.Where(f => f.Message.Attempts >= _options.DeadLetterAttempts))
        {
            LogDeadLetterCandidate(logger, message.EventId, message.Attempts, error);
        }

        var pending = await db.OutboxMessages.CountAsync(m => m.ProcessedAt == null, cancellationToken);
        LogBatch(logger, batch.Count, deliveredIds.Count, failures.Count, pending);

        if (deliveredIds.Count == 0)
        {
            // Nothing got through: outage or poison messages? Probe the bus. If it is reachable, the
            // broker rejected these specific messages (they retry by lease and grow Attempts); if not,
            // the probe throws and the pipeline backs off.
            _eventBusDown = true;
            await publisher.EnsureAvailableAsync(cancellationToken);
            _eventBusDown = false;
        }

        return batch.Count;
    }

    /// <summary>
    /// One autocommit statement. TOP + ORDER BY live inside the CTE (UPDATE TOP alone does not honor
    /// an order). ROWLOCK keeps locks at row level; READPAST skips rows another relay instance is
    /// claiming right now instead of waiting for them. Served by the filtered index IX_Outbox_Pending.
    /// </summary>
    private Task<List<OutboxMessage>> ClaimBatchAsync(CoreDbContext db, CancellationToken cancellationToken) =>
        db.OutboxMessages
            .FromSql($"""
                WITH batch AS (
                    SELECT TOP ({_options.BatchSize}) *
                    FROM outbox.OutboxMessages WITH (ROWLOCK, READPAST)
                    WHERE ProcessedAt IS NULL
                      AND (LeaseUntil IS NULL OR LeaseUntil < SYSUTCDATETIME())
                    ORDER BY Id
                )
                UPDATE batch
                SET LeaseUntil = DATEADD(SECOND, {_options.LeaseSeconds}, SYSUTCDATETIME()),
                    Attempts   = Attempts + 1
                OUTPUT inserted.*;
                """)
            .AsNoTracking()
            .ToListAsync(cancellationToken);

    private async Task AcknowledgeAsync(CoreDbContext db, List<long> ids, CancellationToken cancellationToken)
    {
        if (ids.Count == 0)
        {
            return;
        }

        var now = timeProvider.GetUtcNow().UtcDateTime;
        await db.OutboxMessages
            .Where(m => ids.Contains(m.Id) && m.ProcessedAt == null)
            .ExecuteUpdateAsync(s => s.SetProperty(m => m.ProcessedAt, now), cancellationToken);
    }

    private static async Task RecordFailuresAsync(CoreDbContext db, List<(OutboxMessage Message, string Error)> failures, CancellationToken cancellationToken)
    {
        // Usually every failure in a batch shares the same error (e.g. broker timeout): one UPDATE per error.
        foreach (var group in failures.GroupBy(f => f.Error, StringComparer.Ordinal))
        {
            var ids = group.Select(f => f.Message.Id).ToList();
            var error = group.Key;
            await db.OutboxMessages
                .Where(m => ids.Contains(m.Id))
                .ExecuteUpdateAsync(s => s.SetProperty(m => m.LastError, error), cancellationToken);
        }
    }

    private static OutboundEvent ToOutboundEvent(OutboxMessage m) =>
        new(m.EventId, m.EventType, m.AggregateType, m.AggregateId, m.TenantId, m.ActorId,
            m.OccurredAt, m.SchemaVersion, m.Payload, m.TraceId);

    private static string Truncate(string value) =>
        value.Length <= LastErrorMaxLength ? value : value[..LastErrorMaxLength];

    [LoggerMessage(Level = LogLevel.Information, Message = "Outbox relay started: batch={BatchSize} lease={LeaseSeconds}s poll={PollIntervalMs}ms")]
    private static partial void LogStarted(ILogger logger, int batchSize, int leaseSeconds, int pollIntervalMs);

    [LoggerMessage(Level = LogLevel.Information, Message = "Outbox batch: claimed={Claimed} delivered={Delivered} failed={Failed} pending={Pending}")]
    private static partial void LogBatch(ILogger logger, int claimed, int delivered, int failed, int pending);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Dead-letter candidate: event {EventId} failed {Attempts} attempts. LastError: {LastError}")]
    private static partial void LogDeadLetterCandidate(ILogger logger, Guid eventId, int attempts, string lastError);

    [LoggerMessage(Level = LogLevel.Information, Message = "Outbox relay recovered after {Failures} failed attempts ({DowntimeSeconds:F1}s degraded)")]
    private static partial void LogRecovered(ILogger logger, int failures, double downtimeSeconds);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Outbox relay backing off: attempt {Attempt} failed, next try in {DelayMs} ms. Cause: {Cause}")]
    internal static partial void LogBackingOff(ILogger logger, int attempt, long delayMs, string? cause);
}
