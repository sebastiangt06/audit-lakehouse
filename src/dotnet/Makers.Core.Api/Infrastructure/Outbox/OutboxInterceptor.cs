using System.Diagnostics;
using System.Text.Json;
using Makers.Core.Api.Domain;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;

namespace Makers.Core.Api.Infrastructure.Outbox;

/// <summary>
/// The heart of ADR-001: converts the domain events recorded by tracked aggregates into outbox rows
/// right before EF Core writes, so they are INSERTed by the very same SaveChanges — and therefore
/// in the very same database transaction — as the business change. Either both commit or neither
/// does: there is no dual-write and no window in which a transaction exists without its audit event.
/// Nothing here talks to Kafka; publishing is the relay's job, outside the request path.
/// </summary>
public sealed class OutboxInterceptor : SaveChangesInterceptor
{
    private static readonly JsonSerializerOptions PayloadJson = new(JsonSerializerDefaults.Web);

    public override InterceptionResult<int> SavingChanges(DbContextEventData eventData, InterceptionResult<int> result)
    {
        EnqueueOutboxMessages(eventData.Context);
        return base.SavingChanges(eventData, result);
    }

    public override ValueTask<InterceptionResult<int>> SavingChangesAsync(
        DbContextEventData eventData,
        InterceptionResult<int> result,
        CancellationToken cancellationToken = default)
    {
        EnqueueOutboxMessages(eventData.Context);
        return base.SavingChangesAsync(eventData, result, cancellationToken);
    }

    private static void EnqueueOutboxMessages(DbContext? context)
    {
        if (context is null)
        {
            return;
        }

        var aggregates = context.ChangeTracker
            .Entries<AggregateRoot>()
            .Select(entry => entry.Entity)
            .Where(aggregate => aggregate.DomainEvents.Count > 0)
            .ToList();

        var traceId = Activity.Current?.TraceId.ToString();

        foreach (var aggregate in aggregates)
        {
            foreach (var domainEvent in aggregate.DomainEvents)
            {
                context.Set<OutboxMessage>().Add(new OutboxMessage
                {
                    EventId = domainEvent.EventId,
                    EventType = domainEvent.EventType,
                    AggregateType = aggregate.AggregateType,
                    AggregateId = aggregate.AggregateId,
                    TenantId = domainEvent.TenantId,
                    ActorId = domainEvent.ActorId,
                    OccurredAt = domainEvent.OccurredAt,
                    SchemaVersion = domainEvent.SchemaVersion,
                    Payload = JsonSerializer.Serialize(domainEvent, domainEvent.GetType(), PayloadJson),
                    TraceId = traceId,
                });
            }

            // Cleared once copied into the outbox, so a second SaveChanges on the same context
            // cannot enqueue the same event twice. If the commit fails, the request fails as a whole.
            aggregate.ClearDomainEvents();
        }
    }
}
