namespace Makers.Core.Api.Domain;

/// <summary>
/// Marker for domain events. Every event carries its own id (UUIDv7, time-ordered) and the instant
/// it happened, so the outbox row and every downstream copy share the same identity.
/// </summary>
public interface IDomainEvent
{
    Guid EventId { get; }
    DateTime OccurredAt { get; }
    string TenantId { get; }
    string ActorId { get; }
    string EventType { get; }
    int SchemaVersion { get; }
}

/// <summary>
/// Aggregates record events instead of publishing them. Publishing is an infrastructure concern:
/// the outbox interceptor turns recorded events into rows inside the same SaveChanges.
/// </summary>
public abstract class AggregateRoot
{
    private readonly List<IDomainEvent> _domainEvents = [];

    public IReadOnlyCollection<IDomainEvent> DomainEvents => _domainEvents;

    public abstract string AggregateType { get; }

    public abstract string AggregateId { get; }

    protected void Raise(IDomainEvent domainEvent) => _domainEvents.Add(domainEvent);

    public void ClearDomainEvents() => _domainEvents.Clear();
}
