namespace Makers.Core.Api.Application.Ports;

/// <summary>
/// Outbound port to the event bus. Kafka is the local adapter; MSK, Kinesis or Event Hubs would be
/// other adapters behind the same contract (ADR-004).
/// </summary>
public interface IEventBusPublisher
{
    /// <summary>
    /// Publishes a batch and reports the outcome of EACH event, in the same order as
    /// <paramref name="events"/>, so the caller acknowledges exactly the ones the broker confirmed.
    /// </summary>
    Task<IReadOnlyList<PublishOutcome>> PublishAsync(IReadOnlyList<OutboundEvent> events, CancellationToken cancellationToken);

    /// <summary>
    /// Cheap reachability probe. Throws <see cref="EventBusUnavailableException"/> when the bus cannot
    /// be reached; used to tell an outage apart from messages the broker rejects.
    /// </summary>
    Task EnsureAvailableAsync(CancellationToken cancellationToken);
}

/// <summary>Event as it leaves the service: the outbox row, independent of the broker.</summary>
public sealed record OutboundEvent(
    Guid EventId,
    string EventType,
    string AggregateType,
    string AggregateId,
    string TenantId,
    string ActorId,
    DateTime OccurredAt,
    int SchemaVersion,
    string Payload,
    string? TraceId);

public sealed record PublishOutcome(Guid EventId, bool Delivered, string? Error);

public sealed class EventBusUnavailableException(string message, Exception? innerException = null)
    : Exception(message, innerException);
