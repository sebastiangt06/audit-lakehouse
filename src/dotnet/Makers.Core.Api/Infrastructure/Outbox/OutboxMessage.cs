namespace Makers.Core.Api.Infrastructure.Outbox;

/// <summary>
/// Row of outbox.OutboxMessages. Written in the same transaction as the business change;
/// the relay (step 03) claims rows by lease and marks ProcessedAt after Kafka acks.
/// </summary>
public sealed class OutboxMessage
{
    public long Id { get; set; }

    public Guid EventId { get; set; }

    public required string EventType { get; set; }

    public required string AggregateType { get; set; }

    public required string AggregateId { get; set; }

    public required string TenantId { get; set; }

    public required string ActorId { get; set; }

    public DateTime OccurredAt { get; set; }

    public int SchemaVersion { get; set; }

    /// <summary>JSON document of the event (camelCase).</summary>
    public required string Payload { get; set; }

    public string? TraceId { get; set; }

    public DateTime? ProcessedAt { get; set; }

    public DateTime? LeaseUntil { get; set; }

    public int Attempts { get; set; }

    public string? LastError { get; set; }
}
