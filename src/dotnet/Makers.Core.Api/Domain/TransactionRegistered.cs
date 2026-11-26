namespace Makers.Core.Api.Domain;

/// <summary>
/// Audit event contract, schema version 1. Changes must be backward compatible (new optional
/// fields + SchemaVersion bump); consumers read schema_version from the Kafka header.
/// </summary>
public sealed record TransactionRegistered(
    Guid EventId,
    DateTime OccurredAt,
    Guid TransactionId,
    string TenantId,
    string ActorId,
    string From,
    string To,
    decimal Amount,
    string Currency) : IDomainEvent
{
    public const string Type = "transaction.registered";

    public string EventType => Type;

    public int SchemaVersion => 1;
}
