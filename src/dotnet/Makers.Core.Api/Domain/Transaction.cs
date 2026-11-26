namespace Makers.Core.Api.Domain;

/// <summary>A transfer registered on an <see cref="Account"/>. Immutable once created.</summary>
public sealed class Transaction
{
    // EF Core materialization.
    private Transaction()
    {
        TenantId = string.Empty;
        FromAccount = string.Empty;
        ToAccount = string.Empty;
        Currency = string.Empty;
    }

    internal Transaction(Guid id, string tenantId, string fromAccount, string toAccount, decimal amount, string currency, DateTime occurredAt)
    {
        Id = id;
        TenantId = tenantId;
        FromAccount = fromAccount;
        ToAccount = toAccount;
        Amount = amount;
        Currency = currency;
        OccurredAt = occurredAt;
    }

    public Guid Id { get; private set; }

    public string TenantId { get; private set; }

    public string FromAccount { get; private set; }

    public string ToAccount { get; private set; }

    public decimal Amount { get; private set; }

    public string Currency { get; private set; }

    public DateTime OccurredAt { get; private set; }
}
