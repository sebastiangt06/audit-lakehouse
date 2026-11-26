namespace Makers.Core.Api.Domain;

/// <summary>
/// Account aggregate. Identified by (TenantId, Number). Registering a transfer creates a
/// <see cref="Transaction"/> and records a <see cref="TransactionRegistered"/> event.
/// </summary>
public sealed class Account : AggregateRoot
{
    private readonly List<Transaction> _transactions = [];

    // EF Core materialization.
    private Account()
    {
        TenantId = string.Empty;
        Number = string.Empty;
    }

    private Account(string tenantId, string number, DateTime openedAt)
    {
        TenantId = tenantId;
        Number = number;
        OpenedAt = openedAt;
    }

    public string TenantId { get; private set; }

    public string Number { get; private set; }

    public DateTime OpenedAt { get; private set; }

    /// <summary>Only newly registered transactions are tracked here; history is not loaded.</summary>
    public IReadOnlyCollection<Transaction> Transactions => _transactions;

    public override string AggregateType => "Account";

    public override string AggregateId => $"{TenantId}:{Number}";

    public static Account Open(string tenantId, string number, DateTime openedAt)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(tenantId);
        ArgumentException.ThrowIfNullOrWhiteSpace(number);
        return new Account(tenantId, number, openedAt);
    }

    public Transaction RegisterTransfer(string toAccount, decimal amount, string currency, string actorId, DateTime occurredAt)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(toAccount);
        ArgumentException.ThrowIfNullOrWhiteSpace(actorId);
        if (amount <= 0)
        {
            throw new DomainException("Amount must be greater than zero.");
        }

        if (string.Equals(toAccount, Number, StringComparison.Ordinal))
        {
            throw new DomainException("Source and destination accounts must differ.");
        }

        if (currency is not { Length: 3 } || !currency.All(char.IsAsciiLetterUpper))
        {
            throw new DomainException("Currency must be an ISO-4217 code (3 upper-case letters).");
        }

        var transaction = new Transaction(Guid.CreateVersion7(occurredAt), TenantId, Number, toAccount, amount, currency, occurredAt);
        _transactions.Add(transaction);

        Raise(new TransactionRegistered(
            EventId: Guid.CreateVersion7(occurredAt),
            OccurredAt: occurredAt,
            TransactionId: transaction.Id,
            TenantId: TenantId,
            ActorId: actorId,
            From: Number,
            To: toAccount,
            Amount: amount,
            Currency: currency));

        return transaction;
    }
}

public sealed class DomainException(string message) : Exception(message);
