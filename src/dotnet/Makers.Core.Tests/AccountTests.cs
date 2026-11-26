using Makers.Core.Api.Domain;

namespace Makers.Core.Tests;

public sealed class AccountTests
{
    private static readonly DateTime Now = new(2026, 11, 26, 10, 0, 0, DateTimeKind.Utc);

    [Fact]
    public void RegisterTransfer_ValidTransfer_RecordsTransactionAndOneRegisteredEvent()
    {
        var account = Account.Open("t1", "A1", Now.AddDays(-30));

        var transaction = account.RegisterTransfer("B2", 150_000m, "COP", "tester", Now);

        Assert.Same(transaction, Assert.Single(account.Transactions));
        Assert.Equal(("t1", "A1", "B2", 150_000m, "COP", Now),
            (transaction.TenantId, transaction.FromAccount, transaction.ToAccount, transaction.Amount, transaction.Currency, transaction.OccurredAt));

        var registered = Assert.IsType<TransactionRegistered>(Assert.Single(account.DomainEvents));
        Assert.Equal(7, registered.EventId.Version); // UUIDv7: time-ordered, the end-to-end dedupe key
        Assert.Equal(1, registered.SchemaVersion);
        Assert.Equal(TransactionRegistered.Type, registered.EventType);
        Assert.Equal(transaction.Id, registered.TransactionId);
        Assert.Equal(("t1", "tester", "A1", "B2", 150_000m, "COP", Now),
            (registered.TenantId, registered.ActorId, registered.From, registered.To, registered.Amount, registered.Currency, registered.OccurredAt));
    }

    [Fact]
    public void RegisterTransfer_SameSourceAndDestination_ThrowsDomainException()
    {
        var account = Account.Open("t1", "A1", Now);

        Assert.Throws<DomainException>(() => account.RegisterTransfer("A1", 1_000m, "COP", "tester", Now));
        AssertNothingRecorded(account);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(-150000)]
    public void RegisterTransfer_NonPositiveAmount_ThrowsDomainException(int amount)
    {
        var account = Account.Open("t1", "A1", Now);

        Assert.Throws<DomainException>(() => account.RegisterTransfer("B2", amount, "COP", "tester", Now));
        AssertNothingRecorded(account);
    }

    // A rejected command must leave no half-state behind: no transaction, no event.
    private static void AssertNothingRecorded(Account account)
    {
        Assert.Empty(account.Transactions);
        Assert.Empty(account.DomainEvents);
    }
}
