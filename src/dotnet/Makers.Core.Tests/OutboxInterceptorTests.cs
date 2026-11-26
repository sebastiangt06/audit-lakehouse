using Makers.Core.Api.Domain;
using Makers.Core.Api.Infrastructure.Outbox;
using Makers.Core.Api.Infrastructure.Persistence;
using Microsoft.EntityFrameworkCore;

namespace Makers.Core.Tests;

/// <summary>
/// Event → outbox row mapping. The interceptor runs inside SaveChanges regardless of the provider,
/// so EF InMemory is enough here; transactional atomicity is covered against real SQL Server in
/// <see cref="OutboxAtomicityTests"/> (InMemory has no transactions).
/// </summary>
public sealed class OutboxInterceptorTests
{
    [Fact]
    public async Task SaveChanges_WithOneDomainEvent_WritesExactlyOneMatchingOutboxRow()
    {
        var options = new DbContextOptionsBuilder<CoreDbContext>()
            .UseInMemoryDatabase($"outbox-{Guid.NewGuid()}")
            .AddInterceptors(new OutboxInterceptor())
            .Options;
        var ct = TestContext.Current.CancellationToken;

        await using (var db = new CoreDbContext(options))
        {
            var account = Account.Open("t1", "A1", DateTime.UtcNow);
            db.Accounts.Add(account);
            account.RegisterTransfer("B2", 150_000m, "COP", "tester", DateTime.UtcNow);
            var registered = Assert.IsType<TransactionRegistered>(Assert.Single(account.DomainEvents));

            await db.SaveChangesAsync(ct);

            Assert.Empty(account.DomainEvents); // cleared: a second SaveChanges cannot enqueue it again

            // Second SaveChanges on the same context: still exactly one row.
            await db.SaveChangesAsync(ct);

            await using var verify = new CoreDbContext(options);
            var row = Assert.Single(await verify.OutboxMessages.ToListAsync(ct));
            Assert.Equal(registered.EventId, row.EventId);
            Assert.Equal(registered.TenantId, row.TenantId);
            Assert.Equal(registered.EventType, row.EventType);
            Assert.Equal(registered.SchemaVersion, row.SchemaVersion);
            Assert.Equal("Account", row.AggregateType);
            Assert.Equal("t1:A1", row.AggregateId);
            Assert.Null(row.ProcessedAt);
        }
    }
}
