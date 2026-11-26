using Makers.Core.Api.Domain;
using Makers.Core.Api.Infrastructure.Outbox;
using Microsoft.EntityFrameworkCore;

namespace Makers.Core.Api.Infrastructure.Persistence;

/// <summary>
/// The physical schema (indexes, filtered index, clustering) is owned by infra/sqlserver/init.sql,
/// not by EF migrations; this model only maps to it.
/// </summary>
public sealed class CoreDbContext(DbContextOptions<CoreDbContext> options) : DbContext(options)
{
    public DbSet<Account> Accounts => Set<Account>();

    public DbSet<OutboxMessage> OutboxMessages => Set<OutboxMessage>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Account>(account =>
        {
            account.ToTable("Accounts", "core");
            account.HasKey(a => new { a.TenantId, a.Number });
            account.Property(a => a.TenantId).HasMaxLength(64);
            account.Property(a => a.Number).HasMaxLength(64);
            account.Ignore(a => a.DomainEvents);

            account.HasMany(a => a.Transactions)
                .WithOne()
                .HasForeignKey(t => new { t.TenantId, t.FromAccount });
            account.Navigation(a => a.Transactions).UsePropertyAccessMode(PropertyAccessMode.Field);
        });

        modelBuilder.Entity<Transaction>(transaction =>
        {
            transaction.ToTable("Transactions", "core");
            // UUIDv7 is time-ordered, but SQL Server sorts uniqueidentifier by its LAST bytes,
            // so a clustered PK on it would fragment. Clustering is on OccurredAt (init.sql).
            transaction.HasKey(t => t.Id).IsClustered(false);
            transaction.Property(t => t.Id).ValueGeneratedNever();
            transaction.Property(t => t.TenantId).HasMaxLength(64);
            transaction.Property(t => t.FromAccount).HasMaxLength(64);
            transaction.Property(t => t.ToAccount).HasMaxLength(64);
            transaction.Property(t => t.Amount).HasPrecision(19, 4);
            transaction.Property(t => t.Currency).HasMaxLength(3).IsFixedLength();
        });

        modelBuilder.Entity<OutboxMessage>(outbox =>
        {
            outbox.ToTable("OutboxMessages", "outbox");
            outbox.HasKey(o => o.Id);
            outbox.Property(o => o.Id).UseIdentityColumn();
            outbox.Property(o => o.EventType).HasMaxLength(128);
            outbox.Property(o => o.AggregateType).HasMaxLength(64);
            outbox.Property(o => o.AggregateId).HasMaxLength(160);
            outbox.Property(o => o.TenantId).HasMaxLength(64);
            outbox.Property(o => o.ActorId).HasMaxLength(128);
            outbox.Property(o => o.TraceId).HasMaxLength(32);
        });
    }
}
