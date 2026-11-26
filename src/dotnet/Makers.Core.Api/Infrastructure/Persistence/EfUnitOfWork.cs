using Makers.Core.Api.Application.Ports;

namespace Makers.Core.Api.Infrastructure.Persistence;

public sealed class EfUnitOfWork(CoreDbContext db) : IUnitOfWork
{
    // One SaveChanges = one transaction: business rows + outbox rows (OutboxInterceptor).
    public Task SaveChangesAsync(CancellationToken cancellationToken) => db.SaveChangesAsync(cancellationToken);
}
