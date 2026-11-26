using Makers.Core.Api.Application.Ports;
using Makers.Core.Api.Domain;
using Microsoft.EntityFrameworkCore;

namespace Makers.Core.Api.Infrastructure.Persistence;

public sealed class AccountRepository(CoreDbContext db) : IAccountRepository
{
    // Transaction history is deliberately not loaded: registering a transfer only appends.
    public Task<Account?> FindAsync(string tenantId, string number, CancellationToken cancellationToken) =>
        db.Accounts.SingleOrDefaultAsync(a => a.TenantId == tenantId && a.Number == number, cancellationToken);

    public void Add(Account account) => db.Accounts.Add(account);
}
