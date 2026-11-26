using Makers.Core.Api.Domain;

namespace Makers.Core.Api.Application.Ports;

public interface IAccountRepository
{
    Task<Account?> FindAsync(string tenantId, string number, CancellationToken cancellationToken);

    void Add(Account account);
}
