namespace Makers.Core.Api.Application.Ports;

public interface IUnitOfWork
{
    /// <summary>
    /// Commits business changes and their outbox messages atomically (one database transaction).
    /// </summary>
    Task SaveChangesAsync(CancellationToken cancellationToken);
}
