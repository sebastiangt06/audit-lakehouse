using Makers.Core.Api.Application.Ports;
using Makers.Core.Api.Domain;

namespace Makers.Core.Api.Application;

public sealed record RegisterTransactionCommand(
    string TenantId,
    string From,
    string To,
    decimal Amount,
    string Currency,
    string ActorId);

public sealed record RegisterTransactionResult(Guid TransactionId, Guid EventId, DateTime OccurredAt);

/// <summary>The source account does not exist. A core never opens accounts as a side effect of a transfer.</summary>
public sealed class AccountNotFoundException(string tenantId, string number)
    : Exception($"Account '{number}' does not exist for tenant '{tenantId}'.");

/// <summary>
/// Use case: register a transfer. Plain class resolved by DI (no mediator). The audit event is not
/// published here: it is recorded by the aggregate and persisted to the outbox in the same commit.
/// </summary>
public sealed class RegisterTransactionHandler(
    IAccountRepository accounts,
    IUnitOfWork unitOfWork,
    TimeProvider clock)
{
    /// <exception cref="AccountNotFoundException">The source account does not exist.</exception>
    /// <exception cref="DomainException">A business rule was violated.</exception>
    public async Task<RegisterTransactionResult> HandleAsync(RegisterTransactionCommand command, CancellationToken cancellationToken)
    {
        var account = await accounts.FindAsync(command.TenantId, command.From, cancellationToken)
            ?? throw new AccountNotFoundException(command.TenantId, command.From);

        var now = clock.GetUtcNow().UtcDateTime;
        var transaction = account.RegisterTransfer(command.To, command.Amount, command.Currency, command.ActorId, now);
        var registered = account.DomainEvents.OfType<TransactionRegistered>().Single(e => e.TransactionId == transaction.Id);

        await unitOfWork.SaveChangesAsync(cancellationToken);

        return new RegisterTransactionResult(transaction.Id, registered.EventId, transaction.OccurredAt);
    }
}
