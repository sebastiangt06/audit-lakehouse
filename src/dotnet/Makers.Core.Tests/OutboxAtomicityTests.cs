using System.Data.Common;
using Makers.Core.Api.Domain;
using Makers.Core.Api.Infrastructure.Outbox;
using Makers.Core.Api.Infrastructure.Persistence;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;

namespace Makers.Core.Tests;

/// <summary>
/// ADR-001 guarantee: the business row and its outbox row commit together or not at all.
/// Runs against the compose SQL Server on purpose (not SQLite / InMemory): what is under test is
/// SQL Server's real transaction semantics with the production model and interceptor. Without a
/// reachable server the test is reported as SKIPPED, never as passed.
/// </summary>
public sealed class OutboxAtomicityTests
{
    /// <summary>Connection string to the core database; set by scripts/test.sh.</summary>
    private const string ConnectionVariable = "CORE_TEST_CONNECTION";

    [Fact]
    public async Task SaveChanges_FailingAfterInsertsBeforeCommit_PersistsNeitherTransactionNorOutboxRow()
    {
        var ct = TestContext.Current.CancellationToken;
        var connectionString = await RequireSqlServerAsync(ct);
        var tenant = $"test-{Guid.NewGuid():N}"; // own tenant: never collides with t1..t5 or other runs

        try
        {
            await using (var seed = CreateContext(connectionString))
            {
                seed.Accounts.Add(Account.Open(tenant, "A1", DateTime.UtcNow));
                await seed.SaveChangesAsync(ct);
            }

            var failBeforeCommit = new FailBeforeCommitInterceptor(tenant);
            Exception? failure = null;
            await using (var db = CreateContext(connectionString, failBeforeCommit))
            {
                var account = await db.Accounts.SingleAsync(a => a.TenantId == tenant && a.Number == "A1", ct);
                account.RegisterTransfer("B1", 150_000m, "COP", "atomicity-test", DateTime.UtcNow);

                try
                {
                    await db.SaveChangesAsync(ct);
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    failure = ex;
                }
            }

            // If EF ever stopped wrapping the two INSERTs in one transaction, the hook would not run.
            Assert.True(failBeforeCommit.Invoked, "SaveChanges committed without a database transaction.");
            // Not vacuous: both rows really were written inside the transaction before it failed.
            Assert.Equal((Transactions: 1, OutboxRows: 1), failBeforeCommit.RowsInsideTransaction);
            // The failure is a real constraint violation (PK_Accounts), surfaced to the caller.
            Assert.Equal(2627, FindSqlException(failure)?.Number);

            // Fresh connection: after the rollback, neither row exists.
            await using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync(ct);
            Assert.Equal((Transactions: 0, OutboxRows: 0), await CountRowsAsync(connection, null, tenant, ct));
        }
        finally
        {
            await CleanupAsync(connectionString, tenant);
        }
    }

    private static async Task<string> RequireSqlServerAsync(CancellationToken ct)
    {
        var connectionString = Environment.GetEnvironmentVariable(ConnectionVariable);
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            Assert.Skip($"{ConnectionVariable} is not set. Run `make test` with the compose stack up.");
        }

        try
        {
            var probe = new SqlConnectionStringBuilder(connectionString) { ConnectTimeout = 5 };
            await using var connection = new SqlConnection(probe.ConnectionString);
            await connection.OpenAsync(ct);
        }
        catch (SqlException ex)
        {
            Assert.Skip($"SQL Server not reachable ({ex.Message}). Run `make up` first.");
        }

        return connectionString;
    }

    private static CoreDbContext CreateContext(string connectionString, params IInterceptor[] extra) =>
        new(new DbContextOptionsBuilder<CoreDbContext>()
            .UseSqlServer(connectionString, sql => sql.UseCompatibilityLevel(160))
            .AddInterceptors([new OutboxInterceptor(), .. extra])
            .Options);

    private static async Task<(int Transactions, int OutboxRows)> CountRowsAsync(
        DbConnection connection, DbTransaction? transaction, string tenant, CancellationToken ct)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            SELECT (SELECT COUNT(*) FROM core.Transactions WHERE TenantId = @tenant),
                   (SELECT COUNT(*) FROM outbox.OutboxMessages WHERE TenantId = @tenant)
            """;
        AddTenant(command, tenant);
        await using var reader = await command.ExecuteReaderAsync(ct);
        await reader.ReadAsync(ct);
        return (reader.GetInt32(0), reader.GetInt32(1));
    }

    private static async Task CleanupAsync(string connectionString, string tenant)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            DELETE FROM outbox.OutboxMessages WHERE TenantId = @tenant;
            DELETE FROM core.Transactions WHERE TenantId = @tenant;
            DELETE FROM core.Accounts WHERE TenantId = @tenant;
            """;
        AddTenant(command, tenant);
        await command.ExecuteNonQueryAsync();
    }

    private static void AddTenant(DbCommand command, string tenant)
    {
        var parameter = command.CreateParameter();
        parameter.ParameterName = "@tenant";
        parameter.Value = tenant;
        command.Parameters.Add(parameter);
    }

    private static SqlException? FindSqlException(Exception? exception)
    {
        for (var current = exception; current is not null; current = current.InnerException)
        {
            if (current is SqlException sql)
            {
                return sql;
            }
        }

        return null;
    }

    /// <summary>
    /// Runs when SaveChanges has executed every INSERT but has not committed yet. It records what
    /// the transaction can see and then triggers a real constraint violation (duplicate PK on
    /// core.Accounts) on the same transaction, so SaveChanges fails and EF rolls back.
    /// Deterministic, unlike a bad INSERT inside the batch, whose position EF does not guarantee.
    /// </summary>
    private sealed class FailBeforeCommitInterceptor(string tenant) : DbTransactionInterceptor
    {
        public bool Invoked { get; private set; }

        public (int Transactions, int OutboxRows) RowsInsideTransaction { get; private set; }

        public override async ValueTask<InterceptionResult> TransactionCommittingAsync(
            DbTransaction transaction,
            TransactionEventData eventData,
            InterceptionResult result,
            CancellationToken cancellationToken = default)
        {
            Invoked = true;
            var connection = transaction.Connection!;
            RowsInsideTransaction = await CountRowsAsync(connection, transaction, tenant, cancellationToken);

            await using var violate = connection.CreateCommand();
            violate.Transaction = transaction;
            violate.CommandText = "INSERT INTO core.Accounts (TenantId, Number, OpenedAt) VALUES (@tenant, N'A1', SYSUTCDATETIME());";
            AddTenant(violate, tenant);
            await violate.ExecuteNonQueryAsync(cancellationToken); // throws SqlException 2627

            return result;
        }
    }
}
