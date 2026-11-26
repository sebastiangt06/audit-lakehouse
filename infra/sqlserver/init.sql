/*
  Core schema for the PoC. Idempotent: safe to run on every `docker compose up`.
  Run by the `sqlserver-init` service:  sqlcmd -I -v CORE_DB=... -i init.sql
  (-I = QUOTED_IDENTIFIER ON, required to create/maintain filtered indexes.)

  The physical schema lives here, not in EF migrations, so clustering and the filtered index
  are explicit and reviewable by a DBA.
*/
SET NOCOUNT ON;
SET QUOTED_IDENTIFIER ON;
SET ANSI_NULLS ON;
GO

IF DB_ID(N'$(CORE_DB)') IS NULL
BEGIN
    PRINT 'Creating database $(CORE_DB)';
    CREATE DATABASE [$(CORE_DB)];
END
GO

-- Read Committed Snapshot: readers do not block writers on the hot OLTP tables.
IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = N'$(CORE_DB)' AND is_read_committed_snapshot_on = 1)
    ALTER DATABASE [$(CORE_DB)] SET READ_COMMITTED_SNAPSHOT ON WITH ROLLBACK IMMEDIATE;
GO

USE [$(CORE_DB)];
GO

IF SCHEMA_ID(N'core')   IS NULL EXEC (N'CREATE SCHEMA core');
IF SCHEMA_ID(N'outbox') IS NULL EXEC (N'CREATE SCHEMA outbox');
IF SCHEMA_ID(N'audit')  IS NULL EXEC (N'CREATE SCHEMA audit');
GO

/* ---------------------------------------------------------------------------------------------
   core: transactional business tables
--------------------------------------------------------------------------------------------- */
IF OBJECT_ID(N'core.Accounts', N'U') IS NULL
CREATE TABLE core.Accounts
(
    TenantId  nvarchar(64) NOT NULL,
    Number    nvarchar(64) NOT NULL,
    OpenedAt  datetime2(7) NOT NULL,
    CONSTRAINT PK_Accounts PRIMARY KEY CLUSTERED (TenantId, Number)
);
GO

IF OBJECT_ID(N'core.Transactions', N'U') IS NULL
BEGIN
    CREATE TABLE core.Transactions
    (
        -- UUIDv7 (Guid.CreateVersion7). SQL Server orders uniqueidentifier by its last 6 bytes,
        -- so it is NOT sequential here: PK is nonclustered, clustering is on OccurredAt.
        Id          uniqueidentifier NOT NULL,
        TenantId    nvarchar(64)     NOT NULL,
        FromAccount nvarchar(64)     NOT NULL,
        ToAccount   nvarchar(64)     NOT NULL,
        Amount      decimal(19,4)    NOT NULL,
        Currency    nchar(3)         NOT NULL,
        OccurredAt  datetime2(7)     NOT NULL,
        CONSTRAINT PK_Transactions PRIMARY KEY NONCLUSTERED (Id),
        CONSTRAINT FK_Transactions_Accounts FOREIGN KEY (TenantId, FromAccount)
            REFERENCES core.Accounts (TenantId, Number)
    );
    CREATE CLUSTERED INDEX CX_Transactions_OccurredAt ON core.Transactions (OccurredAt);
END
GO

-- Seed accounts A1..A50 and B1..B50 for tenants t1..t5 (500 rows). Idempotent: only missing ones.
-- The API never opens accounts on a transfer (an unknown source account is a 422).
WITH t AS (SELECT CONCAT(N't', v) AS TenantId FROM (VALUES (1),(2),(3),(4),(5)) x(v)),
     p AS (SELECT Prefix FROM (VALUES (N'A'),(N'B')) x(Prefix)),
     n AS (SELECT TOP (50) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS i FROM sys.all_objects)
INSERT INTO core.Accounts (TenantId, Number, OpenedAt)
SELECT t.TenantId, CONCAT(p.Prefix, n.i), SYSUTCDATETIME()
FROM t CROSS JOIN p CROSS JOIN n
WHERE NOT EXISTS (SELECT 1 FROM core.Accounts a WHERE a.TenantId = t.TenantId AND a.Number = CONCAT(p.Prefix, n.i));
GO

/* ---------------------------------------------------------------------------------------------
   outbox: audit events written in the SAME transaction as the business change (ADR-001)
--------------------------------------------------------------------------------------------- */
IF OBJECT_ID(N'outbox.OutboxMessages', N'U') IS NULL
CREATE TABLE outbox.OutboxMessages
(
    Id            bigint IDENTITY(1,1) NOT NULL,   -- monotonic: clustered key + relay claim order
    EventId       uniqueidentifier     NOT NULL,   -- UUIDv7, end-to-end identity (dedupe key)
    EventType     nvarchar(128)        NOT NULL,
    AggregateType nvarchar(64)         NOT NULL,
    AggregateId   nvarchar(160)        NOT NULL,   -- Kafka message key (per-aggregate ordering)
    TenantId      nvarchar(64)         NOT NULL,
    ActorId       nvarchar(128)        NOT NULL,
    OccurredAt    datetime2(7)         NOT NULL,
    SchemaVersion int                  NOT NULL,
    Payload       nvarchar(max)        NOT NULL,
    TraceId       nvarchar(32)         NULL,
    ProcessedAt   datetime2(7)         NULL,       -- set by the relay after Kafka acks
    LeaseUntil    datetime2(7)         NULL,       -- relay claim lease (ADR-002)
    Attempts      int                  NOT NULL CONSTRAINT DF_Outbox_Attempts DEFAULT (0),
    LastError     nvarchar(2000)       NULL,
    CONSTRAINT PK_OutboxMessages PRIMARY KEY CLUSTERED (Id),
    CONSTRAINT CK_Outbox_Payload_IsJson CHECK (ISJSON(Payload) = 1)
);
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'UX_Outbox_EventId' AND object_id = OBJECT_ID(N'outbox.OutboxMessages'))
    CREATE UNIQUE NONCLUSTERED INDEX UX_Outbox_EventId ON outbox.OutboxMessages (EventId);
GO

-- Filtered index: only pending rows. It stays tiny no matter how large the processed backlog is,
-- so the relay claim query is a short range seek over the oldest pending Ids.
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_Outbox_Pending' AND object_id = OBJECT_ID(N'outbox.OutboxMessages'))
    CREATE NONCLUSTERED INDEX IX_Outbox_Pending
        ON outbox.OutboxMessages (Id)
        INCLUDE (LeaseUntil)
        WHERE ProcessedAt IS NULL;
GO

/* ---------------------------------------------------------------------------------------------
   audit.AuditLog: the LEGACY table that "grows without control" (the business problem).
   New audit events go through the outbox; this table only holds history to offload (step 09).
--------------------------------------------------------------------------------------------- */
IF OBJECT_ID(N'audit.AuditLog', N'U') IS NULL
BEGIN
    CREATE TABLE audit.AuditLog
    (
        Id         bigint IDENTITY(1,1) NOT NULL,
        EventId    uniqueidentifier     NOT NULL CONSTRAINT DF_AuditLog_EventId DEFAULT (NEWID()),
        OccurredAt datetime2(7)         NOT NULL,
        TenantId   nvarchar(64)         NOT NULL,
        ActorId    nvarchar(128)        NOT NULL,
        Action     nvarchar(64)         NOT NULL,
        EntityType nvarchar(64)         NOT NULL,
        EntityId   nvarchar(160)        NOT NULL,
        Details    nvarchar(max)        NULL,      -- JSON blob, typical of legacy audit tables
        CONSTRAINT PK_AuditLog PRIMARY KEY CLUSTERED (Id)
    );
    CREATE NONCLUSTERED INDEX IX_AuditLog_OccurredAt ON audit.AuditLog (OccurredAt);
END
GO

-- Seed ~200k historical rows spread over the last 18 months (only if empty). Set-based.
IF NOT EXISTS (SELECT 1 FROM audit.AuditLog)
BEGIN
    PRINT 'Seeding audit.AuditLog with 200000 historical rows';
    DECLARE @rows int = 200000;
    DECLARE @now  datetime2(7) = SYSUTCDATETIME();
    DECLARE @span int = DATEDIFF(SECOND, DATEADD(MONTH, -18, @now), @now);

    WITH n AS
    (
        SELECT TOP (@rows) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS i
        FROM sys.all_objects a CROSS JOIN sys.all_objects b
    ),
    r AS
    (
        SELECT i,
               ABS(CHECKSUM(NEWID())) AS h1,
               ABS(CHECKSUM(NEWID())) AS h2
        FROM n
    )
    INSERT INTO audit.AuditLog (OccurredAt, TenantId, ActorId, Action, EntityType, EntityId, Details)
    SELECT
        DATEADD(SECOND, -(h1 % @span), @now),
        CONCAT(N't', 1 + h2 % 5),
        CONCAT(N'user-', h1 % 500),
        CHOOSE(1 + h2 % 4, N'transaction.registered', N'account.updated', N'login', N'limit.changed'),
        CHOOSE(1 + h2 % 4, N'Transaction', N'Account', N'Session', N'Account'),
        CONCAT(N'A', h1 % 10000),
        CONCAT(N'{"amount":', h1 % 20000000, N',"currency":"COP","channel":"legacy","ip":"10.0.',
               h2 % 255, N'.', h1 % 255, N'","userAgent":"LegacyCore/1.0 (+core-banking)","note":"',
               REPLICATE(N'x', 40 + h2 % 120), N'"}')
    FROM r;
END
GO

SELECT 'core.Accounts' AS [table], COUNT_BIG(*) AS [rows] FROM core.Accounts
UNION ALL SELECT 'core.Transactions', COUNT_BIG(*) FROM core.Transactions
UNION ALL SELECT 'outbox.OutboxMessages', COUNT_BIG(*) FROM outbox.OutboxMessages
UNION ALL SELECT 'audit.AuditLog', COUNT_BIG(*) FROM audit.AuditLog;
GO
