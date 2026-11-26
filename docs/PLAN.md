# PLAN — commit a commit

Cada paso trae: **objetivo**, **prompt para Claude Code** (cópialo tal cual), **verificación** (lo que tú
debes ver antes de commitear) y **mensaje de commit**.

Recomendación de flujo por paso:
1. En Claude Code, entra en *plan mode* (Shift+Tab) y pega el prompt. Revisa el plan que propone.
2. Aprueba, deja que implemente, corre la verificación.
3. Si tuviste que corregirlo → **anota la corrección en `docs/ai-log.md` en ese momento** (es 30% de la nota).
4. Claude Code hace el commit en la rama actual tras tu confirmación. **Las ramas y el push los haces tú**
   (está bloqueado en `.claude/settings.json`).

Presupuesto de tiempo (hasta 26-nov 12:00). Los pasos marcados ⭐ son los que más puntúan; si vas corto
de tiempo, recorta los marcados 🟡.

| # | Paso | Tiempo | |
|---|---|---|---|
| 00 | Scaffold | 15 min | |
| 01 | Infra base | 1.5 h | ⭐ riesgo alto |
| 02 | .NET: dominio + API + outbox | 1.5 h | ⭐ |
| 03 | .NET: relay → Kafka | 1 h | ⭐ |
| 04 | LoadGen + tests | 45 min | |
| 05 | AWS Glue 5.0 (Spark) + bronze + streaming | 1.5 h | ⭐ riesgo alto |
| 06 | Trino + dbt | 1.5 h | ⭐ |
| 07 | Compactación / mantenimiento | 45 min | ⭐ |
| 08 | Evolución de esquema | 30 min | ⭐ |
| 09 | Ciclo de vida (backfill + purga) | 1 h | ⭐ |
| 10 | Resiliencia + replay | 45 min | ⭐ |
| 11 | Hiper-logs con buffer asíncrono | 45 min | 🟡 |
| 12 | Métrica costo-beneficio | 30 min | ⭐ |
| 13 | README final | 1.5 h | ⭐ |
| 14 | Prueba desde clone limpio | 30 min | ⭐ obligatorio |

---

## 00 — Scaffold
**Prompt**
```
Lee CLAUDE.md y docs/. Crea el scaffold del repo según la estructura de CLAUDE.md:
.gitignore (dotnet, python, dbt target/logs, .env, volúmenes locales, .claude/settings.local.json), .env.example con todas las
variables que usaremos (SA password, credenciales S3 locales, nombres de topic/bucket), un Makefile con
targets vacíos documentados (up, down, clean, demo, load, dbt, maintenance, chaos, replay, evolve,
lifecycle, metrics) y carpetas vacías con .gitkeep. No crees servicios todavía.
```
**Verificación**: `tree -L 3` coincide con CLAUDE.md. `make help` lista los targets.
**Commit**: `chore: scaffold repository structure and project context`

---

## 01 — Infra base ⭐
**Prompt**
```
Implementa en docker-compose.yml los servicios: sqlserver, kafka, kafka-init, seaweedfs, storage-init,
iceberg-rest. Reglas:
- Versiones pinneadas; verifica cada tag con `docker manifest inspect` antes de usarlo.
- sqlserver: MSSQL_PID=Developer, healthcheck con /opt/mssql-tools18/bin/sqlcmd -C (verifica la ruta
  dentro de la imagen), mem_limit 2g.
- kafka: apache/kafka en modo KRaft single-node, listeners interno (kafka:9092) y externo (localhost:29092).
- kafka-init: crea topic `audit.events` (6 particiones, retention 7 días, cleanup delete) y
  `app.logs` (3 particiones, retention 3 días). Idempotente (--if-not-exists).
- seaweedfs: `server -s3` con config de credenciales desde infra/seaweedfs/s3.json, volumen persistente.
- storage-init: amazon/aws-cli crea el bucket `warehouse` con --endpoint-url, idempotente.
- iceberg-rest: apache/iceberg-rest-fixture con S3FileIO apuntando a seaweedfs (path-style), warehouse
  s3://warehouse/, y catálogo JDBC sqlite PERSISTIDO en volumen (por defecto es en memoria y se pierde
  al reiniciar: verifica los nombres exactos de variables CATALOG_* en la documentación de la imagen).
- Healthchecks en todos y depends_on con condition: service_healthy / service_completed_successfully.
Agrega `make up`, `make down`, `make clean` (down -v).
```
**Verificación**
```bash
make up && docker compose ps          # todo healthy / init "exited (0)"
curl -s localhost:8181/v1/config      # responde JSON
docker compose restart iceberg-rest && curl -s localhost:8181/v1/namespaces   # sigue respondiendo
```
**Commit**: `chore(infra): add sqlserver, kafka, s3 storage and iceberg rest catalog`

---

## 02 — .NET: dominio + API + outbox en la misma transacción ⭐
**Prompt**
```
Crea la solución .NET 10 en src/dotnet según CLAUDE.md (un solo proyecto API con carpetas
Domain/Application/Infrastructure, sin MediatR ni AutoMapper).
Dominio: agregado `Account` y entidad `Transaction` (transferencia: from, to, amount, currency, tenantId).
El agregado registra eventos de dominio (`TransactionRegistered`).
Application: caso de uso `RegisterTransactionHandler` (clase plana) y puerto `IUnitOfWork`.
Infrastructure:
- EF Core 10 SqlServer. Tabla `outbox.OutboxMessages` (Id bigint identity, EventId uniqueidentifier
  (Guid.CreateVersion7), EventType, AggregateType, AggregateId, TenantId, ActorId, OccurredAt
  datetime2, SchemaVersion int, Payload nvarchar(max) JSON, TraceId, ProcessedAt null, LeaseUntil null,
  Attempts int, LastError null).
- Un SaveChangesInterceptor convierte los eventos de dominio en filas de outbox DENTRO del mismo
  SaveChanges (misma transacción). Esto es el corazón: no hay dual-write.
- Índice filtrado: (Id) INCLUDE(LeaseUntil) WHERE ProcessedAt IS NULL.
- Esquema: crea infra/sqlserver/init.sql idempotente (schemas core/outbox/audit) y un servicio
  `sqlserver-init` en compose que lo ejecuta. Incluye también la tabla legada `audit.AuditLog` (la que
  "crece sin control") con datos históricos sembrados: ~200k filas repartidas en los últimos 18 meses.
API minimal: POST /transactions, GET /health. Servicio `core-api` en compose (puerto 8081), Dockerfile
multi-stage con imagen chiseled/aspnet 10.
```
**Verificación**
```bash
curl -X POST localhost:8081/transactions -H 'content-type: application/json' \
  -d '{"tenantId":"t1","from":"A1","to":"B2","amount":150000,"currency":"COP"}'
docker compose exec sqlserver /opt/mssql-tools18/bin/sqlcmd -C -U sa -P "$SA_PASSWORD" -d MakersCore \
  -Q "select count(*) from core.Transactions; select count(*) from outbox.OutboxMessages where ProcessedAt is null"
```
Ambos conteos deben subir juntos.
**Commit**: `feat(dotnet): transactional core with outbox written in the same transaction`

---

## 03 — .NET: Outbox relay → Kafka ⭐
**Prompt**
```
Implementa el relay como BackgroundService en el mismo proyecto, activado por configuración
(Role=Relay), y un servicio `outbox-relay` en compose que usa la misma imagen con Role=Relay.
Puerto `IEventBusPublisher` en Application, adaptador `KafkaEventBusPublisher` (Confluent.Kafka).
Algoritmo (lease-based, sin transacción SQL abierta durante I/O de red):
1. Claim: UPDATE de las N filas más antiguas (CTE con ORDER BY Id) con ProcessedAt IS NULL y
   (LeaseUntil IS NULL OR LeaseUntil < now) → set LeaseUntil = now + 30s, Attempts += 1,
   OUTPUT inserted.*. Hints ROWLOCK, READPAST.
2. Publish: producir el lote a `audit.events`, key = AggregateId, headers event_type/schema_version/
   event_id. Producer: EnableIdempotence=true, Acks=All, CompressionType=Zstd, LingerMs=10.
   Esperar los delivery reports.
3. Ack: UPDATE ProcessedAt = now para los entregados; para los fallidos guardar LastError
   (la lease expira y se reintentan).
4. Backoff exponencial con jitter cuando Kafka no está disponible (Microsoft.Extensions.Resilience).
Métricas por log: lote, entregados, fallidos, lag (filas pendientes).
Documenta en código por qué no mantenemos la transacción abierta y por qué la entrega es
at-least-once.
```
**Verificación**
```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 \
  --topic audit.events --from-beginning --max-messages 3 --property print.key=true
```
Y `ProcessedAt` se llena en SQL Server.
**Commit**: `feat(dotnet): lease-based outbox relay publishing to kafka`

---

## 04 — LoadGen + tests
**Prompt**
```
Crea Makers.Core.LoadGen (consola) que hace N POST /transactions con paralelismo P y tenants
aleatorios (t1..t5), con montos que incluyan algunos > 10.000.000 COP. Servicio `loadgen` en compose
con profile "tools". Target `make load N=5000 P=20`.
Crea Makers.Core.Tests (xUnit) con: test unitario del agregado y un test del interceptor que
verifica que la transacción y el mensaje de outbox se persisten juntos o ninguno (usa SQLite
in-memory o Testcontainers si está disponible sin cuentas; explica la elección).
```
**Verificación**: `make load N=2000 P=20` termina; `dotnet test` en verde.
**Commit**: `feat(dotnet): load generator and outbox atomicity tests`

---

## 05 — AWS Glue 5.0 (Spark) + tabla bronze + streaming ⭐
**Prompt**
```
El motor Spark es la imagen OFICIAL de AWS Glue 5.0: public.ecr.aws/glue/aws-glue-libs:5
(Spark 3.5.4, Iceberg 1.7.1 precargado, librería awsglue). Pull anónimo, sin cuenta AWS.
1. Antes de escribir nada, inspecciona la imagen: `docker run --rm --entrypoint bash
   public.ecr.aws/glue/aws-glue-libs:5 -c "ls /usr/lib/spark/jars | grep -Ei 'iceberg|kafka|aws'"`
   (ajusta la ruta si no existe; búscala) y dime: versión exacta de Iceberg, si trae el conector
   Kafka para Structured Streaming (spark-sql-kafka-0-10) e iceberg-aws/S3FileIO, usuario y
   entrypoint por defecto. NO agregues otra versión de Iceberg. Si falta el conector Kafka,
   descárgalo EN BUILD en la versión compatible con Spark 3.5.4 / Scala 2.12 (verifica en Maven Central).
2. lakehouse/glue/Dockerfile FROM esa imagen (pin por digest además del tag "5").
3. conf: catálogo `lake` tipo rest (uri iceberg-rest:8181), io-impl S3FileIO, s3.endpoint
   seaweedfs, path-style-access=true, credenciales dummy de .env, extensiones de Iceberg SQL.
   Documenta en comentario los dos bloques de producción: (a) endpoint Iceberg REST de AWS Glue
   con SigV4, (b) catalog-impl org.apache.iceberg.aws.glue.GlueCatalog. El cambio es solo config.
4. Los jobs se escriben como jobs AWS Glue: getResolvedOptions para parámetros, GlueContext,
   Job.init / job.commit; la lógica en funciones puras de PySpark (testeables). Así corren igual en
   AWS Glue.

jobs/create_tables.py: namespace lake.audit y tabla `lake.audit.events_bronze`:
  columnas: event_id, event_type, aggregate_type, aggregate_id, tenant_id, actor_id,
  occurred_at timestamp, schema_version int, payload string, trace_id,
  kafka_partition int, kafka_offset bigint, ingested_at timestamp
  PARTITIONED BY (days(occurred_at)); WRITE ORDERED BY tenant_id, occurred_at
  TBLPROPERTIES: format-version=2, write.parquet.compression-codec=zstd,
  write.target-file-size-bytes=134217728, write.distribution-mode=hash,
  write.metadata.delete-after-commit.enabled=true, write.metadata.previous-versions-max=50
  Idempotente (IF NOT EXISTS).

jobs/ingest_audit_stream.py: Structured Streaming desde audit.events → append a events_bronze.
  trigger processingTime configurable (default 60s — justifica en comentario: latencia vs small files),
  checkpoint en volumen /checkpoints/audit_bronze, maxOffsetsPerTrigger configurable,
  startingOffsets earliest, failOnDataLoss=true. Parsear el JSON del valor; conservar partition/offset.
Servicio `glue-ingest` (long-running, spark-submit del job de streaming) y `glue-jobs`
(profile tools, para docker compose run). Nota: la imagen pesa varios GB; documenta en el README
que el primer `make up` tarda por el pull.
```
**Verificación**
```bash
make load N=3000 P=20
# esperar ~1–2 triggers
docker compose run --rm glue-jobs spark-sql -e "select count(*), count(distinct event_id) from lake.audit.events_bronze"
docker compose run --rm glue-jobs spark-sql -e "select partition, file_count from lake.audit.events_bronze.partitions"
```
**Commit**: `feat(spark): iceberg bronze table and kafka streaming ingestion`

---

## 06 — Trino + dbt ⭐
**Prompt**
```
Agrega servicio `trino` (pinneado, JVM -Xmx1G) con catálogos:
- infra/trino/catalog/lake.properties: connector iceberg, iceberg.catalog.type=rest, uri iceberg-rest,
  fs.native-s3.enabled=true, s3.endpoint seaweedfs, s3.path-style-access=true, credenciales de .env.
- infra/trino/catalog/sqlserver.properties: connector sqlserver al core (para reconciliación).
Crea lakehouse/dbt (dbt-core + dbt-trino pinneados, imagen python slim) con profiles.yml por env vars.
Modelos:
- sources.yml: lake.audit.events_bronze con freshness (loaded_at_field: ingested_at) y
  sqlserver.outbox.OutboxMessages.
- staging/stg_audit_events (view): tipado, extracción de campos del payload JSON (amount, currency,
  from, to), normalización.
- silver/audit_events: incremental, incremental_strategy=merge, unique_key=event_id,
  on_schema_change=append_new_columns, properties partitioning day(occurred_at), formato parquet.
  Filtro incremental con ventana de lookback de 1h sobre occurred_at para eventos tardíos.
  Dedupe con row_number() sobre event_id (quedarse con el menor kafka_offset).
- gold/daily_activity_by_tenant y gold/high_value_transactions (monto >= 10.000.000 COP — reporte
  regulatorio de ejemplo).
Tests: unique + not_null(event_id) en silver, accepted_values(event_type), not_null(occurred_at,
tenant_id), y un test singular `assert_no_event_loss` que compara event_id despachados del outbox
(ProcessedAt not null) vs silver vía Trino (cross-catalog) y falla si falta alguno.
dbt docs generate. Target `make dbt` = dbt build.
Crea también `make demo` (scripts/e2e_demo.sh): espera servicios healthy → crea tablas → load 3000 →
espera (polling vía Trino, con timeout) hasta que bronze tenga todos los event_id despachados del
outbox → dbt build → imprime un resumen de gold. Este es el "flujo end-to-end" que pide la prueba.
```
**Verificación**: `make dbt` → todos los tests PASS. `select count(*)` en bronze ≥ silver (bronze puede tener duplicados).
**Commit**: `feat(dbt): silver/gold models on iceberg via trino with data quality tests`

---

## 07 — Compactación / small files ⭐
**Prompt**
```
jobs/maintenance.py (parametrizable por tabla): 
1. Reporta ANTES: número de data files, tamaño promedio, snapshots (consultando metadata tables
   .files y .snapshots).
2. CALL lake.system.rewrite_data_files(table, strategy=>'binpack', options=>map('min-input-files','5',
   'target-file-size-bytes','134217728'))  — opcional sort por tenant_id, occurred_at.
3. CALL rewrite_manifests.
4. CALL expire_snapshots(older_than => now - RETENTION_SNAPSHOTS_DAYS, retain_last => 10).
5. CALL remove_orphan_files(older_than => now - 3 días).
6. Reporta DESPUÉS y una tabla comparativa.
Crea scripts/small_files_demo.sh: baja el trigger a 5s, mete carga, muestra la explosión de archivos,
corre maintenance y muestra el antes/después. Target `make maintenance`.
Explica en comentarios por qué expire_snapshots limita el time travel y cómo se elige la ventana.
```
**Verificación**: el reporte muestra menos archivos y tamaño promedio mayor después.
**Commit**: `feat(spark): iceberg table maintenance (compaction, snapshot expiry, orphan cleanup)`

---

## 08 — Evolución de esquema ⭐
**Prompt**
```
Demo de evolución de esquema sin downtime:
1. En .NET agrega `Channel` (web/mobile/branch) al evento TransactionRegistered con SchemaVersion=2,
   manteniendo compatibilidad (campo opcional).
2. scripts/evolve_schema.sh: ALTER TABLE lake.audit.events_bronze ADD COLUMN channel string (solo
   metadatos, no reescribe archivos — demuéstralo comparando el conteo de data files antes/después),
   el streaming job mapea channel si existe, carga eventos v2, dbt build (on_schema_change append),
   y consulta que v1 tiene channel NULL y v2 lo trae.
3. Muestra time travel: select de la tabla `FOR VERSION AS OF <snapshot anterior>`.
Target `make evolve`.
```
**Commit**: `feat: schema evolution demo (v2 events, metadata-only column add, time travel)`

---

## 09 — Ciclo de vida: backfill + purga ⭐
**Prompt**
```
Política de ciclo de vida (documentar en docs/decisions.md ADR-008):
HOT  SQL Server: solo últimos HOT_DAYS (default 90) en audit.AuditLog; outbox procesado se purga a los 7 días.
WARM Iceberg: todo el histórico, consultable.
COLD (prod): S3 Intelligent-Tiering / lifecycle; retención regulatoria y borrado por partición.

1. jobs/backfill_audit_log.py: lee audit.AuditLog por JDBC (mssql-jdbc: verifica si la imagen Glue lo trae; si no, agrégalo en build) en rangos por
   mes (partitionColumn/lowerBound/upperBound o predicates), escribe en lake.audit.events_bronze
   (con event_type 'legacy.audit') de forma idempotente (MERGE por event_id o overwrite de
   particiones del rango). Verifica conteos por mes origen vs destino y aborta si no cuadran.
2. infra/sqlserver/purge.sql: procedimiento que borra en lotes (DELETE TOP (5000) con WAITFOR
   corto entre lotes) filas con fecha < now - HOT_DAYS SOLO si el mes ya está verificado en una
   tabla de control audit.OffloadWatermark. Comentario: en prod usar particionamiento por mes +
   TRUNCATE TABLE ... WITH (PARTITIONS) para que sea metadata-only.
3. BackgroundService OutboxPurger en .NET (borrado por lotes de ProcessedAt < now-7d).
4. Retención en Iceberg: ejemplo de DELETE por rango de partición alineado (metadata-only) para
   datos fuera de la ventana regulatoria.
Target `make lifecycle` que corre backfill → verificación → purga y muestra sp_spaceused antes/después.
```
**Commit**: `feat: data lifecycle — verified backfill to iceberg and safe batched purge in sql server`

---

## 10 — Resiliencia + replay ⭐
**Prompt**
```
scripts/chaos_demo.sh (target `make chaos`):
1. Escenario A — cae la ingesta: docker compose stop glue-ingest; carga 5000; verificar que la API
   respondió 2xx siempre y que Kafka acumula lag (kafka-consumer-groups / offsets); start glue-ingest;
   esperar y mostrar que bronze recuperó todo.
2. Escenario B — cae Kafka: stop kafka; carga 2000; mostrar que la API sigue OK y que el outbox
   acumula pendientes; start kafka; el relay drena; reconciliación OK.
3. Al final corre el test dbt assert_no_event_loss.
scripts/replay.sh (target `make replay FROM=<iso-datetime>`): replay desde Kafka reseteando la
posición: como Structured Streaming guarda offsets en el checkpoint (no en el consumer group),
el replay es: parar el job, levantar uno one-off con startingOffsetsByTimestamp desde FROM y
checkpoint nuevo, escribiendo a bronze; silver queda correcto por el merge por event_id.
Y replay desde el outbox (si Kafka perdió retención): UPDATE ProcessedAt = NULL por rango de fechas.
Documenta en el README ambos caminos.
```
**Commit**: `feat: resilience chaos demo and replay mechanisms (kafka offsets and outbox)`

---

## 11 — Hiper-logs con buffer asíncrono 🟡
**Prompt**
```
Logs técnicos (no auditoría): un ILoggerProvider/sink propio que escribe a un Channel<LogEvent>
acotado (capacidad configurable, FullMode=DropOldest) y un BackgroundService que publica en lotes a
`app.logs` con acks=1. Contador de logs descartados expuesto en /metrics o en log. NO pasan por SQL
Server. Job de streaming adicional (o el mismo con segundo query) a lake.logs.app_logs particionado
por days(ts) con retención corta. Explica el trade-off (aceptamos pérdida acotada en logs técnicos;
nunca en auditoría).
```
**Commit**: `feat: async bounded buffer for technical logs bypassing sql server`

---

## 12 — Métrica costo-beneficio ⭐
**Prompt**
```
scripts/metrics.sh (target `make metrics`) que imprime una tabla:
- SQL Server: sp_spaceused de audit.AuditLog + outbox (data + index, MB) y filas.
- Iceberg: sum(file_size_in_bytes) desde lake.audit."events_bronze$files" (Trino), filas.
- Ratio de compresión bytes/fila en cada lado.
- p95 de latencia de POST /transactions medido por LoadGen con AuditLog grande vs después de la purga.
Genera docs/metrics.md con los resultados reales de tu máquina.
```
**Commit**: `feat: storage and latency metrics for cost-benefit analysis`

---

## 13 — README final ⭐
**Prompt**
```
Completa README.md siguiendo su esqueleto: diagrama mermaid de la arquitectura, quick start
(prerrequisitos, make up/demo, puertos, cómo ver cada paso), tabla de servicios, resumen de
decisiones (link a docs/decisions.md), respuestas a las 4 preguntas usando los NÚMEROS REALES de
docs/metrics.md, limitaciones conocidas del PoC y camino a producción en AWS (Glue Catalog, MSK,
EMR Serverless/Glue jobs, S3, Lake Formation, Glue managed compaction), y la sección de IA basada en
docs/ai-log.md. No inventes resultados: si algo no se midió, dilo.
```
**Commit**: `docs: complete readme with architecture, decisions and answers`

---

## 14 — Clone limpio (obligatorio)
```bash
cd /tmp && rm -rf test-clone && git clone <tu-repo> test-clone && cd test-clone
cp .env.example .env && make clean && make up && make demo
```
Sigue SOLO el README. Todo lo que tengas que hacer distinto → corrige el README.
Verifica que el repo es **público** (abrir en ventana incógnito).
**Commit**: `docs: fix setup instructions after clean-clone test`