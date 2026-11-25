# CLAUDE.md — Contexto del proyecto para Claude Code

## Qué es esto
Prueba técnica (makers) — "Desarrollador .NET con énfasis en datos".
Problema de negocio: un core transaccional en .NET + SQL Server cuyas tablas de auditoría e
hiper-logs crecen sin control, degradan el OLTP y suben costos. Se diseña y valida (PoC local en
Docker Compose) una arquitectura de **retención y ciclo de vida** que hace offloading de esos
eventos a un **Lakehouse** (Python/PySpark, Apache Iceberg, dbt, catálogo compatible con AWS Glue).

Se evalúa **criterio** más que volumen de código:
- 40% Arquitectura y decisiones (trade-offs, desacoplamiento .NET, eficiencia del Lakehouse)
- 30% Gobierno y calidad de datos (Iceberg bien usado, dbt, ciclo de vida de logs)
- 30% Uso de IA (prompts, correcciones humanas a la IA, foco en valor de negocio)

Entrega: 26-nov-2026 12:00 (hora Colombia). El evaluador debe poder levantar todo SOLO con el README.

## Restricciones DURAS (no negociables)
1. **100% gratuito y sin cuentas**: nada que requiera licencia paga, registro, API key o cuenta cloud
   para correr el entorno. Solo imágenes oficiales/abiertas.
2. **El stack que exige la prueba es OBLIGATORIO**: Python, Apache Spark, Apache Iceberg, dbt y
   AWS Glue. Lo que está prohibido son ciertos *empaquetadores/proveedores* de imágenes, no las
   tecnologías:
   - `minio/minio` → sin imágenes Docker oficiales desde oct-2025. Reemplazo: **SeaweedFS**.
   - `bitnami/*` (p. ej. `bitnami/kafka`, `bitnami/spark`) → catálogo gratuito retirado en 2025.
     Reemplazo: imágenes **oficiales** (`apache/kafka`, `public.ecr.aws/glue/aws-glue-libs`, etc.).
   - LocalStack → la Community Edition fue retirada; requiere cuenta. No se usa para emular Glue.
   - MediatR ≥13 → licencia dual RPL/comercial. No usar MediatR (ver ADR-004).
   - Redpanda → licencia BSL. Reemplazo: `apache/kafka` (Apache 2.0).
   - **AWS Glue se usa** con la imagen oficial `public.ecr.aws/glue/aws-glue-libs:5` (Glue 5.0:
     Spark 3.5.4 + Iceberg 1.7.1 precargado). Pull anónimo desde ECR Public, sin cuenta.
3. **Pinnear versiones**. Nunca `latest`. Antes de usar un tag de imagen, verificar que existe
   (`docker manifest inspect <imagen:tag>`). Antes de usar un jar, verificar coordenadas en Maven Central
   y que la versión de Iceberg/Kafka connector coincide con Spark y Scala.
4. **.NET 10 (LTS)**. .NET 8 llegó a end-of-support el 10-nov-2026.
5. Cada commit debe dejar el repo en un estado que **levanta** (`docker compose up -d` sin errores
   para los servicios existentes hasta ese momento).

## Stack (todo local, Docker Compose)
| Servicio | Imagen / base | Rol |
|---|---|---|
| sqlserver | `mcr.microsoft.com/mssql/server:2022-*` (`MSSQL_PID=Developer`) | Core OLTP + Outbox |
| kafka | `apache/kafka` (KRaft, 1 nodo) | Buffer durable + replay |
| kafka-init | `apache/kafka` | Crea topics |
| seaweedfs | `chrislusf/seaweedfs` (`server -s3`) | Object storage S3-compatible |
| storage-init | `amazon/aws-cli` | Crea bucket `warehouse` |
| iceberg-rest | `apache/iceberg-rest-fixture` (catálogo JDBC sqlite en volumen) | Catálogo Iceberg (≈ Glue en prod) |
| core-api | build `src/dotnet` | API transaccional (.NET 10) |
| outbox-relay | build `src/dotnet` (mismo código, rol relay) | Outbox → Kafka |
| glue-ingest | build `lakehouse/glue` (FROM `public.ecr.aws/glue/aws-glue-libs:5`) | Job Glue 5.0 / PySpark streaming Kafka → Iceberg bronze |
| glue-jobs | mismo build, `docker compose run` | Jobs Glue: mantenimiento, backfill, demos |
| trino | `trinodb/trino` | Motor SQL para dbt + catálogo `sqlserver` para reconciliación |
| dbt | build `lakehouse/dbt` (python + dbt-core + dbt-trino) | Silver/Gold + tests |
| loadgen | build `src/dotnet` (consola) | Genera carga |

Requisito host: Docker con ≥ 8 GB RAM. SQL Server es solo amd64 (en Apple Silicon corre con Rosetta).

## Decisiones ya tomadas (no re-discutir sin que el usuario lo pida) — detalle en docs/decisions.md
- **Auditoría → Transactional Outbox** (no CDC): evento escrito en la MISMA transacción que el negocio.
- **Logs técnicos (hiper-logs) → buffer asíncrono** (`Channel<T>` acotado → Kafka), sin pasar por SQL Server.
  Garantías distintas para datos distintos.
- Relay con **lease-based claiming** (`UPDATE ... OUTPUT` con `LeaseUntil`), sin mantener transacción
  SQL abierta mientras se habla con Kafka. Productor Kafka idempotente (`EnableIdempotence`, `Acks=All`).
- Entrega **at-least-once** end-to-end; deduplicación por `event_id` en **silver (dbt incremental merge)**.
- **Bronze append-only** en Iceberg, `PARTITIONED BY days(occurred_at)` (hidden partitioning), format v2,
  zstd, target file 128 MB. Nunca particionar por hora ni por timestamp crudo.
- **Spark** = mover bytes con garantías (streaming, backfill, mantenimiento de tablas).
  **dbt** = semántica de negocio, dedupe, calidad (tests), linaje, docs.
- Motor Spark = **imagen oficial AWS Glue 5.0**: los jobs PySpark se escriben como jobs Glue
  (`GlueContext`, `getResolvedOptions`, `Job.init/commit`) para que corran en AWS Glue sin cambios.
  No descargar otra versión de Iceberg: usar la 1.7.1 que trae la imagen (Trino y el catálogo deben
  ser compatibles con ella).
- Catálogo Iceberg **REST** local; en AWS se apunta al **endpoint Iceberg REST de AWS Glue**
  (o a `GlueCatalog`) solo con configuración.
- Spark en modo `local[*]` (no cluster), como corre la imagen Glue local; se justifica en README.
- Checkpoints de streaming en volumen Docker (en prod: S3).

## Estructura del repo
```
.
├─ CLAUDE.md
├─ .claude/commands/    # /paso <n> y /ai-log <texto> (settings.local.json NO se commitea)
├─ README.md
├─ Makefile
├─ docker-compose.yml
├─ .env.example
├─ infra/
│  ├─ sqlserver/        # init.sql (schemas, tablas, índices filtrados), entrypoint de init
│  ├─ seaweedfs/        # s3.json (credenciales locales)
│  └─ trino/catalog/    # lake.properties (iceberg REST), sqlserver.properties
├─ src/dotnet/
│  ├─ Makers.Core.sln
│  ├─ Makers.Core.Api/  # un solo proyecto: Domain/ Application/ Infrastructure/ (hexagonal ligero)
│  ├─ Makers.Core.LoadGen/
│  └─ Makers.Core.Tests/
├─ lakehouse/
│  ├─ glue/             # Dockerfile (FROM aws-glue-libs:5), jobs/*.py (jobs Glue), conf/
│  └─ dbt/              # dbt_project.yml, profiles.yml, models/, tests/
├─ scripts/             # demos: e2e, chaos/replay, schema evolution, métricas
└─ docs/
   ├─ PLAN.md           # plan commit a commit
   ├─ decisions.md      # ADRs
   └─ ai-log.md         # registro de uso de IA (SE EVALÚA)
```

## Convenciones
- Código, nombres y comentarios en **inglés**; documentación (README, docs/) en **español**.
- .NET: Minimal APIs, EF Core 10, `TreatWarningsAsErrors`, nullable habilitado, sin AutoMapper/MediatR.
  Casos de uso como clases planas (`RegisterTransactionHandler`) inyectadas por DI.
  Puertos (interfaces) en `Application/Ports`, adaptadores en `Infrastructure/`.
- Ids de evento: `Guid.CreateVersion7()` (ordenable por tiempo).
- Python: 3.11+, jobs PySpark idempotentes y parametrizados por variables de entorno.
- dbt: `staging/` (views) → `silver/` (incremental merge) → `gold/` (marts). Todo modelo silver/gold con tests.
- Commits: **Conventional Commits** (`feat(dotnet): ...`, `feat(spark): ...`, `chore(infra): ...`, `docs: ...`).
  Commits pequeños, uno por paso del PLAN.

## Cómo trabajar en este repo (instrucciones para Claude Code)
- Sigue `docs/PLAN.md` paso a paso. No adelantes pasos ni añadas servicios no listados.
- Al terminar cada paso: corre la verificación indicada en el PLAN y muestra la salida antes de commitear.
- Si una versión/imagen/jar no existe o hay incompatibilidad, **dilo explícitamente** y propone la
  alternativa; no la "inventes". Estos casos se registran en `docs/ai-log.md`.
- Si el usuario corrige algo tuyo, agrega una entrada a `docs/ai-log.md` (formato del archivo).
## Git: ramas y push los maneja el usuario
- **Nunca** crees, cambies, borres, fusiones ni rebasees ramas (`git checkout -b`, `git switch`,
  `git branch`, `git merge`, `git rebase`). **Nunca** hagas `git push`. Tampoco `git reset --hard`.
- Trabaja siempre en la rama actual. Si estás en `main`/`master`, avisa y espera instrucción.
- Solo haces `git add` + `git commit` en la rama actual, y únicamente después de que el usuario
  confirme el mensaje de commit.
- Al terminar un paso, indica: rama actual, hash del commit y que queda listo para que el usuario haga push.