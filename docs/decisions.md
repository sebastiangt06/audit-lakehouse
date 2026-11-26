# Decisiones de arquitectura (ADRs)

Formato corto: contexto → decisión → alternativas descartadas → consecuencias.
Todas se pueden defender en la sesión de revisión.

---

## ADR-001 — Auditoría: Transactional Outbox (no CDC, no publicación directa)
**Contexto.** Los eventos de auditoría no se pueden perder y no deben bloquear ni ralentizar la
transacción de negocio.
**Decisión.** El evento se escribe en `outbox.OutboxMessages` en la **misma transacción** de SQL Server
que el cambio de negocio. Un proceso aparte (relay) lo publica a Kafka.
**Alternativas.**
- *Publicar a Kafka desde el handler después del commit* (dual-write): si el proceso cae entre el
  commit y el publish, el evento se pierde. Descartado.
- *CDC + Debezium*: no toca el código de la app, pero exige SQL Server Agent y Kafka Connect (un
  componente pesado más), y acopla el contrato del evento al esquema físico de las tablas: un rename
  de columna rompe a los consumidores. Es una opción válida para tablas legadas que no podemos tocar,
  y es el plan B que se documenta.
**Consecuencias.** La escritura extra es pequeña (una fila más en la misma transacción) y queda
contenida porque el outbox se purga a los 7 días. Tenemos un contrato explícito y versionado
(`schema_version`). La entrega es at-least-once, así que hace falta dedupe aguas abajo (ADR-005).
**Contrato del mensaje (`audit.events`).** Sobre JSON en snake_case (`event_id`, `event_type`,
`aggregate_type`, `aggregate_id`, `tenant_id`, `actor_id`, `occurred_at` ISO-8601 UTC con 6 decimales,
`schema_version`, `payload`, `trace_id`), con `payload` como objeto JSON anidado, idéntico byte a byte
a la fila del outbox. **El dinero viaja como string JSON con su precisión exacta**
(`"amount":"12345678.1234"`), nunca como número: casi todo consumidor (Jackson en Spark/Glue,
JavaScript, herramientas de BI) lee los números JSON como `double`, y lo verificamos en la imagen de
Glue: `from_json` convertía `12345678.1234` en `1.23456781234E7` y truncaba un monto de 19 dígitos.
Se implementa con un converter de `decimal` acotado al payload (`DecimalAsStringJsonConverter`), no
con `JsonNumberHandling` global, para que los enteros como `schemaVersion` sigan siendo números. El
cambio se hizo antes de existir consumidores, por eso sigue siendo `schema_version = 1`.

## ADR-002 — Relay con lease, sin transacción abierta durante I/O de red
**Decisión.** El relay reclama un lote con `UPDATE ... SET LeaseUntil = now+30s OUTPUT inserted.*`
(`ROWLOCK, READPAST`), publica fuera de cualquier transacción y luego marca `ProcessedAt`.
**Por qué.** Mantener `SELECT ... WITH (UPDLOCK)` dentro de una transacción mientras se espera a Kafka
deja locks abiertos sobre la tabla caliente; si Kafka se vuelve lento, el OLTP lo sufre. Con lease,
los locks duran milisegundos y varias instancias del relay pueden correr en paralelo (`READPAST`).
**Consecuencia.** Si el relay cae después de publicar y antes de marcar, la lease expira y el lote se
reenvía. Eso genera duplicados, que se aceptan y se resuelven en silver. El orden se garantiza por
`AggregateId` (key de la partición Kafka) dentro de un lote, no globalmente; para auditoría el orden
lo da `occurred_at` más el id ordenable (UUIDv7).

## ADR-003 — Hiper-logs técnicos: buffer asíncrono, no outbox
**Decisión.** Los logs técnicos van por un `Channel<T>` acotado en memoria (`DropOldest`) hacia Kafka y
nunca tocan SQL Server.
**Por qué.** Datos distintos merecen garantías distintas. Escribir cada log técnico en SQL Server es
justo el problema que estamos resolviendo. Aceptamos una pérdida acotada y medida (contador de
descartados) en logs técnicos; en auditoría la tolerancia a pérdida es cero.

## ADR-004 — .NET 10, hexagonal ligero, sin MediatR
**Decisión.** Un solo proyecto con carpetas `Domain/Application/Infrastructure`, puertos
(`IEventBusPublisher`, `IUnitOfWork`) y adaptadores. Los casos de uso son clases planas inyectadas
por DI. Los eventos de dominio pasan al outbox con un `SaveChangesInterceptor`.
**Por qué.**
- .NET 8 llegó a fin de soporte el 10-nov-2026; .NET 10 es LTS hasta nov-2028. El enunciado pide "8+".
- MediatR ≥ 13 tiene licencia dual RPL-1.5/comercial, y la restricción es 100% gratis sin
  ambigüedad de licencia. Además, en un servicio con pocos casos de uso no aporta lo suficiente para
  justificar la indirección. Y hay un riesgo concreto: usar `INotification`/`Publish` para "emitir el
  evento de auditoría" es *in-process*, o sea un dual-write disfrazado.
- Hexagonal sí, porque el puerto `IEventBusPublisher` permite cambiar Kafka por MSK, Kinesis o Event
  Hubs sin tocar el dominio. Varios proyectos/capas físicas serían ceremonia para este alcance.

## ADR-005 — Semántica de entrega: at-least-once + dedupe en silver
**Decisión.** Bronze es append-only y guarda lo que llegó, con `kafka_partition`/`kafka_offset`.
Silver (dbt incremental `merge`, `unique_key=event_id`) es la verdad deduplicada.
**Incremental de silver.** Watermark sobre `ingested_at` (tiempo de llegada) con lookback de 1 h, no
sobre `occurred_at` (tiempo del evento): un evento tardío (caída de la ingesta, replay, backfill) trae un
`occurred_at` antiguo y un filtro por tiempo de evento lo descartaría para siempre. El particionado
sigue siendo `day(occurred_at)`. **Gana la primera entrega** (menor `kafka_offset`): dentro del lote,
`row_number()` por `event_id`; y los `event_id` ya presentes en silver se excluyen con un anti-join, así
el `merge` solo inserta y una re-entrega no reemplaza la fila original. El anti-join se acota a las
particiones de los días del lote (`occurred_at` entre su mínimo y su máximo, que una re-entrega conserva
porque el evento es inmutable), con las cotas como literales para que Iceberg pode particiones en vez
de recorrer silver completa en cada corrida (IA-11).
**Alternativa.** `MERGE` en el streaming hacia bronze. Da dedupe más temprano, pero cada micro-batch
tiene que leer la tabla destino, lo que agrega costo y latencia y complica el job de Spark.
**Por qué.** Mantiene a Spark "tonto y confiable" (mover bytes) y pone la semántica en dbt, que es
testeable (`unique`, `assert_no_event_loss`). Bronze con duplicados además sirve de evidencia
forense de reintentos.

## ADR-006 — Diseño de tablas Iceberg
- **Particionamiento:** `days(occurred_at)` con hidden partitioning: las consultas filtran por
  `occurred_at` sin conocer la columna de partición. Particionar por hora crea demasiadas particiones
  con archivos diminutos; el `tenant_id` con pocos valores se resuelve mejor con un **sort order**
  (`tenant_id, occurred_at`) que con otra dimensión de partición. Si un tenant domina, se evalúa
  `bucket(N, tenant_id)`; la evolución de particiones de Iceberg permite cambiarlo sin reescribir
  el histórico.
- **Small files:** el trigger de 60 s (no de segundos) se suma a la compactación periódica
  (`rewrite_data_files` binpack a 128 MB), `rewrite_manifests`, `expire_snapshots` y
  `remove_orphan_files`. En AWS, la compactación administrada de Glue Data Catalog para Iceberg
  reemplaza el job.
- **ACID:** commits atómicos por snapshot. Los lectores nunca ven micro-batches a medias, y el
  checkpoint de Structured Streaming junto con el commit de Iceberg evitan duplicados por reintentos
  del job.
- **Esquema:** `format-version=2`. Agregar columnas solo cambia metadatos y no reescribe archivos; las
  columnas se rastrean por ID, no por nombre, así que los renames son seguros. dbt usa
  `on_schema_change=append_new_columns`.
- **Time travel:** acotado por la ventana de `expire_snapshots` (7 días por defecto). Es un trade-off
  entre costo de almacenamiento y capacidad de auditoría o rollback.

## ADR-007 — Catálogo: Iceberg REST local, Glue en AWS
**Decisión.** Usamos `apache/iceberg-rest-fixture` con persistencia en volumen. En AWS se apunta al
**endpoint Iceberg REST de AWS Glue** (mismo protocolo, solo cambia la URI y se agrega auth SigV4) o
se usa `catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog`.
**Por qué.** No hay emulación gratuita y sin cuenta del Glue Data Catalog (LocalStack ahora exige
cuenta). Hablar el protocolo REST de Iceberg en local es hablar el mismo protocolo que Glue expone
en la nube. La gobernanza no exige rehacer el modelo porque **el esquema, las particiones y los snapshots
viven en los metadatos de Iceberg en S3**, no en el catálogo; el catálogo solo guarda el puntero al
`metadata.json` vigente. Glue suma descubrimiento, Lake Formation (permisos por columna y fila) y
compactación administrada sin tocar tablas ni modelos dbt.

## ADR-008 — Ciclo de vida de datos (hot / warm / cold)
| Capa | Dónde | Retención | Mecanismo |
|---|---|---|---|
| Hot | SQL Server `audit.AuditLog` | 90 días (configurable) | Purga por lotes **solo de meses verificados** en Iceberg (`audit.OffloadWatermark`); en prod se usa particionamiento mensual + `TRUNCATE ... WITH (PARTITIONS)` |
| Buffer | `outbox.OutboxMessages` | 7 días después de procesado | `OutboxPurger` por lotes |
| Buffer | Kafka `audit.events` | 7 días | Ventana de replay |
| Warm | Iceberg bronze/silver | Ventana regulatoria | Consultable vía Trino/Athena |
| Cold | S3 (prod) | Ventana regulatoria | Intelligent-Tiering; `DELETE` alineado a partición al vencer |

Nota: S3 Glacier "clásico" no es legible por Iceberg sin restaurar, por eso se prefiere
Intelligent-Tiering. Si se exige inmutabilidad (WORM), S3 Object Lock entra en conflicto con la
compactación y el `remove_orphan_files`. Opciones: modo *governance* con retención alineada, o un
bucket de archivo inmutable separado de la tabla operativa. La retención legal exacta depende del
tipo de registro y se valida con el área de cumplimiento.

## ADR-011 — Motor Spark = imagen oficial AWS Glue 5.0
**Decisión.** Los jobs PySpark corren en `public.ecr.aws/glue/aws-glue-libs:5` (Spark 3.5.4, Iceberg
1.7.1 precargado, librería `awsglue`) y se escriben como jobs Glue (`GlueContext`,
`getResolvedOptions`, `Job.commit`).
**Por qué.** La prueba pide AWS Glue dockerizado localmente. Con esta imagen, el código del PoC es
el mismo que correría en AWS Glue, y no hay que armar a mano la matriz de compatibilidad
Spark/Scala/Iceberg, porque AWS ya la fija.
**Alternativa.** `apache/spark` + jars de Iceberg: es más liviana, pero no prueba nada sobre Glue y
obliga a mantener la compatibilidad de jars a mano.
**Consecuencias.** La imagen pesa varios GB (el primer pull es lento). Iceberg queda fijo en 1.7.1,
y Trino y el catálogo deben ser compatibles con esa versión. Hay funciones de Glue que no existen en
local (bookmarks gestionados, conexiones del catálogo), y se documentan como limitaciones.

## ADR-009 — Spark en modo local, Trino como motor de dbt
**Decisión.** Spark (imagen Glue) corre en `local[*]` (sin master/workers) y dbt usa `dbt-trino`.
**Por qué.** En un PoC un cluster Spark solo suma RAM y puntos de falla sin probar nada nuevo. Trino
da un adaptador dbt estable sobre Iceberg y un catálogo `sqlserver` que permite **reconciliar
outbox vs lakehouse con SQL** (test `assert_no_event_loss`). En AWS el equivalente es Athena (motor
Trino) con `dbt-athena`.

## ADR-010 — Restricción 100% gratuita
| Elegido | Descartado | Motivo |
|---|---|---|
| SeaweedFS | MinIO | Sin imágenes Docker oficiales desde oct-2025 |
| `apache/kafka`, `public.ecr.aws/glue/aws-glue-libs:5` | `bitnami/*` | Catálogo gratuito retirado en 2025 |
| `apache/kafka` | Redpanda | Licencia BSL |
| Iceberg REST fixture | LocalStack (Glue) | Requiere cuenta; el plan gratis es no comercial |
| Clases planas | MediatR ≥13 | Licencia dual RPL/comercial |
| SQL Server Developer | SQL Server Standard | Developer es gratis para dev/test |