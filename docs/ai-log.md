# Registro de uso de IA

**Herramientas:** Claude (chat) para diseño y razonamiento de arquitectura, y Claude Code para la
implementación commit a commit.
**Principio:** la IA acelera, pero no decide. Cada versión, imagen y afirmación técnica se verifica
contra fuentes primarias (docs oficiales, Docker Hub, Maven Central) antes de entrar al repo.

Formato de cada entrada:
> **Contexto** · **Prompt** · **Respuesta de la IA** · **Problema detectado** · **Decisión humana** · **Evidencia**

---

## Fase 1 — Diseño (Claude chat)

### IA-01 · Arquitectura base y patrón .NET
- **Prompt clave:** "Tengo esta prueba técnica [PDF]. Estaba pensando, como siempre, en arquitectura
  limpia, hexagonal, con patrón MediatR. Ayúdame a pensarla."
- **Respuesta de la IA:** cuestionó gastar el tiempo en capas del lado .NET dados los pesos de la
  rúbrica, propuso Outbox en lugar de CDC y advirtió que publicar eventos con `INotification` de
  MediatR es *in-process* (dual-write disfrazado).
- **Decisión humana:** conservé hexagonal ligero (un proyecto con puertos y adaptadores) porque el
  puerto del bus sí tiene valor; quité MediatR (ver IA-04). → ADR-001, ADR-004.

### IA-02 · La IA propuso MinIO y Redpanda (desactualizado / no 100% libre) ⚠️
- **Contexto:** la primera propuesta de stack incluía MinIO como S3 local y Redpanda como broker.
- **Problema detectado:** al agregar la restricción "todo debe ser gratuito" pedí verificar. MinIO
  **dejó de publicar imágenes Docker** de la edición community en octubre de 2025
  (`docker pull minio/minio` falla). Redpanda tiene licencia BSL.
- **Decisión humana:** SeaweedFS (Apache 2.0, API S3) y `apache/kafka` en KRaft. → ADR-010.
- **Lección:** el conocimiento de un modelo sobre el *ecosistema* (qué imágenes existen y bajo qué
  licencia) envejece más rápido que su conocimiento conceptual. Toda dependencia se verifica.

### IA-03 · La IA escribió ".NET 8" en el primer diagrama ⚠️
- **Problema detectado:** lo copió del enunciado. .NET 8 llegó a **fin de soporte el 10-nov-2026** y la
  entrega es el 26-nov-2026.
- **Decisión humana:** .NET 10 LTS (soportado hasta nov-2028); cumple ".NET 8+". → ADR-004.

### IA-04 · Licencia de MediatR
- **Verificación:** MediatR ≥ 13 (jul-2025) tiene licencia dual RPL-1.5/comercial. La edición community
  es gratis por debajo de USD 5M de ingresos, pero introduce ambigüedad de licencia frente a la
  restricción del proyecto.
- **Decisión humana:** casos de uso como clases planas y `SaveChangesInterceptor` para el outbox.

### IA-05 · Glue en local
- **Riesgo típico de la IA:** sugerir LocalStack para emular el Glue Data Catalog.
- **Verificación:** LocalStack retiró su Community Edition; ahora requiere cuenta y el plan gratis es no
  comercial. El evaluador no podría levantar el entorno "solo con el README".
- **Decisión humana:** Iceberg REST Catalog local, con el bloque de configuración Glue documentado.
  → ADR-007.

### IA-06 · Bitnami
- **Riesgo típico:** muchos ejemplos de compose (Kafka, Spark) usan `bitnami/*`, catálogo retirado del
  free tier en 2025.
- **Decisión humana:** imágenes oficiales `apache/*`.

### IA-07 · La IA redujo "AWS Glue" a "solo el catálogo" y redactó una regla ambigua ⚠️
- **Contexto:** al revisar con Claude Code el resumen de las "restricciones duras" de CLAUDE.md,
  la regla 2 decía "Prohibido: … bitnami/* (se usan imágenes apache/*)". Leída rápido, parecía
  prohibir las imágenes de Apache, justo el stack que exige la prueba.
- **Pregunta humana:** "La prueba pide Spark, Iceberg, dbt y AWS Glue. ¿Está usando todo eso?"
- **Problema detectado:** (1) la regla mezclaba proveedores de imágenes prohibidos con las
  tecnologías obligatorias; (2) el diseño inicial cubría Glue solo como "catálogo equivalente en
  producción", más débil que lo que pide el enunciado ("AWS Glue dockerizado localmente").
- **Decisión humana:** reescribí la regla 2 para separar "stack obligatorio" de "empaquetadores
  prohibidos", y el motor Spark pasó a ser la imagen oficial AWS Glue 5.0
  (public.ecr.aws/glue/aws-glue-libs:5), con los jobs escritos como jobs Glue. → ADR-011.
- **Lección:** las instrucciones para un agente necesitan la misma revisión que el código: una regla
  ambigua en CLAUDE.md se propaga a cada paso.

---

## Fase 2 — Implementación (Claude Code)
> Completa una entrada cada vez que corrijas a Claude Code. Que sean reales y con el commit donde quedó.
> Candidatas probables a vigilar:
> - Jars de Iceberg/Kafka que no coinciden con la versión de Spark o Scala.
> - `iceberg-rest-fixture` con catálogo en memoria (se pierden las tablas al reiniciar).
> - Ruta de `sqlcmd` (`mssql-tools` vs `mssql-tools18` + `-C`).
> - Relay con `UPDLOCK` dentro de una transacción larga mientras publica (ADR-002).
> - `PARTITIONED BY hours(...)` o por timestamp crudo (small files).
> - Uso de `--packages` en runtime (descarga en cada arranque, frágil sin red).
> - Replay "reseteando el consumer group" cuando Structured Streaming guarda offsets en el checkpoint.
> - Tests que "pasan" porque no verifican nada (asserts vacíos o mocks de todo).

### IA-08 · La IA silenció los logs de SQL de toda la app para tapar una carrera ⚠️
- **Commit:** _por completar_ (paso 02, `feat(dotnet): transactional core with outbox written in the same transaction`)
- **Prompt:** `/step 02` → prompt del paso 02 de `docs/PLAN.md` (dominio + API + outbox en la misma transacción).
- **Respuesta de la IA:** en el plan propuso auto-abrir la cuenta origen si no existía, con reintento
  ante violación de unique (2601/2627) y `ChangeTracker.Clear()`. Aprobé ese plan sin notar el problema de dominio.
  En una prueba de 40 requests en paralelo sobre una cuenta nueva, todas respondieron 201, pero EF Core
  registró las 12 carreras como `fail:` con stack trace (120 líneas del log contenían `fail`/`exception`).
  Para limpiar el log, la IA subió a `Critical` las categorías `Microsoft.EntityFrameworkCore.Database.Command`
  y `Microsoft.EntityFrameworkCore.Update`, y lo presentó como resuelto.
- **Problema detectado:** eso ocultaba todos los errores de SQL de la app, incluidos deadlocks,
  timeouts y los del relay (paso 03). Además, la causa era de dominio: un core no abre cuentas al recibir una transferencia.
- **Decisión humana:** rechacé el silenciado y ataqué la causa: sin auto-apertura de cuentas (se
  eliminaron el reintento por `DuplicateKeyException` y `Reset()`), cuentas `A1..A50` y `B1..B50` para
  `t1..t5` sembradas de forma idempotente en `init.sql`, y **422** con ProblemDetails si la cuenta
  origen no existe. Restauré los niveles de log. Ocultar el síntoma no es una solución.
- **Evidencia:**
  - `src/dotnet/Makers.Core.Api/appsettings.json:6` → `Database.Command` en `Warning`, sin entrada para `Update`.
  - `infra/sqlserver/init.sql:68-77` → seed idempotente de cuentas.
  - `src/dotnet/Makers.Core.Api/Application/RegisterTransactionHandler.cs:34` y `Program.cs:57` → 422.
  - Re-prueba con 40 requests en paralelo sobre `t3:A7`: `40 201`; `docker compose logs core-api | grep -c "fail:"` → `0`;
    transacciones = filas de outbox para `t3:A7` → `40 / 40`. Cuenta `NOPE9` → `HTTP 422 "Unknown source account"`.
- **ADR afectado:** ninguno. La auto-apertura de cuentas no estaba en ningún ADR; fue un detalle de implementación del plan.

### IA-09 · La IA configuró xUnit v3 con el runner VSTest, que el SDK de .NET 10 ya no soporta ⚠️
- **Commit:** _por completar_ (paso 04, `feat(dotnet): load generator and outbox atomicity tests`)
- **Prompt:** `/step 04` + prompt detallado del paso 04: "Makers.Core.Tests (xUnit, en la solución) … Test de
  ATOMICIDAD contra el SQL Server real del compose … Si no hay SQL Server disponible, el test se marca como
  skipped, no como verde … crea scripts/test.sh que corre `dotnet test` dentro de la imagen sdk pinneada".
- **Respuesta de la IA:** en el plan propuso xUnit v3 (por `Assert.Skip` nativo) corriendo sobre VSTest:
  `Microsoft.NET.Test.Sdk` 18.12.0 + `xunit.runner.visualstudio` 4.0.1 + `xunit.v3` 4.0.3. Dijo que había
  verificado las versiones en nuget.org. Solo verificó que existieran, no que fueran compatibles con el
  runner del SDK de .NET 10. Aprobé el plan sin notarlo.
- **Problema detectado:** la primera corrida de `bash scripts/test.sh` falló en el build:
  `error : Testing with VSTest target is no longer supported by Microsoft.Testing.Platform on .NET 10 SDK
  and later. If you use dotnet test, you should opt-in to the new dotnet test experience.`
  xUnit v3 4.x corre sobre Microsoft.Testing.Platform (MTP) y el SDK 10.0.404 ya no acepta el modo VSTest para él.
- **Decisión humana:** pasar a MTP: `src/dotnet/global.json` con `"test": { "runner": "Microsoft.Testing.Platform" }`,
  quitar los paquetes de VSTest y cambiar el script a `dotnet test --solution Makers.Core.sln`.
  Lección: la verificación ejecutada atrapa lo que la revisión del plan no ve; por eso cada paso exige
  salida real.
- **Evidencia:**
  - Error y guía oficial: https://aka.ms/dotnet-test-mtp-error (enlace incluido en el propio mensaje de error).
  - `src/dotnet/global.json:2-4` → opt-in a MTP.
  - `src/dotnet/Makers.Core.Tests/Makers.Core.Tests.csproj:4-5,13-14` → solo `xunit.v3` + EF InMemory, sin paquetes VSTest.
  - `scripts/test.sh:49` → `dotnet test --solution Makers.Core.sln -c Release --output Detailed`.
  - Re-corrida: `Test run summary: Passed! total: 7 failed: 0 succeeded: 7 skipped: 0`, con
    `passed Makers.Core.Tests.OutboxAtomicityTests.SaveChanges_FailingAfterInsertsBeforeCommit_PersistsNeitherTransactionNorOutboxRow`.
    Sin la red del compose: `succeeded: 6 skipped: 1`.
- **ADR afectado:** ninguno. El runner de tests no está en ningún ADR; ADR-004 cubre .NET 10 y la estructura, no el tooling de tests.

### IA-10 · La IA asumió que `from_json` conserva el JSON crudo, y el agente propuso una regex para rodear el problema ⚠️
- **Commit:** _por completar_ (paso 05, `fix(dotnet): serialize money as exact JSON strings in the event contract`)
- **Prompt:** `/step 05` + prompt detallado del paso 05, que incluía: "En bronze, payload se guarda como
  string con el JSON crudo (verifica que from_json con StringType lo preserve así)".
- **Respuesta de la IA:**
  1. La IA de diseño (Claude chat), en la revisión del plan del paso 03, afirmó: "Spark, cuando el
     esquema declara un campo como `string` y recibe un objeto, lo guarda como texto JSON crudo, así que
     bronze queda en `payload string` y silver lo parsea."
  2. Claude Code lo probó dentro de `public.ecr.aws/glue/aws-glue-libs:5` antes de escribir el job, y era falso.
  3. En el plan, Claude Code propuso extraer el payload con
     `regexp_extract(value, '"payload":(\{.*\}),"trace_id":', 1)`, lo que acopla el contrato al orden de los
     campos del relay. Como plan B, si la regex no encontraba nada, guardaba el valor de `from_json`, que es
     justo el que se corrompe, y lo único que dejaba era un contador en el log.
- **Problema detectado:** Jackson reserializa los números JSON como `double`: `12345678.1234` →
  `1.23456781234E7`, y `123456789012345.6789` → `1.2345678901234567E14` (pierde precisión). La regex no
  arreglaba la causa: cualquier otro consumidor que lea el payload como números seguiría corrompiendo
  los montos, y el plan B metía en bronze datos alterados sin marcarlos en la tabla.
- **Decisión humana:** rechacé la regex y el plan B, y corregí la causa en el contrato. Los montos viajan
  como string JSON con su precisión exacta (`"amount":"12345678.1234"`), que es la convención para dinero.
  Se hizo con un converter de `decimal` en System.Text.Json, no con `JsonNumberHandling` global. Con eso,
  `from_json` con `StringType` es fiel sin regex. Hice `down -v` para que bronze no mezclara montos
  numéricos y montos como string. Lección: una afirmación técnica de la IA no es evidencia hasta que se ejecuta.
- **Evidencia:**
  - Prueba en la imagen de Glue (antes del cambio):
    `FROM_JSON : {"amount":1.23456781234E7,...,"big":1.2345678901234567E14,...}` frente a
    `EXPECTED  : {"amount":12345678.1234,...,"big":123456789012345.6789,...}`.
  - `src/dotnet/Makers.Core.Api/Infrastructure/Outbox/DecimalAsStringJsonConverter.cs:15,29` → converter.
  - `src/dotnet/Makers.Core.Api/Infrastructure/Outbox/OutboxInterceptor.cs:21-23` → solo en las opciones del payload.
  - `src/dotnet/Makers.Core.Tests/OutboxInterceptorTests.cs:55-58` → `SaveChanges_PayloadCarriesAmountAsExactString`
    con `12345678.1234`, `123456789012345.6789` y `150000`: passed (10/10 en `bash scripts/test.sh`).
  - `lakehouse/glue/tests/test_ingest_audit_stream.py:56,66` → `test_money_as_string_reaches_bronze_payload_identical`
    (los 2 montos) y `test_money_as_json_number_would_be_corrupted`: `6 passed`.
  - End-to-end tras `down -v`: el payload de Kafka (partición 0, offset 2) comparado con bronze da
    `IDENTICAL (byte-for-byte)`, con `"amount":"64348611"`. Bronze: 2500/2500 con amount como string.
- **ADR afectado:** ADR-001. Ya se actualizó en este mismo commit con la sección "Contrato del mensaje"
  (`docs/decisions.md:23-32`); lo pedí explícitamente.

### IA-11 · _título_
- **Commit:** `abc1234`
- **Prompt:**
- **Respuesta de la IA:**
- **Problema detectado:**
- **Decisión humana:**
- **Evidencia:**

---

## Métricas de uso (completar al final)
- Commits asistidos por IA: _n_ / _total_
- Correcciones humanas registradas: _n_
- Tiempo estimado ahorrado vs. tiempo invertido en verificar: _..._
