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

### IA-08 · _título_
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
