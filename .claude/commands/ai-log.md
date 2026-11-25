---
description: Registra en docs/ai-log.md una corrección o decisión humana sobre output de la IA
argument-hint: <qué propuso la IA y qué corregiste>
allowed-tools: Bash(git log:*), Bash(git diff:*), Read, Edit
---

Quiero registrar una entrada en `docs/ai-log.md`. Mi descripción:

> $ARGUMENTS

Contexto del repo:
- Últimos commits: !`git log -5 --oneline`

Instrucciones:
1. Lee `docs/ai-log.md` y usa el siguiente número correlativo (IA-NN) en la sección
   "Fase 2 — Implementación". Si existe la plantilla vacía `IA-07 · _título_`, reemplázala.
2. Llena el formato: **Commit**, **Prompt**, **Respuesta de la IA**, **Problema detectado**,
   **Decisión humana**, **Evidencia**.
3. Reglas de honestidad (esto se evalúa y se defiende en vivo):
   - Usa SOLO lo que yo describí y lo que puedes ver en esta conversación o en el repo. No inventes ni
     embellezcas el error ni la corrección.
   - Si te falta un dato (el prompt exacto, el commit), deja `_por completar_` en lugar de suponerlo.
   - Si el error fue tuyo en esta misma sesión, dilo tal cual.
4. En **Evidencia** pon algo verificable: hash de commit, comando con su salida, link a la doc oficial
   o el archivo y línea donde quedó la corrección.
5. Si la corrección cambia una decisión de arquitectura, indica qué ADR de `docs/decisions.md`
   habría que actualizar (no lo edites sin que te lo pida).
6. Muéstrame la entrada antes de guardarla.
