---
description: Ejecuta un paso del plan (docs/PLAN.md) con verificación antes del commit
argument-hint: <número de paso, ej. 05>
allowed-tools: Bash(git status:*), Bash(git log:*), Bash(git rev-parse:*), Bash(git add:*), Bash(docker:*), Bash(docker compose:*), Bash(make:*)
---

Vamos a ejecutar el **paso $ARGUMENTS** de `docs/PLAN.md`.

Estado actual del repo:
- Rama actual: !`git rev-parse --abbrev-ref HEAD`
- Último commit: !`git log -1 --oneline`
- Cambios pendientes: !`git status --short`

Instrucciones:
1. Lee `CLAUDE.md` (restricciones duras y decisiones ya tomadas) y la sección del paso $ARGUMENTS
   en `docs/PLAN.md`. Si hay cambios pendientes sin commitear de un paso anterior, o si la rama
   actual es `main`/`master`, avísame y espera antes de seguir. Yo manejo las ramas: no las crees ni
   las cambies.
2. Antes de escribir código, muéstrame un plan corto: archivos que vas a crear o modificar, versiones,
   imágenes y jars que vas a usar, y cómo las verificaste (por ejemplo `docker manifest inspect` o
   Maven Central). Si algo del paso choca con `CLAUDE.md` o con `docs/decisions.md`, dilo
   explícitamente en lugar de resolverlo en silencio.
3. Espera mi aprobación.
4. Implementa SOLO el alcance de ese paso. No agregues servicios, dependencias ni refactors no pedidos.
5. Corre la **verificación** que indica el paso y muéstrame la salida real. Si falla, diagnostica y
   corrige; si tras 2 intentos sigue fallando, detente y explícame la causa probable.
6. Si en el proceso cambiaste de enfoque respecto a lo que propusiste primero (versión incompatible,
   imagen inexistente, bug tuyo), resúmelo en 2 líneas para que yo decida si va a `docs/ai-log.md`.
7. Propón el mensaje de commit del PLAN (Conventional Commits) y **no hagas commit hasta que yo lo
   confirme**. Cuando lo confirme: `git add` + `git commit` en la rama actual y nada más.
   Termina mostrando rama, hash y "listo para push". El push lo hago yo.
