# Repository working notes

## Render log exports

When asked to export or inspect Render logs, first read
`docs/RENDER_LOG_EXPORT.md`. Reuse `scripts/export-render-logs.ps1` and the
documented PowerShell commands instead of recreating a CLI or pagination recipe.

The user's export directory is `C:\Users\artbe\Projects\render`. The guide covers
24-hour and 7-day exports, process-scoped execution-policy handling, secure API-key
input, and known failures. Never print, store in Git, or recover and reuse secret
API-key values from chat screenshots or shell history.
