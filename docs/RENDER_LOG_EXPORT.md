# Render-logs exporteren vanuit VS Code

Werkende werkwijze bevestigd op 4 september 2026. Dit is de samenvatting van de
exportconversatie, zonder API-keys of andere geheime waarden.

## Snel: laatste 7 dagen

Gebruik de **PowerShell-terminal** in VS Code, niet Bash. Voer de volgende twee
opdrachten afzonderlijk uit. De huidige werkmap maakt niet uit.

### 1. API-key afgeschermd invoeren

Voer deze opdracht ONGEWIJZIGD uit. Laat de tekst `Render API-key` letterlijk staan.

```powershell
$env:RENDER_API_KEY = ([System.Net.NetworkCredential]::new("", (Read-Host "Render API-key" -AsSecureString)).Password).Trim()
```

Pas wanneer `Render API-key:` verschijnt, plak je de volledige Render API-key en
druk je op Enter. Plak ALLEEN de key: geen aanhalingstekens, geen `Bearer`, geen
gemaskeerde sterretjes en geen volledige opdracht. De invoer wordt afgeschermd.

De key staat alleen in de omgeving van deze terminalsessie en wordt doorgegeven
aan het exportproces. In een nieuwe terminal moet je deze stap opnieuw uitvoeren.
Zet de key nooit letterlijk in de opdracht, documentatie, chat of Git.

### 2. Weekexport starten

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "C:\Users\artbe\tradingview-alert-connector\scripts\export-render-logs.ps1" -Days 7 -OutFile "C:\Users\artbe\Projects\render\logs-7d.txt"
```

De tijdelijke execution-policy geldt alleen voor dit PowerShell-proces. Er is
geen permanente policywijziging of administratorvenster nodig.

De export toont voortgang per pagina en eindigt met:

```text
Klaar: ... logs opgeslagen in C:\Users\artbe\Projects\render\logs-7d.txt
```

De export betreft de laatste 7 x 24 uur, begrensd door Render-retentie en de
daadwerkelijk beschikbare logs. Een bestaande `logs-7d.txt` wordt aan het begin
verwijderd; een mislukte of onderbroken export kan dus leeg of onvolledig zijn.
`logs-24h.txt` blijft bij deze opdracht intact.

## Laatste 24 uur

Gebruik dezelfde API-key-invoerstap en vervolgens:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "C:\Users\artbe\tradingview-alert-connector\scripts\export-render-logs.ps1" -Days 1 -OutFile "C:\Users\artbe\Projects\render\logs-24h.txt"
```

## Eerder opgeloste fouten

| Melding of situatie | Oorzaak en oplossing |
|---|---|
| `running scripts is disabled` | Gebruik de volledige opdracht met `powershell.exe -NoProfile -ExecutionPolicy Bypass -File`, niet alleen `& ...ps1`. |
| `RENDER_API_KEY is niet ingesteld` | Voer stap 1 uit in DEZELFDE terminal waarin je stap 2 start. |
| `HTTP 401 Unauthorized` | Render accepteert de key niet. In deze conversatie waren meegeplakte AANHALINGSTEKENS de oorzaak. Voer de kale key opnieuw in. Controleer anders of hij compleet, geldig en niet ingetrokken is. `.Trim()` verwijdert witruimte, niet aanhalingstekens. |
| Key zichtbaar als prompt of in een screenshot | De key is per ongeluk in de vraagtekst gezet. Trek die key in, maak een nieuwe en laat `"Render API-key"` in de opdracht ongewijzigd. |
| `catch is not recognized` | Een los deel van een meerregelig script werd uitgevoerd. Gebruik het bestaande `.ps1`-bestand, geen grote los geplakte `try/catch`-blokken. |
| `render.exe is not recognized` | De oude CLI-opdracht was afhankelijk van de werkmap. Deze API-export heeft `render.exe` niet nodig. |
| `invalid limit: too large` | Geen `--limit 10000` gebruiken. Het script haalt steeds maximaal 100 regels op en loopt automatisch door alle pagina's. |
| API-rate-limit of tijdelijke netwerkfout | Het script wacht tussen pagina's en probeert tijdelijke fouten beperkt opnieuw. Herstart niet telkens de hele export; bij een definitieve fout toont het script de HTTP-status. |

Maak of beheer API-keys via Render Account Settings -> API Keys. De volledige
waarde wordt bij het aanmaken getoond. Zie <https://render.com/docs/api>.
De uiteindelijke logexport kan zelf gevoelige inhoud bevatten; commit die niet.

## Voor volgende sessies

- Hergebruik `scripts/export-render-logs.ps1`; bouw de export niet opnieuw.
- Script: `C:\Users\artbe\tradingview-alert-connector\scripts\export-render-logs.ps1`.
- Exportmap: `C:\Users\artbe\Projects\render`.
- Render-resource: `srv-d5ubre94tr6s73el8hh0`.
- Render-owner: `tea-d5r0epshg0os73cli8jg`.
- Het script gebruikt de REST API `/v1/logs`, maximaal 100 regels per pagina,
  `hasMore`/`nextStartTime`/`nextEndTime` en 2.200 ms wachttijd tussen pagina's.
- De oorspronkelijke 24-uursvariant stond als geplakt PowerShell-blok in de
  opdrachtgeschiedenis; het huidige script vervangt dat foutgevoelige plakken.
- Geen wijziging aan de tradingservice of deploy nodig om lokaal logs te exporteren.
