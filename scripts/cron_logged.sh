#!/bin/sh
# scripts/cron_logged.sh <logfile> <url> — Heatr-cron-call met timestamped log-regel.
#
# Waarom: de oude cron-regels appendden curl-output zonder timestamp of newline
# (write-only blobs; de 401-storm na een key-rotatie was onzichtbaar) én hadden de
# API-key hardcoded. Deze wrapper leest de key bij RUNTIME uit .env — key-rotatie
# breekt cron dus nooit meer — en schrijft één nette regel per run.
#
# Gebruik (crontab):
#   0 8 * * * /Users/nemesis/Heatr/scripts/cron_logged.sh /Users/nemesis/Heatr/logs/discovery.log http://localhost:8001/discovery-schedules/run-due
set -u
LOG="$1"
URL="$2"
ENV_FILE="/Users/nemesis/Heatr/.env"

KEY=$(grep -E '^HEATR_API_KEY=' "$ENV_FILE" | head -1 | cut -d= -f2-)
STAMP=$(date '+%Y-%m-%d %H:%M:%S')

{
    printf '[%s] %s → ' "$STAMP" "$URL"
    curl -sS --max-time 60 -X POST -H "X-API-Key: $KEY" "$URL" 2>&1
    printf '\n'
} >> "$LOG"
