#!/usr/bin/env bash
# Smoke test — spins up local Postgres + Metabase, seeds fake data, runs dbt.
# Requirements: Docker, Python 3.11+, psycopg2-binary, dbt-postgres installed.

set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$SCRIPT_DIR/.."
PYTHON="$ROOT/../.venv/bin/python3"

echo "==> Starting Postgres + Metabase containers..."
docker compose -f "$SCRIPT_DIR/docker-compose.smoke.yml" up -d

echo "==> Waiting for Postgres to be ready..."
until docker exec xero-smoke-pg pg_isready -U xero -d xero_smoke -q 2>/dev/null; do
  printf '.'
  sleep 2
done
echo " ready."

echo "==> Seeding fake data..."
cd "$ROOT"
"$PYTHON" smoke_test/seed_data.py

echo "==> Running dbt build (target=smoke)..."
cd "$ROOT/xero_dbt"
"$ROOT/../.venv/bin/dbt" build --profiles-dir . --target smoke --select "bronze silver gold"

echo ""
echo "============================================================"
echo "  Smoke test complete!"
echo "============================================================"
echo ""
echo "  Metabase:  http://localhost:3001"
echo ""
echo "  Connect Metabase to Postgres using these details:"
echo "    Host:     host.docker.internal"
echo "    Port:     5433"
echo "    Database: xero_smoke"
echo "    Username: xero"
echo "    Password: xero_smoke_pw"
echo ""
echo "  Gold views to query:"
echo "    gold.gold_ceo_weekly_dashboard   <- start here"
echo "    gold.gold_ar_aging"
echo "    gold.gold_cash_position"
echo "    gold.gold_revenue_by_type"
echo "    gold.gold_stripe_movement"
echo ""
echo "  To tear down:"
echo "    docker compose -f smoke_test/docker-compose.smoke.yml down -v"
echo ""

# open browser if possible
if command -v open &>/dev/null; then
  echo "==> Opening browser in 15s (waiting for Metabase to start)..."
  sleep 15 && open http://localhost:3001 &
fi
