# Snowops Analytics Service

Snowops Analytics implements EPIC 6 requirements: dashboards, trip analytics, violations, contractor/driver KPIs, camera and polygon monitoring, contract tracking, and RLS per organization.

## Highlights

- Real-time dashboard: active/completed trips, tickets, violations, camera load, contract progress, city heatmap.
- Trip analytics: time series (day/week/month), top drivers/contractors, average duration & volume, detailed trip card with LPR/Volume photos.
- Violation analytics: distribution by type, rankings per contractor/driver/camera, daily trend, heat indicators for areas/polygons.
- Performance: contractors, drivers, vehicles KPIs (utilization, violation rate, average fill rate, idle time).
- Area-focused view: frequency of cleaning, idle hours, responsible drivers, GeoJSON polygons and heat levels.
- Driver/vehicle registers: KPI lists with last trip timestamps for operational follow-up.
- Technical dashboard for TOO/Akimat: camera health, polygon loads, aggregated LPR/Volume event statistics.
- Contract & budget view: SUCCESS/FAIL, budget usage, minimal volume progress, risky/over-budget contracts.
- Materialized views (`mv_trip_daily`, `mv_violation_daily`, `mv_contract_daily`, `mv_cleaning_area_daily`) keep analytics fast; they can be refreshed on schedule.
- JWT-based RLS: Akimat sees city-wide data, KGU sees its contractors, Contractor sees only own org, TOO sees technical telemetry, drivers denied.

## Requirements

- Go 1.23+
- PostgreSQL 15+ with `uuid-ossp`, `pgcrypto`, `postgis` extensions

## Quick start

```bash
cd deploy
docker compose up -d

cd ..
APP_ENV=development \
DB_DSN="postgres://postgres:postgres@localhost:5440/analytics_db?sslmode=disable" \
JWT_ACCESS_SECRET="secret-key" \
go run ./cmd/analytics-service
```

## Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `APP_ENV` | Environment (`development` / `production`) | `development` |
| `HTTP_HOST` / `HTTP_PORT` | HTTP bind | `0.0.0.0` / `7085` |
| `DB_DSN` | Postgres DSN | `postgres://postgres:postgres@localhost:5440/analytics_db?sslmode=disable` |
| `DB_MAX_OPEN_CONNS` / `DB_MAX_IDLE_CONNS` | Connection pool | `25` / `10` |
| `DB_CONN_MAX_LIFETIME` | Connection TTL | `1h` |
| `JWT_ACCESS_SECRET` | JWT verification secret | — |
| `ANALYTICS_DEFAULT_RANGE_DAYS` | Default range (days back) | `7` |
| `ANALYTICS_MAX_RANGE_DAYS` | Max range (days) | `90` |

## API (all endpoints require `Authorization: Bearer <jwt>`)

- `GET /healthz` — service health.
- `GET /analytics/dashboard` — summary metrics, contractors, cameras, map overlays (query: `from`, `to`).
- `GET /analytics/trips` — time series, TOP drivers/contractors, duration/volume stats (`from`, `to`, `group_by`, `contractor_id`, `driver_id`).
- `GET /analytics/trips/{id}` — trip card with assignments, media, violations.
- `GET /analytics/violations` — trend & distribution of violations with leaders (`from`, `to`, `group_by`, filters).
- `GET /analytics/performance` — contractor/driver/vehicle KPIs (`from`, `to`, `group_by`).
- `GET /analytics/contracts` — contract summary (SUCCESS/FAIL, budget, risk flags).
- `GET /analytics/areas` — per cleaning-area KPI (frequency, idle hours, GeoJSON, volume) (`from`, `to`, `contractor_id`).
- `GET /analytics/drivers` — driver KPI list with last trip timestamp (`from`, `to`, `contractor_id`, `driver_id`).
- `GET /analytics/vehicles` — vehicle KPI list (fill rate, idle hours) (`from`, `to`, `contractor_id`).
- `GET /analytics/technical` — camera/polygon technical telemetry for TOO/Akimat (`from`, `to`).

## Architecture

- `internal/db` — GORM wrapper + migrations (extensions + materialized views).
- `internal/model` — DTOs for dashboards, KPIs, technical data.
- `internal/repository` — SQL aggregations over core tables and materialized views.
- `internal/service` — range normalization, RLS enforcement, orchestration.
- `internal/http` — Gin router, auth middleware, JSON API.

The service reads existing Snowops domain tables (`trips`, `tickets`, `ticket_assignments`, `contracts`, `contract_usage`, `lpr_events`, `volume_events`, `cleaning_areas`, `polygons`, `cameras`, `organizations`, `drivers`, `vehicles`).
