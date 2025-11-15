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

## Endpoint details

All requests require `Authorization: Bearer <jwt>` and support RFC 3339 timestamps.

### Dashboard – `GET /analytics/dashboard`

Query params: `from`, `to` (optional).

```
GET /analytics/dashboard?from=2025-01-01T00:00:00Z&to=2025-01-07T23:59:59Z
Authorization: Bearer <akimat_jwt>
```

```json
{
  "data": {
    "stats": {
      "active_trips": 42,
      "completed_trips": 318,
      "violations": 27,
      "tickets_in_progress": 58
    },
    "contractors": {
      "active": [{ "id": "42e5…", "name": "Contractor LLP", "count": 180, "share": 0.41 }],
      "idle": [{ "id": "3ab1…", "name": "Nova Build" }]
    },
    "contracts": [ { "contract_id": "c1ad…", "ui_status": "ACTIVE", "budget_progress": 0.43, "volume_progress": 0.52 } ],
    "map": {
      "areas": [{ "id": "a8d1…", "has_trips": true, "intensity": 0.76 }],
      "polygons": [{ "id": "f91c…", "trip_count": 43, "volume_m3": 512.3 }],
      "cameras": [{ "camera_id": "0f12…", "camera_name": "Cam-12", "error_events": 3 }]
    },
    "cameras": [{ "camera_id": "0f12…", "lpr_events": 430, "volume_events": 428, "error_rate": 0.01 }]
  }
}
```

### Trips

#### `GET /analytics/trips`

Params: `from`, `to`, `group_by` (`day|week|month`), `contractor_id`, `driver_id`.

```
GET /analytics/trips?from=2025-01-01T00:00:00Z&to=2025-01-31T23:59:59Z&group_by=week
Authorization: Bearer <kgu_jwt>
```

```json
{
  "data": {
    "series": [
      { "bucket": "2025-01-06T00:00:00Z", "count": 180 },
      { "bucket": "2025-01-13T00:00:00Z", "count": 210 }
    ],
    "volume_series": [
      { "bucket": "2025-01-06T00:00:00Z", "count": 180, "value": 4200.5 }
    ],
    "top_drivers": [{ "id": "drv-1…", "name": "Aidos Nur", "count": 34 }],
    "top_contractors": [{ "id": "ctr-3…", "name": "Contractor LLP", "count": 120 }],
    "duration_stats": { "avg_minutes": 35, "p90_minutes": 52 },
    "volume_stats": { "avg_m3": 14.2, "p90_m3": 19.8 }
  }
}
```

#### `GET /analytics/trips/{id}`

```
GET /analytics/trips/a7ac4d08-6c93-46bb-9f38-5b88b29be8a4
Authorization: Bearer <jwt>
```

Response includes trip metadata, linked ticket/contractor, LPR/volume photo URLs, violations and assignment info.

### Violations analytics – `GET /analytics/violations`

Params: `from`, `to`, `group_by`, `contractor_id`, `driver_id`, `violation_type`.

```
GET /analytics/violations?from=2025-01-01T00:00:00Z&to=2025-01-15T23:59:59Z
Authorization: Bearer <akimat_jwt>
```

```json
{
  "data": {
    "series": [
      { "bucket": "2025-01-01T00:00:00Z", "count": 5 },
      { "bucket": "2025-01-02T00:00:00Z", "count": 7 }
    ],
    "breakdown": [
      { "name": "ROUTE_VIOLATION", "count": 6 },
      { "name": "MISMATCH_PLATE", "count": 4 }
    ],
    "top_contractors": [{ "id": "ctr-1…", "name": "Contractor LLP", "count": 4 }],
    "top_drivers": [{ "id": "drv-2…", "name": "Bauyrzhan S.", "count": 3 }],
    "top_cameras": [{ "camera_id": "cam-4…", "camera_name": "Cam-17", "error_events": 2 }]
  }
}
```

### Performance – `GET /analytics/performance`

Params: `from`, `to`, `group_by`.

```
GET /analytics/performance?from=2025-01-01T00:00:00Z&to=2025-01-31T23:59:59Z
Authorization: Bearer <kgu_jwt>
```

Returns `contractors`, `drivers`, `vehicles` arrays with utilization, violation_rate, avg_fill_rate, idle_hours.

### Contracts – `GET /analytics/contracts`

```
GET /analytics/contracts
Authorization: Bearer <akimat_jwt>
```

```json
{
  "data": {
    "summary": [
      {
        "contract_id": "0c52…",
        "contractor_name": "Contractor LLP",
        "budget_progress": 0.63,
        "volume_progress": 0.58,
        "ui_status": "ACTIVE",
        "result": "NONE"
      }
    ],
    "top_budget": [ { "contract_id": "…" } ],
    "at_risk": [ { "contract_id": "…", "result": "FAIL" } ],
    "budget_issues": [ { "contract_id": "…", "budget_progress": 1.12 } ]
  }
}
```

### Areas – `GET /analytics/areas`

Params: `from`, `to`, `contractor_id`.

```
GET /analytics/areas?from=2025-01-05T00:00:00Z&to=2025-01-12T23:59:59Z
Authorization: Bearer <kgu_jwt>
```

Response fields per area: `trip_count`, `volume_m3`, `violation_count`, `active_drivers`, `active_vehicles`, `avg_interval_hours`, `idle_hours`, `geometry_geojson`.

### Drivers – `GET /analytics/drivers`

```
GET /analytics/drivers?from=2025-01-01T00:00:00Z&to=2025-01-31T23:59:59Z
Authorization: Bearer <contractor_jwt>
```

Returns driver KPIs (`trip_count`, `violation_rate`, `avg_volume_m3`, `last_trip_at`). Same request structure applies to `/analytics/vehicles`.

### Technical – `GET /analytics/technical`

Only Akimat/KGU/TOO tokens allowed.

```
GET /analytics/technical?from=2025-01-10T00:00:00Z&to=2025-01-10T23:59:59Z
Authorization: Bearer <too_jwt>
```

```json
{
  "data": {
    "cameras": [
      { "camera_id": "0f12…", "camera_name": "Cam-12", "lpr_events": 120, "volume_events": 118, "error_events": 1 }
    ],
    "polygons": [
      { "polygon_id": "f91c…", "polygon_name": "Polygon #12", "trip_count": 43, "volume": 512.3, "errors": 2 }
    ],
    "error_rate": 0.014,
    "event_frequency": 8.3,
    "last_event_at": "2025-01-10T18:55:00Z"
  }
}
```

## Architecture

- `internal/db` — GORM wrapper + migrations (extensions + materialized views).
- `internal/model` — DTOs for dashboards, KPIs, technical data.
- `internal/repository` — SQL aggregations over core tables and materialized views.
- `internal/service` — range normalization, RLS enforcement, orchestration.
- `internal/http` — Gin router, auth middleware, JSON API.

The service reads existing Snowops domain tables (`trips`, `tickets`, `ticket_assignments`, `contracts`, `contract_usage`, `lpr_events`, `volume_events`, `cleaning_areas`, `polygons`, `cameras`, `organizations`, `drivers`, `vehicles`).
