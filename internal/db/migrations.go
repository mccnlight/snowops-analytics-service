package db

import (
	"fmt"

	"gorm.io/gorm"
)

var migrationStatements = []string{
	`CREATE EXTENSION IF NOT EXISTS "uuid-ossp";`,
	`CREATE EXTENSION IF NOT EXISTS "pgcrypto";`,
	`CREATE EXTENSION IF NOT EXISTS "postgis";`,
	`DO $$
	BEGIN
		IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'trips') AND
		   NOT EXISTS (SELECT 1 FROM pg_matviews WHERE matviewname = 'mv_trip_daily') THEN
			CREATE MATERIALIZED VIEW mv_trip_daily AS
			SELECT
				DATE_TRUNC('day', tr.entry_at) AS bucket,
				t.contractor_id,
				t.created_by_org_id,
				t.cleaning_area_id,
				tr.driver_id,
				tr.vehicle_id,
				tr.polygon_id,
				COUNT(*) AS total_trips,
				COALESCE(SUM(tr.detected_volume_entry), 0) AS total_volume_m3,
				SUM(CASE WHEN tr.status <> 'OK' THEN 1 ELSE 0 END) AS violation_count
			FROM trips tr
			LEFT JOIN tickets t ON t.id = tr.ticket_id
			GROUP BY 1, t.contractor_id, t.created_by_org_id, t.cleaning_area_id, tr.driver_id, tr.vehicle_id, tr.polygon_id;
		END IF;
	END
	$$;`,
	`DO $$
	BEGIN
		IF EXISTS (SELECT 1 FROM pg_matviews WHERE matviewname = 'mv_trip_daily') THEN
			CREATE INDEX IF NOT EXISTS idx_mv_trip_daily_bucket ON mv_trip_daily (bucket);
			CREATE INDEX IF NOT EXISTS idx_mv_trip_daily_contractor ON mv_trip_daily (contractor_id, created_by_org_id);
		END IF;
	END
	$$;`,
	`DO $$
	BEGIN
		IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'trips') AND
		   NOT EXISTS (SELECT 1 FROM pg_matviews WHERE matviewname = 'mv_violation_daily') THEN
			CREATE MATERIALIZED VIEW mv_violation_daily AS
			SELECT
				DATE_TRUNC('day', tr.entry_at) AS bucket,
				t.contractor_id,
				t.created_by_org_id,
				t.cleaning_area_id,
				tr.driver_id,
				tr.status AS violation_type,
				COUNT(*) AS violation_count
			FROM trips tr
			LEFT JOIN tickets t ON t.id = tr.ticket_id
			WHERE tr.status <> 'OK'
			GROUP BY 1, t.contractor_id, t.created_by_org_id, t.cleaning_area_id, tr.driver_id, tr.status;
		END IF;
	END
	$$;`,
	`DO $$
	BEGIN
		IF EXISTS (SELECT 1 FROM pg_matviews WHERE matviewname = 'mv_violation_daily') THEN
			CREATE INDEX IF NOT EXISTS idx_mv_violation_daily_bucket ON mv_violation_daily (bucket);
			CREATE INDEX IF NOT EXISTS idx_mv_violation_daily_contractor ON mv_violation_daily (contractor_id, created_by_org_id);
		END IF;
	END
	$$;`,
	`DO $$
	BEGIN
		IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'trips') AND
		   NOT EXISTS (SELECT 1 FROM pg_matviews WHERE matviewname = 'mv_contract_daily') THEN
			CREATE MATERIALIZED VIEW mv_contract_daily AS
			SELECT
				DATE_TRUNC('day', tr.entry_at) AS bucket,
				t.contract_id,
				t.contractor_id,
				t.created_by_org_id,
				COUNT(*) AS total_trips,
				COALESCE(SUM(tr.detected_volume_entry), 0) AS total_volume_m3,
				SUM(CASE WHEN tr.status <> 'OK' THEN 1 ELSE 0 END) AS violation_count
			FROM trips tr
			JOIN tickets t ON t.id = tr.ticket_id
			GROUP BY 1, t.contract_id, t.contractor_id, t.created_by_org_id;
		END IF;
	END
	$$;`,
	`DO $$
	BEGIN
		IF EXISTS (SELECT 1 FROM pg_matviews WHERE matviewname = 'mv_contract_daily') THEN
			CREATE INDEX IF NOT EXISTS idx_mv_contract_daily_bucket ON mv_contract_daily (bucket);
			CREATE INDEX IF NOT EXISTS idx_mv_contract_daily_contract ON mv_contract_daily (contract_id);
		END IF;
	END
	$$;`,
	`DO $$
	BEGIN
		IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'trips') AND
		   NOT EXISTS (SELECT 1 FROM pg_matviews WHERE matviewname = 'mv_cleaning_area_daily') THEN
			CREATE MATERIALIZED VIEW mv_cleaning_area_daily AS
			SELECT
				DATE_TRUNC('day', tr.entry_at) AS bucket,
				t.cleaning_area_id,
				t.contractor_id,
				t.created_by_org_id,
				COUNT(*) AS total_trips,
				COALESCE(SUM(tr.detected_volume_entry), 0) AS total_volume_m3,
				SUM(CASE WHEN tr.status <> 'OK' THEN 1 ELSE 0 END) AS violation_count,
				COUNT(DISTINCT tr.driver_id) AS active_drivers,
				COUNT(DISTINCT tr.vehicle_id) AS active_vehicles,
				MIN(tr.entry_at) AS first_entry_at,
				MAX(COALESCE(tr.exit_at, tr.entry_at)) AS last_exit_at
			FROM trips tr
			JOIN tickets t ON t.id = tr.ticket_id
			GROUP BY 1, t.cleaning_area_id, t.contractor_id, t.created_by_org_id;
		END IF;
	END
	$$;`,
	`DO $$
	BEGIN
		IF EXISTS (SELECT 1 FROM pg_matviews WHERE matviewname = 'mv_cleaning_area_daily') THEN
			CREATE INDEX IF NOT EXISTS idx_mv_cleaning_area_daily_bucket ON mv_cleaning_area_daily (bucket);
			CREATE INDEX IF NOT EXISTS idx_mv_cleaning_area_daily_area ON mv_cleaning_area_daily (cleaning_area_id);
		END IF;
	END
	$$;`,
}

func runMigrations(db *gorm.DB) error {
	for i, stmt := range migrationStatements {
		if err := db.Exec(stmt).Error; err != nil {
			return fmt.Errorf("migration %d failed: %w", i+1, err)
		}
	}
	return nil
}
