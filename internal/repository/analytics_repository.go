package repository

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"analytics-service/internal/model"
)

type AnalyticsRepository struct {
	db *gorm.DB
}

func NewAnalyticsRepository(db *gorm.DB) *AnalyticsRepository {
	return &AnalyticsRepository{db: db}
}

func (r *AnalyticsRepository) DashboardStats(ctx context.Context, scope model.Scope, rng model.DateRange) (model.DashboardStats, error) {
	if !r.tablesAvailable(ctx, "trips", "tickets") {
		return model.DashboardStats{}, nil
	}

	var stats model.DashboardStats

	timeRangeFrom := rng.From
	timeRangeTo := rng.To

	query := r.db.WithContext(ctx).
		Table("trips tr").
		Select(`
			SUM(CASE WHEN tr.exit_at IS NULL THEN 1 ELSE 0 END) AS active_trips,
			SUM(CASE WHEN tr.exit_at IS NOT NULL AND tr.entry_at BETWEEN ? AND ? THEN 1 ELSE 0 END) AS completed_trips,
			SUM(CASE WHEN tr.status <> 'OK' AND tr.entry_at BETWEEN ? AND ? THEN 1 ELSE 0 END) AS violations`,
			timeRangeFrom, timeRangeTo, timeRangeFrom, timeRangeTo).
		Joins("LEFT JOIN tickets t ON t.id = tr.ticket_id")

	query = applyTripScope(query, scope)

	if err := query.Scan(&stats).Error; err != nil {
		return model.DashboardStats{}, err
	}

	var ticketsInProgress int64
	ticketQuery := r.db.WithContext(ctx).
		Table("tickets t").
		Where("t.status IN ?", []string{"IN_PROGRESS", "COMPLETED"})
	ticketQuery = applyTicketScope(ticketQuery, scope)
	if err := ticketQuery.Count(&ticketsInProgress).Error; err != nil {
		return model.DashboardStats{}, err
	}
	stats.TicketsInProgress = ticketsInProgress

	return stats, nil
}

func (r *AnalyticsRepository) CleaningAreaActivity(ctx context.Context, scope model.Scope, rng model.DateRange) ([]model.CleaningAreaActivity, error) {
	if !r.tablesAvailable(ctx, "trips", "tickets") {
		return nil, nil
	}

	type row struct {
		CleaningAreaID uuid.UUID
		Trips          int64
		ActiveTrips    int64
		HasViolations  int64
	}
	var rows []row

	query := r.db.WithContext(ctx).
		Table("trips tr").
		Select(`t.cleaning_area_id AS cleaning_area_id,
			COUNT(*) AS trips,
			SUM(CASE WHEN tr.exit_at IS NULL THEN 1 ELSE 0 END) AS active_trips,
			MAX(CASE WHEN tr.status <> 'OK' THEN 1 ELSE 0 END) AS has_violations`).
		Joins("JOIN tickets t ON t.id = tr.ticket_id").
		Where("tr.entry_at BETWEEN ? AND ?", rng.From, rng.To).
		Group("t.cleaning_area_id")

	query = applyTripScope(query, scope)

	if err := query.Scan(&rows).Error; err != nil {
		return nil, err
	}

	var maxTrips int64
	for _, row := range rows {
		if row.Trips > maxTrips {
			maxTrips = row.Trips
		}
	}

	result := make([]model.CleaningAreaActivity, 0, len(rows))
	for _, row := range rows {
		heat := 0.0
		if maxTrips > 0 {
			heat = float64(row.Trips) / float64(maxTrips)
		}
		result = append(result, model.CleaningAreaActivity{
			CleaningAreaID: row.CleaningAreaID,
			Trips:          row.Trips,
			ActiveTrips:    row.ActiveTrips,
			HasViolations:  row.HasViolations > 0,
			TripHeat:       heat,
		})
	}

	return result, nil
}

func (r *AnalyticsRepository) CleaningAreaAnalytics(ctx context.Context, scope model.Scope, filter model.AnalyticsFilter) ([]model.CleaningAreaAnalytics, error) {
	if !r.relationExists(ctx, "mv_cleaning_area_daily") {
		return nil, nil
	}

	type row struct {
		CleaningAreaID uuid.UUID
		Name           *string
		Description    *string
		TripCount      int64
		VolumeM3       float64
		ViolationCount int64
		ActiveDrivers  int64
		ActiveVehicles int64
		FirstEntry     *time.Time
		LastExit       *time.Time
		Geometry       *string
	}
	var rows []row

	query := r.db.WithContext(ctx).
		Table("mv_cleaning_area_daily mv").
		Select(`mv.cleaning_area_id,
			COALESCE(ca.name, 'Cleaning area') AS name,
			ca.description,
			SUM(mv.total_trips) AS trip_count,
			COALESCE(SUM(mv.total_volume_m3), 0) AS volume_m3,
			SUM(mv.violation_count) AS violation_count,
			SUM(mv.active_drivers) AS active_drivers,
			SUM(mv.active_vehicles) AS active_vehicles,
			MIN(mv.first_entry_at) AS first_entry,
			MAX(mv.last_exit_at) AS last_exit,
			COALESCE(ST_AsGeoJSON(ca.geometry)::text, NULL) AS geometry`).
		Joins("LEFT JOIN cleaning_areas ca ON ca.id = mv.cleaning_area_id").
		Where("mv.bucket BETWEEN ? AND ?", filter.Range.From, filter.Range.To).
		Group("mv.cleaning_area_id, ca.name, ca.description, ca.geometry")

	query = applyMVCleaningAreaScope(query, scope)

	if err := query.Scan(&rows).Error; err != nil {
		return nil, err
	}

	result := make([]model.CleaningAreaAnalytics, 0, len(rows))
	for _, row := range rows {
		name := "Cleaning area"
		if row.Name != nil && *row.Name != "" {
			name = *row.Name
		}
		avgInterval := 0.0
		if row.TripCount > 1 && row.FirstEntry != nil && row.LastExit != nil {
			total := row.LastExit.Sub(*row.FirstEntry).Hours()
			if total > 0 {
				avgInterval = total / float64(row.TripCount)
			}
		}
		idle := 0.0
		if row.LastExit != nil {
			if delta := filter.Range.To.Sub(*row.LastExit).Hours(); delta > 0 {
				idle = delta
			}
		}
		result = append(result, model.CleaningAreaAnalytics{
			CleaningAreaID:   row.CleaningAreaID,
			Name:             name,
			Description:      row.Description,
			TripCount:        row.TripCount,
			VolumeM3:         row.VolumeM3,
			ViolationCount:   row.ViolationCount,
			ActiveDrivers:    row.ActiveDrivers,
			ActiveVehicles:   row.ActiveVehicles,
			AvgIntervalHours: clamp(avgInterval),
			IdleHours:        clamp(idle),
			LastTripAt:       row.LastExit,
			GeometryGeoJSON:  row.Geometry,
		})
	}

	return result, nil
}

func (r *AnalyticsRepository) ContractorActivitySplit(ctx context.Context, scope model.Scope, rng model.DateRange) (active []model.EntityMetric, idle []model.EntityMetric, err error) {
	if !r.tablesAvailable(ctx, "trips", "tickets", "organizations") {
		return nil, nil, nil
	}

	type activeRow struct {
		ID     uuid.UUID
		Name   string
		Count  int64
		Volume float64
	}
	var rows []activeRow

	query := r.db.WithContext(ctx).
		Table("trips tr").
		Select(`t.contractor_id AS id, COALESCE(org.name, 'Unknown') AS name,
			COUNT(*) AS count,
			COALESCE(SUM(tr.detected_volume_entry), 0) AS volume`).
		Joins("JOIN tickets t ON t.id = tr.ticket_id").
		Joins("LEFT JOIN organizations org ON org.id = t.contractor_id").
		Where("tr.entry_at BETWEEN ? AND ?", rng.From, rng.To).
		Group("t.contractor_id, org.name")

	query = applyTripScope(query, scope)

	if err := query.Scan(&rows).Error; err != nil {
		return nil, nil, err
	}

	total := float64(0)
	for _, row := range rows {
		total += float64(row.Count)
	}

	for _, row := range rows {
		share := 0.0
		if total > 0 {
			share = float64(row.Count) / total
		}
		active = append(active, model.EntityMetric{
			ID:     row.ID,
			Name:   row.Name,
			Count:  row.Count,
			Volume: row.Volume,
			Share:  share,
		})
	}

	// Idle contractors for Akimat/KGU scopes
	if scope.Type == model.ScopeCity || scope.Type == model.ScopeKgu {
		ids := make([]uuid.UUID, 0, len(rows))
		for _, row := range rows {
			ids = append(ids, row.ID)
		}

		orgQuery := r.db.WithContext(ctx).
			Table("organizations org").
			Select("org.id, org.name").
			Where("org.type = ? AND org.is_active = ?", orgTypeContractor, true)

		if scope.Type == model.ScopeKgu && scope.OrgID != nil {
			orgQuery = orgQuery.Where("org.parent_org_id = ?", *scope.OrgID)
		}

		if len(ids) > 0 {
			orgQuery = orgQuery.Where("org.id NOT IN ?", ids)
		}

		var idleRows []struct {
			ID   uuid.UUID
			Name string
		}
		if err := orgQuery.Scan(&idleRows).Error; err != nil {
			return nil, nil, err
		}
		for _, row := range idleRows {
			idle = append(idle, model.EntityMetric{ID: row.ID, Name: row.Name})
		}
	}

	return active, idle, nil
}

func (r *AnalyticsRepository) CameraLoad(ctx context.Context, scope model.Scope, rng model.DateRange) ([]model.CameraLoadMetric, error) {
	if !r.tablesAvailable(ctx, "cameras", "polygons", "trips", "lpr_events", "volume_events") {
		return nil, nil
	}

	type row struct {
		CameraID     uuid.UUID
		CameraName   string
		PolygonID    *uuid.UUID
		PolygonName  *string
		LprEvents    int64
		VolumeEvents int64
		ErrorEvents  int64
	}
	var rows []row

	subLpr := r.db.WithContext(ctx).
		Table("lpr_events").
		Select("camera_id, COUNT(*) AS cnt").
		Where("detected_at BETWEEN ? AND ?", rng.From, rng.To).
		Group("camera_id")

	subVolume := r.db.WithContext(ctx).
		Table("volume_events").
		Select("camera_id, COUNT(*) AS cnt").
		Where("detected_at BETWEEN ? AND ?", rng.From, rng.To).
		Group("camera_id")

	subErrors := r.db.WithContext(ctx).
		Table("trips").
		Select("camera_id, COUNT(*) AS cnt").
		Where("camera_id IS NOT NULL AND status IN ? AND entry_at BETWEEN ? AND ?",
			[]string{"NO_LPR_EVENT", "NO_VOLUME_EVENT", "CAMERA_ERROR", "MISMATCH_PLATE"}, rng.From, rng.To).
		Group("camera_id")

	query := r.db.WithContext(ctx).
		Table("cameras c").
		Select(`c.id AS camera_id,
			COALESCE(c.name, 'Camera') AS camera_name,
			c.polygon_id AS polygon_id,
			subp.name AS polygon_name,
			COALESCE(l.cnt, 0) AS lpr_events,
			COALESCE(v.cnt, 0) AS volume_events,
			COALESCE(e.cnt, 0) AS error_events`).
		Joins("LEFT JOIN polygons subp ON subp.id = c.polygon_id").
		Joins("LEFT JOIN (?) AS l ON l.camera_id = c.id", subLpr).
		Joins("LEFT JOIN (?) AS v ON v.camera_id = c.id", subVolume).
		Joins("LEFT JOIN (?) AS e ON e.camera_id = c.id", subErrors)

	if scope.Type != model.ScopeCity && scope.Type != model.ScopeTechnical {
		cameraIDs := r.db.WithContext(ctx).
			Table("trips tr").
			Select("DISTINCT tr.camera_id").
			Joins("JOIN tickets t ON t.id = tr.ticket_id").
			Where("tr.camera_id IS NOT NULL").
			Where("tr.entry_at BETWEEN ? AND ?", rng.From, rng.To)
		cameraIDs = applyTripScope(cameraIDs, scope)
		query = query.Where("c.id IN (?)", cameraIDs)
	}

	if err := query.Scan(&rows).Error; err != nil {
		return nil, err
	}

	result := make([]model.CameraLoadMetric, 0, len(rows))
	for _, row := range rows {
		totalEvents := row.LprEvents + row.VolumeEvents
		errorRate := 0.0
		if totalEvents > 0 {
			errorRate = float64(row.ErrorEvents) / float64(totalEvents)
		}
		result = append(result, model.CameraLoadMetric{
			CameraID:     row.CameraID,
			CameraName:   row.CameraName,
			PolygonID:    row.PolygonID,
			PolygonName:  row.PolygonName,
			LprEvents:    row.LprEvents,
			VolumeEvents: row.VolumeEvents,
			ErrorEvents:  row.ErrorEvents,
			ErrorRate:    clamp(errorRate),
		})
	}

	return result, nil
}

func (r *AnalyticsRepository) ContractProgress(ctx context.Context, scope model.Scope) ([]model.ContractProgress, error) {
	if !r.tablesAvailable(ctx, "contracts", "organizations", "contract_usage") {
		return nil, nil
	}

	type row struct {
		ContractID     uuid.UUID
		Name           string
		ContractorID   uuid.UUID
		ContractorName string
		BudgetTotal    float64
		TotalCost      float64
		MinimalVolume  float64
		TotalVolume    float64
		StartAt        time.Time
		EndAt          time.Time
		UIStatus       string
		Result         string
	}
	var rows []row

	now := time.Now()

	query := r.db.WithContext(ctx).
		Table("contracts c").
		Select(`c.id AS contract_id,
			c.name,
			c.contractor_id,
			COALESCE(org.name, 'Contractor') AS contractor_name,
			c.budget_total,
			COALESCE(u.total_cost, 0) AS total_cost,
			c.minimal_volume_m3,
			COALESCE(u.total_volume_m3, 0) AS total_volume,
			c.start_at,
			c.end_at,
			c.is_active`).
		Joins("LEFT JOIN contract_usage u ON u.contract_id = c.id").
		Joins("LEFT JOIN organizations org ON org.id = c.contractor_id")

	query = applyContractScope(query, scope)

	if err := query.Scan(&rows).Error; err != nil {
		return nil, err
	}

	contracts := make([]model.ContractProgress, 0, len(rows))
	for _, row := range rows {
		status := deriveContractStatus(row.StartAt, row.EndAt, now)
		result := deriveContractResult(status, row.TotalVolume, row.MinimalVolume)
		budgetProgress := 0.0
		if row.BudgetTotal > 0 {
			budgetProgress = row.TotalCost / row.BudgetTotal
		}
		volumeProgress := 0.0
		if row.MinimalVolume > 0 {
			volumeProgress = row.TotalVolume / row.MinimalVolume
		}
		contracts = append(contracts, model.ContractProgress{
			ContractID:     row.ContractID,
			Name:           row.Name,
			ContractorID:   row.ContractorID,
			ContractorName: row.ContractorName,
			BudgetTotal:    row.BudgetTotal,
			TotalCost:      row.TotalCost,
			MinimalVolume:  row.MinimalVolume,
			TotalVolume:    row.TotalVolume,
			BudgetProgress: budgetProgress,
			VolumeProgress: volumeProgress,
			UIStatus:       status,
			Result:         result,
			StartAt:        row.StartAt,
			EndAt:          row.EndAt,
		})
	}

	return contracts, nil
}

func (r *AnalyticsRepository) MapStates(ctx context.Context, scope model.Scope, rng model.DateRange) (areas []model.MapAreaState, polygons []model.MapPolygonState, cameras []model.MapCameraState, err error) {
	if !r.tablesAvailable(ctx, "trips", "tickets") {
		return nil, nil, nil, nil
	}
	areaActivity, err := r.CleaningAreaActivity(ctx, scope, rng)
	if err != nil {
		return nil, nil, nil, err
	}
	for _, activity := range areaActivity {
		areas = append(areas, model.MapAreaState{
			ID:             activity.CleaningAreaID,
			HasTrips:       activity.Trips > 0,
			HasActiveTrips: activity.ActiveTrips > 0,
			HasViolations:  activity.HasViolations,
			Intensity:      activity.TripHeat,
		})
	}

	if r.tablesAvailable(ctx, "polygons") {
		type polygonRow struct {
			PolygonID uuid.UUID
			Name      string
			TripCount int64
			Volume    float64
		}
		var polygonRows []polygonRow

		polyQuery := r.db.WithContext(ctx).
			Table("trips tr").
			Select(`tr.polygon_id AS polygon_id,
			COALESCE(p.name, 'Polygon') AS name,
			COUNT(*) AS trip_count,
			COALESCE(SUM(tr.detected_volume_entry), 0) AS volume`).
			Joins("LEFT JOIN polygons p ON p.id = tr.polygon_id").
			Joins("LEFT JOIN tickets t ON t.id = tr.ticket_id").
			Where("tr.polygon_id IS NOT NULL AND tr.entry_at BETWEEN ? AND ?", rng.From, rng.To).
			Group("tr.polygon_id, p.name")

		polyQuery = applyTripScope(polyQuery, scope)

		if err := polyQuery.Scan(&polygonRows).Error; err != nil {
			return nil, nil, nil, err
		}
		for _, row := range polygonRows {
			polygons = append(polygons, model.MapPolygonState{
				ID:        row.PolygonID,
				Name:      row.Name,
				TripCount: row.TripCount,
				VolumeM3:  row.Volume,
			})
		}
	}

	if r.tablesAvailable(ctx, "cameras", "lpr_events", "trips") {
		type cameraRow struct {
			ID     uuid.UUID
			Name   string
			Events int64
			Errors int64
		}
		var cameraRows []cameraRow

		cameraQuery := r.db.WithContext(ctx).
			Table("cameras c").
			Select(`c.id, c.name,
			COALESCE(events.cnt, 0) AS events,
			COALESCE(errors.cnt, 0) AS errors`).
			Joins(`LEFT JOIN (
			SELECT camera_id, COUNT(*) AS cnt
			FROM lpr_events
			WHERE detected_at BETWEEN ? AND ?
			GROUP BY camera_id
		) AS events ON events.camera_id = c.id`, rng.From, rng.To).
			Joins(`LEFT JOIN (
			SELECT camera_id, COUNT(*) AS cnt
			FROM trips
			WHERE camera_id IS NOT NULL AND status <> 'OK' AND entry_at BETWEEN ? AND ?
			GROUP BY camera_id
		) AS errors ON errors.camera_id = c.id`, rng.From, rng.To)

		if scope.Type != model.ScopeCity && scope.Type != model.ScopeTechnical {
			cameraIDs := r.db.WithContext(ctx).
				Table("trips tr").
				Select("DISTINCT tr.camera_id").
				Joins("JOIN tickets t ON t.id = tr.ticket_id").
				Where("tr.camera_id IS NOT NULL").
				Where("tr.entry_at BETWEEN ? AND ?", rng.From, rng.To)
			cameraIDs = applyTripScope(cameraIDs, scope)
			cameraQuery = cameraQuery.Where("c.id IN (?)", cameraIDs)
		}

		if err := cameraQuery.Scan(&cameraRows).Error; err != nil {
			return nil, nil, nil, err
		}
		for _, row := range cameraRows {
			cameras = append(cameras, model.MapCameraState{
				ID:          row.ID,
				Name:        row.Name,
				Events:      row.Events,
				ErrorEvents: row.Errors,
			})
		}
	}

	return areas, polygons, cameras, nil
}

func (r *AnalyticsRepository) TripSeries(ctx context.Context, scope model.Scope, filter model.AnalyticsFilter) ([]model.SeriesPoint, error) {
	if !r.relationExists(ctx, "mv_trip_daily") {
		return nil, nil
	}

	group := buildDateTrunc(filter.GroupBy)
	var rows []model.SeriesPoint

	query := r.db.WithContext(ctx).
		Table("mv_trip_daily mv").
		Select(fmt.Sprintf("DATE_TRUNC('%s', mv.bucket) AS bucket, SUM(mv.total_trips) AS count", group)).
		Where("mv.bucket BETWEEN ? AND ?", filter.Range.From, filter.Range.To).
		Group("bucket").
		Order("bucket ASC")

	if filter.ContractorID != nil {
		query = query.Where("mv.contractor_id = ?", *filter.ContractorID)
	}
	if filter.DriverID != nil {
		query = query.Where("mv.driver_id = ?", *filter.DriverID)
	}

	query = applyMVTripScope(query, scope)

	if err := query.Scan(&rows).Error; err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *AnalyticsRepository) TripVolumeSeries(ctx context.Context, scope model.Scope, filter model.AnalyticsFilter) ([]model.SeriesPoint, error) {
	if !r.relationExists(ctx, "mv_trip_daily") {
		return nil, nil
	}

	group := buildDateTrunc(filter.GroupBy)
	var rows []model.SeriesPoint

	query := r.db.WithContext(ctx).
		Table("mv_trip_daily mv").
		Select(fmt.Sprintf("DATE_TRUNC('%s', mv.bucket) AS bucket, SUM(mv.total_trips) AS count, COALESCE(SUM(mv.total_volume_m3),0) AS value", group)).
		Where("mv.bucket BETWEEN ? AND ?", filter.Range.From, filter.Range.To).
		Group("bucket").
		Order("bucket ASC")

	if filter.ContractorID != nil {
		query = query.Where("mv.contractor_id = ?", *filter.ContractorID)
	}
	if filter.DriverID != nil {
		query = query.Where("mv.driver_id = ?", *filter.DriverID)
	}

	query = applyMVTripScope(query, scope)

	if err := query.Scan(&rows).Error; err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *AnalyticsRepository) TopDrivers(ctx context.Context, scope model.Scope, filter model.AnalyticsFilter, limit int) ([]model.EntityMetric, error) {
	if !r.tablesAvailable(ctx, "trips", "drivers", "tickets") {
		return nil, nil
	}

	var rows []struct {
		ID     uuid.UUID
		Name   string
		Count  int64
		Volume float64
	}

	query := r.db.WithContext(ctx).
		Table("trips tr").
		Select("tr.driver_id AS id, COALESCE(d.full_name, 'Driver') AS name, COUNT(*) AS count, COALESCE(SUM(tr.detected_volume_entry),0) AS volume").
		Joins("LEFT JOIN drivers d ON d.id = tr.driver_id").
		Joins("LEFT JOIN tickets t ON t.id = tr.ticket_id").
		Where("tr.driver_id IS NOT NULL AND tr.entry_at BETWEEN ? AND ?", filter.Range.From, filter.Range.To).
		Group("tr.driver_id, d.full_name").
		Order("count DESC").
		Limit(limit)

	query = applyTripScope(query, scope)

	if err := query.Scan(&rows).Error; err != nil {
		return nil, err
	}

	total := float64(0)
	for _, row := range rows {
		total += float64(row.Count)
	}

	result := make([]model.EntityMetric, 0, len(rows))
	for _, row := range rows {
		share := 0.0
		if total > 0 {
			share = float64(row.Count) / total
		}
		result = append(result, model.EntityMetric{
			ID:     row.ID,
			Name:   row.Name,
			Count:  row.Count,
			Volume: row.Volume,
			Share:  share,
		})
	}
	return result, nil
}

func (r *AnalyticsRepository) TopContractors(ctx context.Context, scope model.Scope, filter model.AnalyticsFilter, limit int) ([]model.EntityMetric, error) {
	if !r.tablesAvailable(ctx, "trips", "tickets", "organizations") {
		return nil, nil
	}

	var rows []struct {
		ID     uuid.UUID
		Name   string
		Count  int64
		Volume float64
	}

	query := r.db.WithContext(ctx).
		Table("trips tr").
		Select("t.contractor_id AS id, COALESCE(org.name, 'Contractor') AS name, COUNT(*) AS count, COALESCE(SUM(tr.detected_volume_entry),0) AS volume").
		Joins("LEFT JOIN tickets t ON t.id = tr.ticket_id").
		Joins("LEFT JOIN organizations org ON org.id = t.contractor_id").
		Where("t.contractor_id IS NOT NULL AND tr.entry_at BETWEEN ? AND ?", filter.Range.From, filter.Range.To).
		Group("t.contractor_id, org.name").
		Order("count DESC").
		Limit(limit)

	query = applyTripScope(query, scope)

	if err := query.Scan(&rows).Error; err != nil {
		return nil, err
	}

	total := float64(0)
	for _, row := range rows {
		total += float64(row.Count)
	}

	result := make([]model.EntityMetric, 0, len(rows))
	for _, row := range rows {
		share := 0.0
		if total > 0 {
			share = float64(row.Count) / total
		}
		result = append(result, model.EntityMetric{
			ID:     row.ID,
			Name:   row.Name,
			Count:  row.Count,
			Volume: row.Volume,
			Share:  share,
		})
	}
	return result, nil
}

func (r *AnalyticsRepository) TripDurationStats(ctx context.Context, scope model.Scope, filter model.AnalyticsFilter) (model.TripDurationStats, error) {
	if !r.tablesAvailable(ctx, "trips", "tickets") {
		return model.TripDurationStats{}, nil
	}

	var stats model.TripDurationStats

	query := r.db.WithContext(ctx).
		Table("trips tr").
		Select(`
			COALESCE(AVG(EXTRACT(EPOCH FROM (COALESCE(tr.exit_at, tr.entry_at) - tr.entry_at)) / 60), 0) AS avg_minutes,
			COALESCE(percentile_disc(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (COALESCE(tr.exit_at, tr.entry_at) - tr.entry_at)) / 60), 0) AS p95_minutes`).
		Joins("LEFT JOIN tickets t ON t.id = tr.ticket_id").
		Where("tr.entry_at BETWEEN ? AND ?", filter.Range.From, filter.Range.To)

	query = applyTripScope(query, scope)

	if err := query.Scan(&stats).Error; err != nil {
		return model.TripDurationStats{}, err
	}

	stats.AvgMinutes = clamp(stats.AvgMinutes)
	stats.P95Minutes = clamp(stats.P95Minutes)
	return stats, nil
}

func (r *AnalyticsRepository) TripVolumeStats(ctx context.Context, scope model.Scope, filter model.AnalyticsFilter) (model.TripVolumeStats, error) {
	if !r.tablesAvailable(ctx, "trips", "tickets") {
		return model.TripVolumeStats{}, nil
	}

	var stats model.TripVolumeStats

	query := r.db.WithContext(ctx).
		Table("trips tr").
		Select(`
			COALESCE(AVG(tr.detected_volume_entry), 0) AS avg_volume,
			COALESCE(MAX(tr.detected_volume_entry), 0) AS max_volume,
			COALESCE(MIN(tr.detected_volume_entry), 0) AS min_volume`).
		Joins("LEFT JOIN tickets t ON t.id = tr.ticket_id").
		Where("tr.entry_at BETWEEN ? AND ?", filter.Range.From, filter.Range.To)

	query = applyTripScope(query, scope)

	if err := query.Scan(&stats).Error; err != nil {
		return model.TripVolumeStats{}, err
	}
	return stats, nil
}

func (r *AnalyticsRepository) TripDetails(ctx context.Context, scope model.Scope, tripID uuid.UUID) (*model.TripDetails, error) {
	if !r.tablesAvailable(ctx, "trips", "tickets") {
		return nil, gorm.ErrRecordNotFound
	}

	type row struct {
		TripID              uuid.UUID
		TicketID            *uuid.UUID
		TicketName          *string
		DriverID            *uuid.UUID
		DriverName          *string
		VehicleID           *uuid.UUID
		VehiclePlate        *string
		ContractorID        *uuid.UUID
		ContractorName      *string
		CleaningAreaID      *uuid.UUID
		PolygonID           *uuid.UUID
		Status              string
		EntryAt             time.Time
		ExitAt              *time.Time
		DetectedVolumeEntry *float64
		DetectedVolumeExit  *float64
		EntryLprID          *uuid.UUID
		ExitLprID           *uuid.UUID
		EntryVolID          *uuid.UUID
		ExitVolID           *uuid.UUID
	}
	var details row

	query := r.db.WithContext(ctx).
		Table("trips tr").
		Select(`tr.id AS trip_id,
			tr.ticket_id,
			t.name AS ticket_name,
			tr.driver_id,
			d.full_name AS driver_name,
			tr.vehicle_id,
			v.plate_number AS vehicle_plate,
			t.contractor_id,
			org.name AS contractor_name,
			t.cleaning_area_id,
			tr.polygon_id,
			tr.status,
			tr.entry_at,
			tr.exit_at,
			tr.detected_volume_entry,
			tr.detected_volume_exit,
			tr.entry_lpr_event_id,
			tr.exit_lpr_event_id,
			tr.entry_volume_event_id,
			tr.exit_volume_event_id`).
		Joins("LEFT JOIN tickets t ON t.id = tr.ticket_id").
		Joins("LEFT JOIN drivers d ON d.id = tr.driver_id").
		Joins("LEFT JOIN vehicles v ON v.id = tr.vehicle_id").
		Joins("LEFT JOIN organizations org ON org.id = t.contractor_id").
		Where("tr.id = ?", tripID).
		Limit(1)

	query = applyTripScope(query, scope)

	if err := query.Take(&details).Error; err != nil {
		return nil, err
	}

	result := &model.TripDetails{
		TripID:              details.TripID,
		TicketID:            details.TicketID,
		TicketName:          details.TicketName,
		DriverID:            details.DriverID,
		DriverName:          details.DriverName,
		VehicleID:           details.VehicleID,
		VehiclePlate:        details.VehiclePlate,
		ContractorID:        details.ContractorID,
		ContractorName:      details.ContractorName,
		CleaningAreaID:      details.CleaningAreaID,
		PolygonID:           details.PolygonID,
		Status:              details.Status,
		EntryAt:             details.EntryAt,
		ExitAt:              details.ExitAt,
		DetectedVolumeEntry: details.DetectedVolumeEntry,
		DetectedVolumeExit:  details.DetectedVolumeExit,
	}

	result.Events = r.resolveTripEvents(ctx, details.EntryLprID, details.ExitLprID, details.EntryVolID, details.ExitVolID)

	return result, nil
}

func (r *AnalyticsRepository) resolveTripEvents(ctx context.Context, entryLpr, exitLpr, entryVol, exitVol *uuid.UUID) model.TripEventDetails {
	fetch := func(table string, eventID *uuid.UUID) *model.TripEvent {
		if eventID == nil {
			return nil
		}
		var row struct {
			ID       uuid.UUID
			CameraID uuid.UUID
			PhotoURL *string
			Captured time.Time
		}
		if err := r.db.WithContext(ctx).
			Table(table).
			Select("id, camera_id, photo_url, detected_at AS captured").
			Where("id = ?", *eventID).
			Scan(&row).Error; err != nil {
			return nil
		}
		return &model.TripEvent{
			EventID:  row.ID,
			CameraID: row.CameraID,
			PhotoURL: row.PhotoURL,
			Captured: row.Captured,
		}
	}

	return model.TripEventDetails{
		EntryLPR:    fetch("lpr_events", entryLpr),
		ExitLPR:     fetch("lpr_events", exitLpr),
		EntryVolume: fetch("volume_events", entryVol),
		ExitVolume:  fetch("volume_events", exitVol),
	}
}

func (r *AnalyticsRepository) ViolationSeries(ctx context.Context, scope model.Scope, filter model.AnalyticsFilter) ([]model.SeriesPoint, error) {
	if !r.relationExists(ctx, "mv_violation_daily") {
		return nil, nil
	}

	group := buildDateTrunc(filter.GroupBy)
	var rows []model.SeriesPoint

	query := r.db.WithContext(ctx).
		Table("mv_violation_daily mv").
		Select(fmt.Sprintf("DATE_TRUNC('%s', mv.bucket) AS bucket, SUM(mv.violation_count) AS count", group)).
		Where("mv.bucket BETWEEN ? AND ?", filter.Range.From, filter.Range.To).
		Group("bucket").
		Order("bucket ASC")

	query = applyMVCleaningAreaScope(query, scope)
	if err := query.Scan(&rows).Error; err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *AnalyticsRepository) ViolationBreakdown(ctx context.Context, scope model.Scope, filter model.AnalyticsFilter) ([]model.ViolationBreakdown, error) {
	if !r.relationExists(ctx, "mv_violation_daily") {
		return nil, nil
	}

	var rows []struct {
		Type  string
		Count int64
	}

	query := r.db.WithContext(ctx).
		Table("mv_violation_daily mv").
		Select("mv.violation_type AS type, SUM(mv.violation_count) AS count").
		Where("mv.bucket BETWEEN ? AND ?", filter.Range.From, filter.Range.To).
		Group("mv.violation_type").
		Order("count DESC")

	query = applyMVCleaningAreaScope(query, scope)
	if err := query.Scan(&rows).Error; err != nil {
		return nil, err
	}

	total := float64(0)
	for _, row := range rows {
		total += float64(row.Count)
	}

	result := make([]model.ViolationBreakdown, 0, len(rows))
	for _, row := range rows {
		share := 0.0
		if total > 0 {
			share = float64(row.Count) / total
		}
		result = append(result, model.ViolationBreakdown{
			Type:  row.Type,
			Count: row.Count,
			Share: share,
		})
	}
	return result, nil
}

func (r *AnalyticsRepository) ViolationLeaders(ctx context.Context, scope model.Scope, filter model.AnalyticsFilter, column string, limit int) ([]model.EntityMetric, error) {
	if !r.tablesAvailable(ctx, "trips", "tickets") {
		return nil, nil
	}

	var rows []struct {
		ID    uuid.UUID
		Name  string
		Count int64
	}
	var nameExpr string
	switch column {
	case "t.contractor_id":
		nameExpr = "COALESCE(org.name, 'Contractor')"
	case "tr.driver_id":
		nameExpr = "COALESCE(d.full_name, 'Driver')"
	case "tr.camera_id":
		nameExpr = "COALESCE(c.name, 'Camera')"
	default:
		nameExpr = "'Unknown'"
	}

	query := r.db.WithContext(ctx).
		Table("trips tr").
		Select(fmt.Sprintf("%s AS id, %s AS name, COUNT(*) AS count", column, nameExpr)).
		Joins("LEFT JOIN tickets t ON t.id = tr.ticket_id").
		Where("tr.status <> 'OK' AND tr.entry_at BETWEEN ? AND ?", filter.Range.From, filter.Range.To).
		Group(column).
		Group(nameExpr).
		Order("count DESC").
		Limit(limit)

	if strings.Contains(column, "contractor") {
		query = query.Joins("LEFT JOIN organizations org ON org.id = t.contractor_id")
	}
	if strings.Contains(column, "driver") {
		query = query.Joins("LEFT JOIN drivers d ON d.id = tr.driver_id")
	}
	if strings.Contains(column, "camera") {
		query = query.Joins("LEFT JOIN cameras c ON c.id = tr.camera_id")
	}

	query = applyTripScope(query, scope)

	if err := query.Scan(&rows).Error; err != nil {
		return nil, err
	}

	total := float64(0)
	for _, row := range rows {
		total += float64(row.Count)
	}

	result := make([]model.EntityMetric, 0, len(rows))
	for _, row := range rows {
		share := 0.0
		if total > 0 {
			share = float64(row.Count) / total
		}
		result = append(result, model.EntityMetric{
			ID:    row.ID,
			Name:  row.Name,
			Count: row.Count,
			Share: share,
		})
	}
	return result, nil
}

func (r *AnalyticsRepository) ContractorPerformance(ctx context.Context, scope model.Scope, filter model.AnalyticsFilter, limit int) ([]model.ContractorPerformance, error) {
	if !r.tablesAvailable(ctx, "trips", "tickets", "organizations") {
		return nil, nil
	}

	var rows []struct {
		ID            uuid.UUID
		Name          string
		TripCount     int64
		AvgVolume     float64
		ViolationRate float64
		Drivers       int64
	}

	query := r.db.WithContext(ctx).
		Table("trips tr").
		Select(`
			t.contractor_id AS id,
			COALESCE(org.name, 'Contractor') AS name,
			COUNT(*) AS trip_count,
			COALESCE(AVG(tr.detected_volume_entry),0) AS avg_volume,
			COALESCE(SUM(CASE WHEN tr.status <> 'OK' THEN 1 ELSE 0 END)::float / NULLIF(COUNT(*),0), 0) AS violation_rate,
			COUNT(DISTINCT tr.driver_id) AS drivers`).
		Joins("LEFT JOIN tickets t ON t.id = tr.ticket_id").
		Joins("LEFT JOIN organizations org ON org.id = t.contractor_id").
		Where("t.contractor_id IS NOT NULL AND tr.entry_at BETWEEN ? AND ?", filter.Range.From, filter.Range.To).
		Group("t.contractor_id, org.name").
		Order("trip_count DESC").
		Limit(limit)

	query = applyTripScope(query, scope)

	if err := query.Scan(&rows).Error; err != nil {
		return nil, err
	}

	result := make([]model.ContractorPerformance, 0, len(rows))
	for _, row := range rows {
		result = append(result, model.ContractorPerformance{
			ContractorID:   row.ID,
			ContractorName: row.Name,
			TripCount:      row.TripCount,
			AvgVolume:      row.AvgVolume,
			ViolationRate:  clamp(row.ViolationRate),
			ActiveDrivers:  row.Drivers,
			Utilization:    clamp(float64(row.TripCount) / math.Max(float64(limit), 1)),
		})
	}
	return result, nil
}

func (r *AnalyticsRepository) DriverPerformance(ctx context.Context, scope model.Scope, filter model.AnalyticsFilter, limit int) ([]model.DriverPerformance, error) {
	if !r.tablesAvailable(ctx, "trips", "tickets", "drivers") {
		return nil, nil
	}

	var rows []struct {
		ID            uuid.UUID
		Name          string
		TripCount     int64
		AvgVolume     float64
		ViolationRate float64
		AvgDuration   float64
	}

	query := r.db.WithContext(ctx).
		Table("trips tr").
		Select(`
			tr.driver_id AS id,
			COALESCE(d.full_name, 'Driver') AS name,
			COUNT(*) AS trip_count,
			COALESCE(AVG(tr.detected_volume_entry),0) AS avg_volume,
			COALESCE(SUM(CASE WHEN tr.status <> 'OK' THEN 1 ELSE 0 END)::float / NULLIF(COUNT(*),0), 0) AS violation_rate,
			COALESCE(AVG(EXTRACT(EPOCH FROM (COALESCE(tr.exit_at, tr.entry_at) - tr.entry_at)) / 60),0) AS avg_duration`).
		Joins("LEFT JOIN tickets t ON t.id = tr.ticket_id").
		Joins("LEFT JOIN drivers d ON d.id = tr.driver_id").
		Where("tr.driver_id IS NOT NULL AND tr.entry_at BETWEEN ? AND ?", filter.Range.From, filter.Range.To).
		Group("tr.driver_id, d.full_name").
		Order("trip_count DESC").
		Limit(limit)

	query = applyTripScope(query, scope)

	if err := query.Scan(&rows).Error; err != nil {
		return nil, err
	}

	result := make([]model.DriverPerformance, 0, len(rows))
	for _, row := range rows {
		result = append(result, model.DriverPerformance{
			DriverID:      row.ID,
			DriverName:    row.Name,
			TripCount:     row.TripCount,
			AvgVolume:     row.AvgVolume,
			ViolationRate: clamp(row.ViolationRate),
			AvgDuration:   clamp(row.AvgDuration),
		})
	}
	return result, nil
}

func (r *AnalyticsRepository) DriverKPIs(ctx context.Context, scope model.Scope, filter model.AnalyticsFilter) ([]model.DriverKPI, error) {
	if !r.tablesAvailable(ctx, "trips", "drivers", "tickets", "organizations") {
		return nil, nil
	}

	type row struct {
		ID             uuid.UUID
		Name           string
		ContractorID   *uuid.UUID
		ContractorName *string
		TripCount      int64
		AvgVolume      float64
		ViolationRate  float64
		AvgDuration    float64
		LastTrip       *time.Time
	}
	var rows []row

	query := r.db.WithContext(ctx).
		Table("trips tr").
		Select(`tr.driver_id AS id,
			COALESCE(d.full_name, 'Driver') AS name,
			t.contractor_id,
			org.name AS contractor_name,
			COUNT(*) AS trip_count,
			COALESCE(AVG(tr.detected_volume_entry),0) AS avg_volume,
			COALESCE(SUM(CASE WHEN tr.status <> 'OK' THEN 1 ELSE 0 END)::float / NULLIF(COUNT(*),0), 0) AS violation_rate,
			COALESCE(AVG(EXTRACT(EPOCH FROM (COALESCE(tr.exit_at, tr.entry_at) - tr.entry_at)) / 60),0) AS avg_duration,
			MAX(tr.entry_at) AS last_trip`).
		Joins("LEFT JOIN drivers d ON d.id = tr.driver_id").
		Joins("LEFT JOIN tickets t ON t.id = tr.ticket_id").
		Joins("LEFT JOIN organizations org ON org.id = t.contractor_id").
		Where("tr.driver_id IS NOT NULL AND tr.entry_at BETWEEN ? AND ?", filter.Range.From, filter.Range.To).
		Group("tr.driver_id, d.full_name, t.contractor_id, org.name")

	if filter.ContractorID != nil {
		query = query.Where("t.contractor_id = ?", *filter.ContractorID)
	}
	if filter.DriverID != nil {
		query = query.Where("tr.driver_id = ?", *filter.DriverID)
	}

	query = applyTripScope(query, scope)

	if err := query.Scan(&rows).Error; err != nil {
		return nil, err
	}

	result := make([]model.DriverKPI, 0, len(rows))
	for _, row := range rows {
		result = append(result, model.DriverKPI{
			DriverID:       row.ID,
			DriverName:     row.Name,
			ContractorID:   row.ContractorID,
			ContractorName: row.ContractorName,
			TripCount:      row.TripCount,
			AvgVolume:      row.AvgVolume,
			ViolationRate:  clamp(row.ViolationRate),
			AvgDuration:    clamp(row.AvgDuration),
			LastTripAt:     row.LastTrip,
		})
	}

	return result, nil
}

func (r *AnalyticsRepository) VehiclePerformance(ctx context.Context, scope model.Scope, filter model.AnalyticsFilter, limit int) ([]model.VehiclePerformance, error) {
	if !r.tablesAvailable(ctx, "trips", "tickets", "vehicles") {
		return nil, nil
	}

	var rows []struct {
		ID            uuid.UUID
		PlateNumber   string
		TripCount     int64
		AvgFillRate   float64
		ViolationRate float64
	}

	query := r.db.WithContext(ctx).
		Table("trips tr").
		Select(`
			tr.vehicle_id AS id,
			COALESCE(v.plate_number, 'Vehicle') AS plate_number,
			COUNT(*) AS trip_count,
			COALESCE(AVG(CASE WHEN v.body_volume_m3 > 0 THEN tr.detected_volume_entry / v.body_volume_m3 END),0) AS avg_fill_rate,
			COALESCE(SUM(CASE WHEN tr.status <> 'OK' THEN 1 ELSE 0 END)::float / NULLIF(COUNT(*),0), 0) AS violation_rate`).
		Joins("LEFT JOIN vehicles v ON v.id = tr.vehicle_id").
		Joins("LEFT JOIN tickets t ON t.id = tr.ticket_id").
		Where("tr.vehicle_id IS NOT NULL AND tr.entry_at BETWEEN ? AND ?", filter.Range.From, filter.Range.To).
		Group("tr.vehicle_id, v.plate_number, v.body_volume_m3").
		Order("trip_count DESC").
		Limit(limit)

	query = applyTripScope(query, scope)

	if err := query.Scan(&rows).Error; err != nil {
		return nil, err
	}

	rangeHours := filter.Range.To.Sub(filter.Range.From).Hours()
	result := make([]model.VehiclePerformance, 0, len(rows))
	for _, row := range rows {
		idle := math.Max(rangeHours-(float64(row.TripCount)*1.5), 0)
		result = append(result, model.VehiclePerformance{
			VehicleID:     row.ID,
			PlateNumber:   row.PlateNumber,
			TripCount:     row.TripCount,
			AvgFillRate:   clamp(row.AvgFillRate),
			ViolationRate: clamp(row.ViolationRate),
			IdleHours:     idle,
		})
	}
	return result, nil
}

func (r *AnalyticsRepository) VehicleKPIs(ctx context.Context, scope model.Scope, filter model.AnalyticsFilter) ([]model.VehicleKPI, error) {
	if !r.tablesAvailable(ctx, "trips", "vehicles", "tickets", "organizations") {
		return nil, nil
	}

	type row struct {
		ID             uuid.UUID
		PlateNumber    string
		ContractorID   *uuid.UUID
		ContractorName *string
		TripCount      int64
		AvgFillRate    float64
		ViolationRate  float64
		LastTrip       *time.Time
	}
	var rows []row

	query := r.db.WithContext(ctx).
		Table("trips tr").
		Select(`tr.vehicle_id AS id,
			COALESCE(v.plate_number, 'Vehicle') AS plate_number,
			t.contractor_id,
			org.name AS contractor_name,
			COUNT(*) AS trip_count,
			COALESCE(AVG(CASE WHEN v.body_volume_m3 > 0 THEN tr.detected_volume_entry / v.body_volume_m3 END),0) AS avg_fill_rate,
			COALESCE(SUM(CASE WHEN tr.status <> 'OK' THEN 1 ELSE 0 END)::float / NULLIF(COUNT(*),0), 0) AS violation_rate,
			MAX(tr.entry_at) AS last_trip`).
		Joins("LEFT JOIN vehicles v ON v.id = tr.vehicle_id").
		Joins("LEFT JOIN tickets t ON t.id = tr.ticket_id").
		Joins("LEFT JOIN organizations org ON org.id = t.contractor_id").
		Where("tr.vehicle_id IS NOT NULL AND tr.entry_at BETWEEN ? AND ?", filter.Range.From, filter.Range.To).
		Group("tr.vehicle_id, v.plate_number, t.contractor_id, org.name, v.body_volume_m3")

	if filter.ContractorID != nil {
		query = query.Where("t.contractor_id = ?", *filter.ContractorID)
	}

	query = applyTripScope(query, scope)

	if err := query.Scan(&rows).Error; err != nil {
		return nil, err
	}

	rangeHours := filter.Range.To.Sub(filter.Range.From).Hours()
	result := make([]model.VehicleKPI, 0, len(rows))
	for _, row := range rows {
		idle := 0.0
		if row.LastTrip != nil {
			delta := filter.Range.To.Sub(*row.LastTrip).Hours()
			if delta > 0 {
				idle = delta
			}
		} else if rangeHours > 0 {
			idle = rangeHours
		}
		result = append(result, model.VehicleKPI{
			VehicleID:      row.ID,
			PlateNumber:    row.PlateNumber,
			ContractorID:   row.ContractorID,
			ContractorName: row.ContractorName,
			TripCount:      row.TripCount,
			AvgFillRate:    clamp(row.AvgFillRate),
			ViolationRate:  clamp(row.ViolationRate),
			IdleHours:      clamp(idle),
			LastTripAt:     row.LastTrip,
		})
	}

	return result, nil
}

func (r *AnalyticsRepository) TechnicalAnalytics(ctx context.Context, scope model.Scope, rng model.DateRange) (model.TechnicalAnalytics, error) {
	if !r.tablesAvailable(ctx, "cameras") {
		return model.TechnicalAnalytics{}, nil
	}

	cameras, err := r.CameraLoad(ctx, scope, rng)
	if err != nil {
		return model.TechnicalAnalytics{}, err
	}

	type polygonRow struct {
		ID        uuid.UUID
		Name      string
		TripCount int64
		Volume    float64
		Errors    int64
	}
	var polygonRows []polygonRow

	if r.tablesAvailable(ctx, "polygons", "trips") {
		polyQuery := r.db.WithContext(ctx).
			Table("polygons p").
			Select(`p.id, p.name,
			COALESCE(trip_data.trip_count, 0) AS trip_count,
			COALESCE(trip_data.volume_m3, 0) AS volume,
			COALESCE(trip_data.errors, 0) AS errors`).
			Joins(`LEFT JOIN (
			SELECT tr.polygon_id,
				COUNT(*) AS trip_count,
				COALESCE(SUM(tr.detected_volume_entry), 0) AS volume_m3,
				SUM(CASE WHEN tr.status <> 'OK' THEN 1 ELSE 0 END) AS errors
			FROM trips tr
			WHERE tr.polygon_id IS NOT NULL AND tr.entry_at BETWEEN ? AND ?
			GROUP BY tr.polygon_id
		) AS trip_data ON trip_data.polygon_id = p.id`, rng.From, rng.To)

		if err := polyQuery.Scan(&polygonRows).Error; err != nil {
			return model.TechnicalAnalytics{}, err
		}
	}

	polygons := make([]model.PolygonLoadMetric, 0, len(polygonRows))
	for _, row := range polygonRows {
		polygons = append(polygons, model.PolygonLoadMetric{
			PolygonID:   row.ID,
			PolygonName: row.Name,
			TripCount:   row.TripCount,
			VolumeM3:    row.Volume,
			ErrorEvents: row.Errors,
		})
	}

	totalEvents := int64(0)
	errorEvents := int64(0)
	for _, cam := range cameras {
		totalEvents += cam.LprEvents + cam.VolumeEvents
		errorEvents += cam.ErrorEvents
	}

	var lastEventValue sql.NullTime
	var lastEvent *time.Time
	if r.tablesAvailable(ctx, "lpr_events", "volume_events") {
		eventQuery := r.db.WithContext(ctx).
			Raw(`
			SELECT MAX(ts) AS last_event
			FROM (
				SELECT detected_at AS ts FROM lpr_events WHERE detected_at BETWEEN ? AND ?
				UNION ALL
				SELECT detected_at AS ts FROM volume_events WHERE detected_at BETWEEN ? AND ?
			) AS union_events
		`, rng.From, rng.To, rng.From, rng.To)
		if err := eventQuery.Scan(&lastEventValue).Error; err != nil {
			return model.TechnicalAnalytics{}, err
		}
		if lastEventValue.Valid {
			t := lastEventValue.Time
			lastEvent = &t
		}
	}

	errorRate := 0.0
	if totalEvents > 0 {
		errorRate = float64(errorEvents) / float64(totalEvents)
	}

	eventFrequency := 0.0
	durationHours := rng.To.Sub(rng.From).Hours()
	if durationHours > 0 {
		eventFrequency = float64(totalEvents) / durationHours
	}

	return model.TechnicalAnalytics{
		Cameras:        cameras,
		Polygons:       polygons,
		ErrorRate:      clamp(errorRate),
		LastEventAt:    lastEvent,
		TotalEvents:    totalEvents,
		EventFrequency: clamp(eventFrequency),
	}, nil
}

func deriveContractStatus(start, end time.Time, now time.Time) string {
	if now.Before(start) {
		return "PLANNED"
	}
	if now.After(end) {
		return "EXPIRED"
	}
	return "ACTIVE"
}

func deriveContractResult(status string, totalVolume, minimalVolume float64) string {
	if status != "EXPIRED" {
		return "NONE"
	}
	if minimalVolume == 0 {
		return "NONE"
	}
	if totalVolume >= minimalVolume {
		return "SUCCESS"
	}
	return "FAIL"
}

func applyTripScope(query *gorm.DB, scope model.Scope) *gorm.DB {
	switch scope.Type {
	case model.ScopeCity:
		return query
	case model.ScopeKgu:
		if scope.OrgID != nil {
			if len(scope.ContractorIDs) > 0 {
				return query.Where("(t.created_by_org_id = ? OR t.contractor_id IN ?)", *scope.OrgID, scope.ContractorIDs)
			}
			return query.Where("t.created_by_org_id = ?", *scope.OrgID)
		}
	case model.ScopeContractor:
		if scope.OrgID != nil {
			return query.Where("t.contractor_id = ?", *scope.OrgID)
		}
	case model.ScopeTechnical:
		return query.Where("1 = 0")
	}
	return query
}

func applyTicketScope(query *gorm.DB, scope model.Scope) *gorm.DB {
	switch scope.Type {
	case model.ScopeCity:
		return query
	case model.ScopeKgu:
		if scope.OrgID != nil {
			return query.Where("t.created_by_org_id = ?", *scope.OrgID)
		}
	case model.ScopeContractor:
		if scope.OrgID != nil {
			return query.Where("t.contractor_id = ?", *scope.OrgID)
		}
	case model.ScopeTechnical:
		return query.Where("1 = 0")
	}
	return query
}

func applyContractScope(query *gorm.DB, scope model.Scope) *gorm.DB {
	switch scope.Type {
	case model.ScopeCity:
		return query
	case model.ScopeKgu:
		if scope.OrgID != nil {
			return query.Where("c.created_by_org = ?", *scope.OrgID)
		}
	case model.ScopeContractor:
		if scope.OrgID != nil {
			return query.Where("c.contractor_id = ?", *scope.OrgID)
		}
	default:
		return query.Where("1 = 0")
	}
	return query
}

func clamp(value float64) float64 {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return 0
	}
	return value
}

func (r *AnalyticsRepository) relationExists(ctx context.Context, name string) bool {
	var exists bool
	err := r.db.WithContext(ctx).
		Raw(`SELECT EXISTS (
			SELECT 1
			FROM pg_catalog.pg_class c
			JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
			WHERE c.relname = ? AND c.relkind IN ('r','m','v') AND n.nspname = 'public'
		)`, name).
		Scan(&exists).Error
	if err != nil {
		return false
	}
	return exists
}

func (r *AnalyticsRepository) tablesAvailable(ctx context.Context, names ...string) bool {
	for _, name := range names {
		if !r.relationExists(ctx, name) {
			return false
		}
	}
	return true
}

func applyMVTripScope(query *gorm.DB, scope model.Scope) *gorm.DB {
	switch scope.Type {
	case model.ScopeCity:
		return query
	case model.ScopeKgu:
		if scope.OrgID != nil {
			if len(scope.ContractorIDs) > 0 {
				return query.Where("(mv.created_by_org_id = ? OR mv.contractor_id IN ?)", *scope.OrgID, scope.ContractorIDs)
			}
			return query.Where("mv.created_by_org_id = ?", *scope.OrgID)
		}
	case model.ScopeContractor:
		if scope.OrgID != nil {
			return query.Where("mv.contractor_id = ?", *scope.OrgID)
		}
	default:
		return query.Where("1 = 0")
	}
	return query
}

func applyMVCleaningAreaScope(query *gorm.DB, scope model.Scope) *gorm.DB {
	switch scope.Type {
	case model.ScopeCity:
		return query
	case model.ScopeKgu:
		if scope.OrgID != nil {
			if len(scope.ContractorIDs) > 0 {
				return query.Where("(mv.created_by_org_id = ? OR mv.contractor_id IN ?)", *scope.OrgID, scope.ContractorIDs)
			}
			return query.Where("mv.created_by_org_id = ?", *scope.OrgID)
		}
	case model.ScopeContractor:
		if scope.OrgID != nil {
			return query.Where("mv.contractor_id = ?", *scope.OrgID)
		}
	default:
		return query.Where("1 = 0")
	}
	return query
}

func normalizeGroupBy(groupBy model.GroupBy) string {
	switch groupBy {
	case model.GroupByWeek:
		return "week"
	case model.GroupByMonth:
		return "month"
	default:
		return "day"
	}
}

func buildDateTrunc(groupBy model.GroupBy) string {
	switch groupBy {
	case model.GroupByWeek:
		return "week"
	case model.GroupByMonth:
		return "month"
	default:
		return "day"
	}
}

func placeholderList(ids []uuid.UUID) string {
	builder := strings.Builder{}
	for i := range ids {
		if i > 0 {
			builder.WriteString(",")
		}
		builder.WriteString("?")
	}
	return builder.String()
}
