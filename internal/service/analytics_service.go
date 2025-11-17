package service

import (
	"context"
	"errors"
	"sort"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"analytics-service/internal/model"
	"analytics-service/internal/repository"
)

var (
	ErrPermissionDenied = errors.New("permission denied")
	ErrNotFound         = errors.New("not found")
)

type AnalyticsService struct {
	scopes       *repository.ScopeRepository
	analytics    *repository.AnalyticsRepository
	defaultRange int
	maxRange     int
}

func NewAnalyticsService(scopes *repository.ScopeRepository, analytics *repository.AnalyticsRepository, defaultRange, maxRange int) *AnalyticsService {
	return &AnalyticsService{
		scopes:       scopes,
		analytics:    analytics,
		defaultRange: defaultRange,
		maxRange:     maxRange,
	}
}

func (s *AnalyticsService) GetDashboard(ctx context.Context, principal model.Principal, rng model.DateRange) (*model.DashboardMetrics, error) {
	if principal.IsDriver() {
		return nil, ErrPermissionDenied
	}

	scope, err := s.scopes.ResolveScope(ctx, principal)
	if err != nil {
		if errors.Is(err, repository.ErrScopeUnsupported) {
			return nil, ErrPermissionDenied
		}
		return nil, err
	}

	rangeNormalized := s.normalizeRange(rng)

	metrics := &model.DashboardMetrics{GeneratedFor: rangeNormalized}

	if scope.Type != model.ScopeTechnical {
		stats, err := s.analytics.DashboardStats(ctx, scope, rangeNormalized)
		if err != nil {
			return nil, err
		}
		areas, err := s.analytics.CleaningAreaActivity(ctx, scope, rangeNormalized)
		if err != nil {
			return nil, err
		}
		active, idle, err := s.analytics.ContractorActivitySplit(ctx, scope, rangeNormalized)
		if err != nil {
			return nil, err
		}
		contracts, err := s.analytics.ContractProgress(ctx, scope)
		if err != nil {
			return nil, err
		}
		mapAreas, mapPolygons, mapCameras, err := s.analytics.MapStates(ctx, scope, rangeNormalized)
		if err != nil {
			return nil, err
		}

		metrics.Stats = stats
		metrics.Areas = areas
		metrics.Contractors = model.DashboardContractors{Active: active, Idle: idle}
		metrics.Contracts = contracts
		metrics.Map = model.MapSummary{Areas: mapAreas, Polygons: mapPolygons, Cameras: mapCameras}
	}

	cameraLoad, err := s.analytics.CameraLoad(ctx, scope, rangeNormalized)
	if err != nil {
		return nil, err
	}
	metrics.Cameras = cameraLoad

	return metrics, nil
}

func (s *AnalyticsService) GetTripAnalytics(ctx context.Context, principal model.Principal, filter model.AnalyticsFilter) (*model.TripAnalytics, error) {
	if principal.IsDriver() {
		return nil, ErrPermissionDenied
	}

	scope, err := s.scopes.ResolveScope(ctx, principal)
	if err != nil || scope.Type == model.ScopeTechnical {
		return nil, ErrPermissionDenied
	}

	normalized := s.normalizeFilter(filter)

	series, err := s.analytics.TripSeries(ctx, scope, normalized)
	if err != nil {
		return nil, err
	}
	volumeSeries, err := s.analytics.TripVolumeSeries(ctx, scope, normalized)
	if err != nil {
		return nil, err
	}
	topDrivers, err := s.analytics.TopDrivers(ctx, scope, normalized, 5)
	if err != nil {
		return nil, err
	}
	topContractors, err := s.analytics.TopContractors(ctx, scope, normalized, 5)
	if err != nil {
		return nil, err
	}
	durationStats, err := s.analytics.TripDurationStats(ctx, scope, normalized)
	if err != nil {
		return nil, err
	}
	volumeStats, err := s.analytics.TripVolumeStats(ctx, scope, normalized)
	if err != nil {
		return nil, err
	}

	return &model.TripAnalytics{
		Series:         series,
		VolumeSeries:   volumeSeries,
		TopDrivers:     topDrivers,
		TopContractors: topContractors,
		DurationStats:  durationStats,
		VolumeStats:    volumeStats,
	}, nil
}

func (s *AnalyticsService) GetTripDetails(ctx context.Context, principal model.Principal, tripID uuid.UUID) (*model.TripDetails, error) {
	if principal.IsDriver() {
		return nil, ErrPermissionDenied
	}

	scope, err := s.scopes.ResolveScope(ctx, principal)
	if err != nil || scope.Type == model.ScopeTechnical {
		return nil, ErrPermissionDenied
	}

	details, err := s.analytics.TripDetails(ctx, scope, tripID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrNotFound
		}
		return nil, err
	}

	return details, nil
}

func (s *AnalyticsService) GetViolationAnalytics(ctx context.Context, principal model.Principal, filter model.AnalyticsFilter) (*model.ViolationAnalytics, error) {
	if principal.IsDriver() {
		return nil, ErrPermissionDenied
	}

	scope, err := s.scopes.ResolveScope(ctx, principal)
	if err != nil || scope.Type == model.ScopeTechnical {
		return nil, ErrPermissionDenied
	}

	normalized := s.normalizeFilter(filter)

	series, err := s.analytics.ViolationSeries(ctx, scope, normalized)
	if err != nil {
		return nil, err
	}
	breakdown, err := s.analytics.ViolationBreakdown(ctx, scope, normalized)
	if err != nil {
		return nil, err
	}
	topContractors, err := s.analytics.ViolationLeaders(ctx, scope, normalized, "t.contractor_id", 5)
	if err != nil {
		return nil, err
	}
	topDrivers, err := s.analytics.ViolationLeaders(ctx, scope, normalized, "tr.driver_id", 5)
	if err != nil {
		return nil, err
	}
	topCameras, err := s.analytics.ViolationLeaders(ctx, scope, normalized, "tr.camera_id", 5)
	if err != nil {
		return nil, err
	}

	return &model.ViolationAnalytics{
		Series:         series,
		Breakdown:      breakdown,
		TopContractors: topContractors,
		TopDrivers:     topDrivers,
		TopCameras:     convertCameraLeaders(topCameras),
	}, nil
}

func (s *AnalyticsService) GetPerformanceAnalytics(ctx context.Context, principal model.Principal, filter model.AnalyticsFilter) (*model.PerformanceAnalytics, error) {
	if principal.IsDriver() {
		return nil, ErrPermissionDenied
	}

	scope, err := s.scopes.ResolveScope(ctx, principal)
	if err != nil || scope.Type == model.ScopeTechnical {
		return nil, ErrPermissionDenied
	}

	normalized := s.normalizeFilter(filter)

	contractors, err := s.analytics.ContractorPerformance(ctx, scope, normalized, 10)
	if err != nil {
		return nil, err
	}
	drivers, err := s.analytics.DriverPerformance(ctx, scope, normalized, 10)
	if err != nil {
		return nil, err
	}
	vehicles, err := s.analytics.VehiclePerformance(ctx, scope, normalized, 10)
	if err != nil {
		return nil, err
	}

	return &model.PerformanceAnalytics{
		Contractors: contractors,
		Drivers:     drivers,
		Vehicles:    vehicles,
	}, nil
}

func (s *AnalyticsService) GetContractAnalytics(ctx context.Context, principal model.Principal) (*model.ContractAnalytics, error) {
	if principal.IsDriver() {
		return nil, ErrPermissionDenied
	}

	scope, err := s.scopes.ResolveScope(ctx, principal)
	if err != nil || scope.Type == model.ScopeTechnical {
		return nil, ErrPermissionDenied
	}

	contracts, err := s.analytics.ContractProgress(ctx, scope)
	if err != nil {
		return nil, err
	}

	summary := make([]model.ContractProgress, len(contracts))
	copy(summary, contracts)

	sort.Slice(summary, func(i, j int) bool {
		return summary[i].BudgetProgress > summary[j].BudgetProgress
	})

	topBudget := takeContracts(summary, 5)
	atRisk := filterContracts(contracts, func(c model.ContractProgress) bool {
		return c.UIStatus == "EXPIRED" && c.Result == "FAIL"
	})
	budgetIssues := filterContracts(contracts, func(c model.ContractProgress) bool {
		return c.BudgetProgress > 1.0
	})

	sort.Slice(atRisk, func(i, j int) bool {
		return atRisk[i].VolumeProgress < atRisk[j].VolumeProgress
	})
	sort.Slice(budgetIssues, func(i, j int) bool {
		return budgetIssues[i].BudgetProgress > budgetIssues[j].BudgetProgress
	})

	return &model.ContractAnalytics{
		Summary:      contracts,
		TopBudget:    topBudget,
		AtRisk:       takeContracts(atRisk, 5),
		BudgetIssues: takeContracts(budgetIssues, 5),
	}, nil
}

func (s *AnalyticsService) GetAreaAnalytics(ctx context.Context, principal model.Principal, filter model.AnalyticsFilter) ([]model.CleaningAreaAnalytics, error) {
	if principal.IsDriver() || principal.IsToo() {
		return nil, ErrPermissionDenied
	}

	scope, err := s.scopes.ResolveScope(ctx, principal)
	if err != nil || scope.Type == model.ScopeTechnical {
		return nil, ErrPermissionDenied
	}

	normalized := s.normalizeFilter(filter)
	data, err := s.analytics.CleaningAreaAnalytics(ctx, scope, normalized)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func (s *AnalyticsService) GetDriverKPIs(ctx context.Context, principal model.Principal, filter model.AnalyticsFilter) ([]model.DriverKPI, error) {
	if principal.IsDriver() || principal.IsToo() {
		return nil, ErrPermissionDenied
	}

	scope, err := s.scopes.ResolveScope(ctx, principal)
	if err != nil || scope.Type == model.ScopeTechnical {
		return nil, ErrPermissionDenied
	}

	normalized := s.normalizeFilter(filter)
	kpis, err := s.analytics.DriverKPIs(ctx, scope, normalized)
	if err != nil {
		return nil, err
	}

	return kpis, nil
}

func (s *AnalyticsService) GetVehicleKPIs(ctx context.Context, principal model.Principal, filter model.AnalyticsFilter) ([]model.VehicleKPI, error) {
	if principal.IsDriver() || principal.IsToo() {
		return nil, ErrPermissionDenied
	}

	scope, err := s.scopes.ResolveScope(ctx, principal)
	if err != nil || scope.Type == model.ScopeTechnical {
		return nil, ErrPermissionDenied
	}

	normalized := s.normalizeFilter(filter)
	kpis, err := s.analytics.VehicleKPIs(ctx, scope, normalized)
	if err != nil {
		return nil, err
	}

	return kpis, nil
}

func (s *AnalyticsService) GetTechnicalAnalytics(ctx context.Context, principal model.Principal, rng model.DateRange) (*model.TechnicalAnalytics, error) {
	if !(principal.IsToo() || principal.IsAkimat() || principal.IsKgu()) {
		return nil, ErrPermissionDenied
	}

	scope, err := s.scopes.ResolveScope(ctx, principal)
	if err != nil {
		return nil, err
	}

	normalized := s.normalizeRange(rng)
	data, err := s.analytics.TechnicalAnalytics(ctx, scope, normalized)
	if err != nil {
		return nil, err
	}

	return &data, nil
}

func (s *AnalyticsService) normalizeFilter(filter model.AnalyticsFilter) model.AnalyticsFilter {
	filter.Range = s.normalizeRange(filter.Range)
	filter.GroupBy = filter.Bucket()
	return filter
}

func (s *AnalyticsService) normalizeRange(rng model.DateRange) model.DateRange {
	if rng.To.IsZero() {
		rng.To = time.Now()
	}
	if rng.From.IsZero() {
		rng.From = rng.To.AddDate(0, 0, -s.defaultRange)
	}
	if rng.To.Before(rng.From) {
		rng.From = rng.To.Add(-24 * time.Hour)
	}
	maxDuration := time.Duration(s.maxRange) * 24 * time.Hour
	if rng.To.Sub(rng.From) > maxDuration {
		rng.From = rng.To.Add(-maxDuration)
	}
	return rng
}

func convertCameraLeaders(metrics []model.EntityMetric) []model.CameraLoadMetric {
	result := make([]model.CameraLoadMetric, 0, len(metrics))
	for _, m := range metrics {
		result = append(result, model.CameraLoadMetric{
			CameraID:    m.ID,
			CameraName:  m.Name,
			ErrorEvents: m.Count,
		})
	}
	return result
}

func takeContracts(items []model.ContractProgress, limit int) []model.ContractProgress {
	if len(items) <= limit {
		return items
	}
	return items[:limit]
}

func filterContracts(items []model.ContractProgress, predicate func(model.ContractProgress) bool) []model.ContractProgress {
	result := make([]model.ContractProgress, 0, len(items))
	for _, item := range items {
		if predicate(item) {
			result = append(result, item)
		}
	}
	return result
}
