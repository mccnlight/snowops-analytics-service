package model

import (
	"time"

	"github.com/google/uuid"
)

type DateRange struct {
	From time.Time `json:"from"`
	To   time.Time `json:"to"`
}

type DashboardMetrics struct {
	Stats        DashboardStats         `json:"stats"`
	Areas        []CleaningAreaActivity `json:"areas"`
	Contractors  DashboardContractors   `json:"contractors"`
	Cameras      []CameraLoadMetric     `json:"cameras"`
	Contracts    []ContractProgress     `json:"contracts"`
	Map          MapSummary             `json:"map"`
	GeneratedFor DateRange              `json:"generated_for"`
}

type DashboardStats struct {
	ActiveTrips       int64 `json:"active_trips"`
	CompletedTrips    int64 `json:"completed_trips"`
	TicketsInProgress int64 `json:"tickets_in_progress"`
	Violations        int64 `json:"violations"`
}

type CleaningAreaActivity struct {
	CleaningAreaID uuid.UUID `json:"cleaning_area_id"`
	Trips          int64     `json:"trips"`
	ActiveTrips    int64     `json:"active_trips"`
	HasViolations  bool      `json:"has_violations"`
	TripHeat       float64   `json:"trip_heat"`
}

type DashboardContractors struct {
	Active []EntityMetric `json:"active"`
	Idle   []EntityMetric `json:"idle"`
}

type EntityMetric struct {
	ID     uuid.UUID `json:"id"`
	Name   string    `json:"name"`
	Count  int64     `json:"count"`
	Volume float64   `json:"volume"`
	Share  float64   `json:"share"`
}

type CameraLoadMetric struct {
	CameraID     uuid.UUID  `json:"camera_id"`
	CameraName   string     `json:"camera_name"`
	PolygonID    *uuid.UUID `json:"polygon_id,omitempty"`
	PolygonName  *string    `json:"polygon_name,omitempty"`
	LprEvents    int64      `json:"lpr_events"`
	VolumeEvents int64      `json:"volume_events"`
	ErrorEvents  int64      `json:"error_events"`
	ErrorRate    float64    `json:"error_rate"`
}

type ContractProgress struct {
	ContractID     uuid.UUID `json:"contract_id"`
	Name           string    `json:"name"`
	ContractorID   uuid.UUID `json:"contractor_id"`
	ContractorName string    `json:"contractor_name"`
	BudgetTotal    float64   `json:"budget_total"`
	TotalCost      float64   `json:"total_cost"`
	BudgetProgress float64   `json:"budget_progress"`
	MinimalVolume  float64   `json:"minimal_volume_m3"`
	TotalVolume    float64   `json:"total_volume_m3"`
	VolumeProgress float64   `json:"volume_progress"`
	UIStatus       string    `json:"ui_status"`
	Result         string    `json:"result"`
	StartAt        time.Time `json:"start_at"`
	EndAt          time.Time `json:"end_at"`
}

type MapSummary struct {
	Areas    []MapAreaState    `json:"areas"`
	Polygons []MapPolygonState `json:"polygons"`
	Cameras  []MapCameraState  `json:"cameras"`
}

type MapAreaState struct {
	ID             uuid.UUID `json:"id"`
	HasTrips       bool      `json:"has_trips"`
	HasActiveTrips bool      `json:"has_active_trips"`
	HasViolations  bool      `json:"has_violations"`
	Intensity      float64   `json:"intensity"`
}

type MapPolygonState struct {
	ID        uuid.UUID `json:"id"`
	Name      string    `json:"name"`
	TripCount int64     `json:"trip_count"`
	VolumeM3  float64   `json:"volume_m3"`
}

type MapCameraState struct {
	ID          uuid.UUID `json:"id"`
	Name        string    `json:"name"`
	Events      int64     `json:"events"`
	ErrorEvents int64     `json:"error_events"`
}

type SeriesPoint struct {
	Bucket time.Time `json:"bucket"`
	Count  int64     `json:"count"`
	Value  float64   `json:"value"`
}

type TripAnalytics struct {
	Series         []SeriesPoint     `json:"series"`
	VolumeSeries   []SeriesPoint     `json:"volume_series"`
	TopDrivers     []EntityMetric    `json:"top_drivers"`
	TopContractors []EntityMetric    `json:"top_contractors"`
	DurationStats  TripDurationStats `json:"duration_stats"`
	VolumeStats    TripVolumeStats   `json:"volume_stats"`
}

type TripDurationStats struct {
	AvgMinutes float64 `json:"avg_minutes"`
	P95Minutes float64 `json:"p95_minutes"`
}

type TripVolumeStats struct {
	AvgVolume float64 `json:"avg_volume"`
	MaxVolume float64 `json:"max_volume"`
	MinVolume float64 `json:"min_volume"`
}

type TripDetails struct {
	TripID              uuid.UUID         `json:"trip_id"`
	TicketID            *uuid.UUID        `json:"ticket_id,omitempty"`
	TicketName          *string           `json:"ticket_name,omitempty"`
	DriverID            *uuid.UUID        `json:"driver_id,omitempty"`
	DriverName          *string           `json:"driver_name,omitempty"`
	VehicleID           *uuid.UUID        `json:"vehicle_id,omitempty"`
	VehiclePlate        *string           `json:"vehicle_plate,omitempty"`
	ContractorID        *uuid.UUID        `json:"contractor_id,omitempty"`
	ContractorName      *string           `json:"contractor_name,omitempty"`
	CleaningAreaID      *uuid.UUID        `json:"cleaning_area_id,omitempty"`
	PolygonID           *uuid.UUID        `json:"polygon_id,omitempty"`
	Status              string            `json:"status"`
	EntryAt             time.Time         `json:"entry_at"`
	ExitAt              *time.Time        `json:"exit_at"`
	DetectedVolumeEntry *float64          `json:"detected_volume_entry"`
	DetectedVolumeExit  *float64          `json:"detected_volume_exit"`
	Violations          []ViolationRecord `json:"violations"`
	Events              TripEventDetails  `json:"events"`
}

type TripEventDetails struct {
	EntryLPR    *TripEvent `json:"entry_lpr,omitempty"`
	ExitLPR     *TripEvent `json:"exit_lpr,omitempty"`
	EntryVolume *TripEvent `json:"entry_volume,omitempty"`
	ExitVolume  *TripEvent `json:"exit_volume,omitempty"`
}

type TripEvent struct {
	EventID  uuid.UUID `json:"event_id"`
	CameraID uuid.UUID `json:"camera_id"`
	PhotoURL *string   `json:"photo_url,omitempty"`
	Captured time.Time `json:"captured_at"`
}

type ViolationRecord struct {
	Type   string    `json:"type"`
	Source string    `json:"source"`
	At     time.Time `json:"at"`
	Note   *string   `json:"note,omitempty"`
}

type ViolationAnalytics struct {
	Series         []SeriesPoint        `json:"series"`
	Breakdown      []ViolationBreakdown `json:"breakdown"`
	TopContractors []EntityMetric       `json:"top_contractors"`
	TopDrivers     []EntityMetric       `json:"top_drivers"`
	TopCameras     []CameraLoadMetric   `json:"top_cameras"`
}

type ViolationBreakdown struct {
	Type  string  `json:"type"`
	Count int64   `json:"count"`
	Share float64 `json:"share"`
}

type PerformanceAnalytics struct {
	Contractors []ContractorPerformance `json:"contractors"`
	Drivers     []DriverPerformance     `json:"drivers"`
	Vehicles    []VehiclePerformance    `json:"vehicles"`
}

type ContractorPerformance struct {
	ContractorID   uuid.UUID `json:"contractor_id"`
	ContractorName string    `json:"contractor_name"`
	TripCount      int64     `json:"trip_count"`
	AvgVolume      float64   `json:"avg_volume"`
	ViolationRate  float64   `json:"violation_rate"`
	ActiveDrivers  int64     `json:"active_drivers"`
	Utilization    float64   `json:"utilization"`
}

type DriverPerformance struct {
	DriverID      uuid.UUID `json:"driver_id"`
	DriverName    string    `json:"driver_name"`
	TripCount     int64     `json:"trip_count"`
	AvgVolume     float64   `json:"avg_volume"`
	ViolationRate float64   `json:"violation_rate"`
	AvgDuration   float64   `json:"avg_duration_minutes"`
}

type VehiclePerformance struct {
	VehicleID     uuid.UUID `json:"vehicle_id"`
	PlateNumber   string    `json:"plate_number"`
	TripCount     int64     `json:"trip_count"`
	AvgFillRate   float64   `json:"avg_fill_rate"`
	ViolationRate float64   `json:"violation_rate"`
	IdleHours     float64   `json:"idle_hours"`
}

type ContractAnalytics struct {
	Summary      []ContractProgress `json:"summary"`
	TopBudget    []ContractProgress `json:"top_budget"`
	AtRisk       []ContractProgress `json:"at_risk"`
	BudgetIssues []ContractProgress `json:"budget_issues"`
}

type CleaningAreaAnalytics struct {
	CleaningAreaID   uuid.UUID  `json:"cleaning_area_id"`
	Name             string     `json:"name"`
	Description      *string    `json:"description,omitempty"`
	TripCount        int64      `json:"trip_count"`
	VolumeM3         float64    `json:"total_volume_m3"`
	ViolationCount   int64      `json:"violation_count"`
	ActiveDrivers    int64      `json:"active_drivers"`
	ActiveVehicles   int64      `json:"active_vehicles"`
	AvgIntervalHours float64    `json:"avg_interval_hours"`
	IdleHours        float64    `json:"idle_hours"`
	LastTripAt       *time.Time `json:"last_trip_at,omitempty"`
	GeometryGeoJSON  *string    `json:"geometry_geojson,omitempty"`
}

type DriverKPI struct {
	DriverID       uuid.UUID  `json:"driver_id"`
	DriverName     string     `json:"driver_name"`
	ContractorID   *uuid.UUID `json:"contractor_id,omitempty"`
	ContractorName *string    `json:"contractor_name,omitempty"`
	TripCount      int64      `json:"trip_count"`
	AvgVolume      float64    `json:"avg_volume"`
	ViolationRate  float64    `json:"violation_rate"`
	AvgDuration    float64    `json:"avg_duration_minutes"`
	LastTripAt     *time.Time `json:"last_trip_at,omitempty"`
}

type VehicleKPI struct {
	VehicleID      uuid.UUID  `json:"vehicle_id"`
	PlateNumber    string     `json:"plate_number"`
	ContractorID   *uuid.UUID `json:"contractor_id,omitempty"`
	ContractorName *string    `json:"contractor_name,omitempty"`
	TripCount      int64      `json:"trip_count"`
	AvgFillRate    float64    `json:"avg_fill_rate"`
	ViolationRate  float64    `json:"violation_rate"`
	IdleHours      float64    `json:"idle_hours"`
	LastTripAt     *time.Time `json:"last_trip_at,omitempty"`
}

type PolygonLoadMetric struct {
	PolygonID   uuid.UUID `json:"polygon_id"`
	PolygonName string    `json:"polygon_name"`
	TripCount   int64     `json:"trip_count"`
	VolumeM3    float64   `json:"volume_m3"`
	ErrorEvents int64     `json:"error_events"`
}

type TechnicalAnalytics struct {
	Cameras        []CameraLoadMetric  `json:"cameras"`
	Polygons       []PolygonLoadMetric `json:"polygons"`
	ErrorRate      float64             `json:"error_rate"`
	LastEventAt    *time.Time          `json:"last_event_at,omitempty"`
	TotalEvents    int64               `json:"total_events"`
	EventFrequency float64             `json:"event_frequency_per_hour"`
}
