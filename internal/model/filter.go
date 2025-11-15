package model

import (
	"time"

	"github.com/google/uuid"
)

type GroupBy string

const (
	GroupByDay   GroupBy = "day"
	GroupByWeek  GroupBy = "week"
	GroupByMonth GroupBy = "month"
)

type AnalyticsFilter struct {
	Range        DateRange
	ContractorID *uuid.UUID
	DriverID     *uuid.UUID
	PolygonID    *uuid.UUID
	CameraID     *uuid.UUID
	GroupBy      GroupBy
}

func (f AnalyticsFilter) ClampRange(defaultRange, maxRange int) AnalyticsFilter {
	if f.Range.From.IsZero() || f.Range.To.IsZero() {
		f.Range.To = time.Now()
		f.Range.From = f.Range.To.AddDate(0, 0, -defaultRange)
	}
	if f.Range.To.Before(f.Range.From) {
		f.Range.To = f.Range.From.Add(24 * time.Hour)
	}
	if f.Range.To.Sub(f.Range.From) > time.Duration(maxRange)*24*time.Hour {
		f.Range.From = f.Range.To.Add(-time.Duration(maxRange) * 24 * time.Hour)
	}
	return f
}

func (f AnalyticsFilter) Bucket() GroupBy {
	switch f.GroupBy {
	case GroupByWeek, GroupByMonth:
		return f.GroupBy
	default:
		return GroupByDay
	}
}
