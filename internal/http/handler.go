package http

import (
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/rs/zerolog"

	"analytics-service/internal/http/middleware"
	"analytics-service/internal/model"
	"analytics-service/internal/service"
)

type Handler struct {
	analytics *service.AnalyticsService
	log       zerolog.Logger
}

func NewHandler(analytics *service.AnalyticsService, log zerolog.Logger) *Handler {
	return &Handler{analytics: analytics, log: log}
}

func (h *Handler) Register(r *gin.Engine, authMiddleware gin.HandlerFunc) {
	protected := r.Group("/analytics")
	protected.Use(authMiddleware)

	protected.GET("/dashboard", h.getDashboard)
	protected.GET("/trips", h.getTripAnalytics)
	protected.GET("/trips/:id", h.getTripDetails)
	protected.GET("/violations", h.getViolationAnalytics)
	protected.GET("/performance", h.getPerformanceAnalytics)
	protected.GET("/contracts", h.getContractAnalytics)
	protected.GET("/areas", h.listAreas)
	protected.GET("/drivers", h.listDrivers)
	protected.GET("/vehicles", h.listVehicles)
	protected.GET("/technical", h.getTechnicalAnalytics)
}

func (h *Handler) getDashboard(c *gin.Context) {
	principal, ok := middleware.MustPrincipal(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, errorResponse("missing principal"))
		return
	}

	rangeFilter := model.DateRange{}
	if fromStr := strings.TrimSpace(c.Query("from")); fromStr != "" {
		if parsed, err := time.Parse(time.RFC3339, fromStr); err == nil {
			rangeFilter.From = parsed
		}
	}
	if toStr := strings.TrimSpace(c.Query("to")); toStr != "" {
		if parsed, err := time.Parse(time.RFC3339, toStr); err == nil {
			rangeFilter.To = parsed
		}
	}

	dashboard, err := h.analytics.GetDashboard(c.Request.Context(), principal, rangeFilter)
	if err != nil {
		h.handleError(c, err)
		return
	}

	c.JSON(http.StatusOK, successResponse(dashboard))
}

func (h *Handler) getTripAnalytics(c *gin.Context) {
	principal, ok := middleware.MustPrincipal(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, errorResponse("missing principal"))
		return
	}

	filter := h.parseAnalyticsFilter(c)

	analytics, err := h.analytics.GetTripAnalytics(c.Request.Context(), principal, filter)
	if err != nil {
		h.handleError(c, err)
		return
	}

	c.JSON(http.StatusOK, successResponse(analytics))
}

func (h *Handler) getTripDetails(c *gin.Context) {
	principal, ok := middleware.MustPrincipal(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, errorResponse("missing principal"))
		return
	}

	idStr := strings.TrimSpace(c.Param("id"))
	tripID, err := uuid.Parse(idStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, errorResponse("invalid trip id"))
		return
	}

	details, err := h.analytics.GetTripDetails(c.Request.Context(), principal, tripID)
	if err != nil {
		h.handleError(c, err)
		return
	}

	c.JSON(http.StatusOK, successResponse(details))
}

func (h *Handler) getViolationAnalytics(c *gin.Context) {
	principal, ok := middleware.MustPrincipal(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, errorResponse("missing principal"))
		return
	}

	filter := h.parseAnalyticsFilter(c)

	analytics, err := h.analytics.GetViolationAnalytics(c.Request.Context(), principal, filter)
	if err != nil {
		h.handleError(c, err)
		return
	}

	c.JSON(http.StatusOK, successResponse(analytics))
}

func (h *Handler) getPerformanceAnalytics(c *gin.Context) {
	principal, ok := middleware.MustPrincipal(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, errorResponse("missing principal"))
		return
	}

	filter := h.parseAnalyticsFilter(c)

	analytics, err := h.analytics.GetPerformanceAnalytics(c.Request.Context(), principal, filter)
	if err != nil {
		h.handleError(c, err)
		return
	}

	c.JSON(http.StatusOK, successResponse(analytics))
}

func (h *Handler) getContractAnalytics(c *gin.Context) {
	principal, ok := middleware.MustPrincipal(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, errorResponse("missing principal"))
		return
	}

	contracts, err := h.analytics.GetContractAnalytics(c.Request.Context(), principal)
	if err != nil {
		h.handleError(c, err)
		return
	}

	c.JSON(http.StatusOK, successResponse(contracts))
}

func (h *Handler) listAreas(c *gin.Context) {
	principal, ok := middleware.MustPrincipal(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, errorResponse("missing principal"))
		return
	}

	filter := h.parseAnalyticsFilter(c)
	areas, err := h.analytics.GetAreaAnalytics(c.Request.Context(), principal, filter)
	if err != nil {
		h.handleError(c, err)
		return
	}

	c.JSON(http.StatusOK, successResponse(areas))
}

func (h *Handler) listDrivers(c *gin.Context) {
	principal, ok := middleware.MustPrincipal(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, errorResponse("missing principal"))
		return
	}

	filter := h.parseAnalyticsFilter(c)
	drivers, err := h.analytics.GetDriverKPIs(c.Request.Context(), principal, filter)
	if err != nil {
		h.handleError(c, err)
		return
	}

	c.JSON(http.StatusOK, successResponse(drivers))
}

func (h *Handler) listVehicles(c *gin.Context) {
	principal, ok := middleware.MustPrincipal(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, errorResponse("missing principal"))
		return
	}

	filter := h.parseAnalyticsFilter(c)
	vehicles, err := h.analytics.GetVehicleKPIs(c.Request.Context(), principal, filter)
	if err != nil {
		h.handleError(c, err)
		return
	}

	c.JSON(http.StatusOK, successResponse(vehicles))
}

func (h *Handler) getTechnicalAnalytics(c *gin.Context) {
	principal, ok := middleware.MustPrincipal(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, errorResponse("missing principal"))
		return
	}

	rangeFilter := model.DateRange{}
	if fromStr := strings.TrimSpace(c.Query("from")); fromStr != "" {
		if parsed, err := time.Parse(time.RFC3339, fromStr); err == nil {
			rangeFilter.From = parsed
		}
	}
	if toStr := strings.TrimSpace(c.Query("to")); toStr != "" {
		if parsed, err := time.Parse(time.RFC3339, toStr); err == nil {
			rangeFilter.To = parsed
		}
	}

	data, err := h.analytics.GetTechnicalAnalytics(c.Request.Context(), principal, rangeFilter)
	if err != nil {
		h.handleError(c, err)
		return
	}

	c.JSON(http.StatusOK, successResponse(data))
}

func (h *Handler) parseAnalyticsFilter(c *gin.Context) model.AnalyticsFilter {
	filter := model.AnalyticsFilter{}

	if fromStr := strings.TrimSpace(c.Query("from")); fromStr != "" {
		if parsed, err := time.Parse(time.RFC3339, fromStr); err == nil {
			filter.Range.From = parsed
		}
	}
	if toStr := strings.TrimSpace(c.Query("to")); toStr != "" {
		if parsed, err := time.Parse(time.RFC3339, toStr); err == nil {
			filter.Range.To = parsed
		}
	}

	if contractorStr := strings.TrimSpace(c.Query("contractor_id")); contractorStr != "" {
		if id, err := uuid.Parse(contractorStr); err == nil {
			filter.ContractorID = &id
		}
	}
	if driverStr := strings.TrimSpace(c.Query("driver_id")); driverStr != "" {
		if id, err := uuid.Parse(driverStr); err == nil {
			filter.DriverID = &id
		}
	}
	if polygonStr := strings.TrimSpace(c.Query("polygon_id")); polygonStr != "" {
		if id, err := uuid.Parse(polygonStr); err == nil {
			filter.PolygonID = &id
		}
	}
	if cameraStr := strings.TrimSpace(c.Query("camera_id")); cameraStr != "" {
		if id, err := uuid.Parse(cameraStr); err == nil {
			filter.CameraID = &id
		}
	}

	switch strings.ToLower(strings.TrimSpace(c.Query("group_by"))) {
	case "week":
		filter.GroupBy = model.GroupByWeek
	case "month":
		filter.GroupBy = model.GroupByMonth
	default:
		filter.GroupBy = model.GroupByDay
	}

	return filter
}

func (h *Handler) handleError(c *gin.Context, err error) {
	switch {
	case errors.Is(err, service.ErrPermissionDenied):
		c.JSON(http.StatusForbidden, errorResponse(err.Error()))
	case errors.Is(err, service.ErrNotFound):
		c.JSON(http.StatusNotFound, errorResponse(err.Error()))
	default:
		h.log.Error().Err(err).Msg("handler error")
		c.JSON(http.StatusInternalServerError, errorResponse("internal error"))
	}
}

func successResponse(data interface{}) gin.H {
	return gin.H{"data": data}
}

func errorResponse(message string) gin.H {
	return gin.H{"error": message}
}
