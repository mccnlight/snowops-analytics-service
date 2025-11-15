package main

import (
	"fmt"
	"os"

	"analytics-service/internal/auth"
	"analytics-service/internal/config"
	"analytics-service/internal/db"
	httphandler "analytics-service/internal/http"
	"analytics-service/internal/http/middleware"
	"analytics-service/internal/logger"
	"analytics-service/internal/repository"
	"analytics-service/internal/service"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load config: %v\n", err)
		os.Exit(1)
	}

	appLogger := logger.New(cfg.Environment)

	database, err := db.New(cfg, appLogger)
	if err != nil {
		appLogger.Fatal().Err(err).Msg("failed to connect database")
	}

	scopeRepo := repository.NewScopeRepository(database)
	analyticsRepo := repository.NewAnalyticsRepository(database)
	analyticsService := service.NewAnalyticsService(scopeRepo, analyticsRepo, cfg.Analytics.DefaultRangeDays, cfg.Analytics.MaxRangeDays)

	tokenParser := auth.NewParser(cfg.Auth.AccessSecret)

	handler := httphandler.NewHandler(analyticsService, appLogger)
	authMiddleware := middleware.Auth(tokenParser)
	router := httphandler.NewRouter(handler, authMiddleware, cfg.Environment)

	addr := fmt.Sprintf("%s:%d", cfg.HTTP.Host, cfg.HTTP.Port)
	appLogger.Info().Str("addr", addr).Msg("starting analytics service")

	if err := router.Run(addr); err != nil {
		appLogger.Error().Err(err).Msg("failed to start server")
		os.Exit(1)
	}
}
