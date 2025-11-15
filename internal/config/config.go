package config

import (
	"fmt"

	"github.com/spf13/viper"
)

type HTTPConfig struct {
	Host string
	Port int
}

type DBConfig struct {
	DSN             string
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime string
}

type AuthConfig struct {
	AccessSecret string
}

type AnalyticsConfig struct {
	DefaultRangeDays int
	MaxRangeDays     int
}

type Config struct {
	Environment string
	HTTP        HTTPConfig
	DB          DBConfig
	Auth        AuthConfig
	Analytics   AnalyticsConfig
}

func Load() (*Config, error) {
	v := viper.New()
	v.SetConfigName("app")
	v.SetConfigType("env")
	v.AddConfigPath(".")
	v.AddConfigPath("./config")
	v.AddConfigPath("./deploy")
	v.AddConfigPath("./internal/config")

	v.AutomaticEnv()

	_ = v.ReadInConfig()

	cfg := &Config{
		Environment: v.GetString("APP_ENV"),
		HTTP: HTTPConfig{
			Host: v.GetString("HTTP_HOST"),
			Port: v.GetInt("HTTP_PORT"),
		},
		DB: DBConfig{
			DSN:             v.GetString("DB_DSN"),
			MaxOpenConns:    v.GetInt("DB_MAX_OPEN_CONNS"),
			MaxIdleConns:    v.GetInt("DB_MAX_IDLE_CONNS"),
			ConnMaxLifetime: v.GetString("DB_CONN_MAX_LIFETIME"),
		},
		Auth: AuthConfig{
			AccessSecret: v.GetString("JWT_ACCESS_SECRET"),
		},
		Analytics: AnalyticsConfig{
			DefaultRangeDays: v.GetInt("ANALYTICS_DEFAULT_RANGE_DAYS"),
			MaxRangeDays:     v.GetInt("ANALYTICS_MAX_RANGE_DAYS"),
		},
	}

	if cfg.HTTP.Host == "" {
		cfg.HTTP.Host = "0.0.0.0"
	}
	if cfg.HTTP.Port == 0 {
		cfg.HTTP.Port = 7085
	}
	if cfg.Environment == "" {
		cfg.Environment = "development"
	}
	if cfg.Analytics.DefaultRangeDays <= 0 {
		cfg.Analytics.DefaultRangeDays = 7
	}
	if cfg.Analytics.MaxRangeDays <= 0 {
		cfg.Analytics.MaxRangeDays = 90
	}

	if err := validate(cfg); err != nil {
		return nil, err
	}

	return cfg, nil
}

func validate(cfg *Config) error {
	if cfg.DB.DSN == "" {
		return fmt.Errorf("DB_DSN is required")
	}
	if cfg.Auth.AccessSecret == "" {
		return fmt.Errorf("JWT_ACCESS_SECRET is required")
	}
	return nil
}
