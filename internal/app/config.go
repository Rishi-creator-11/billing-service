// Package app wires the gRPC server, REST gateway, health, metrics and
// shutdown together.
package app

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	GRPCAddr  string // gRPC listener
	HTTPAddr  string // REST/JSON gateway listener
	AdminAddr string // /metrics, /healthz, /readyz

	Store          string // "spanner" or "memory"
	SpannerDB      string // projects/P/instances/I/databases/D
	AutoCreateDB   bool   // create instance/database/schema if missing (emulator, local dev)
	APIKeys        []string
	AuthDisabled   bool
	DefaultTimeout time.Duration
	ShutdownGrace  time.Duration
	LogLevel       slog.Level
}

func env(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

// ConfigFromEnv reads configuration from environment variables.
func ConfigFromEnv() (Config, error) {
	cfg := Config{
		GRPCAddr:       env("BILLING_GRPC_ADDR", ":50051"),
		HTTPAddr:       env("BILLING_HTTP_ADDR", ":8080"),
		AdminAddr:      env("BILLING_ADMIN_ADDR", ":9090"),
		Store:          env("BILLING_STORE", "spanner"),
		SpannerDB:      env("SPANNER_DATABASE", "projects/billing-local/instances/billing/databases/billing"),
		DefaultTimeout: 30 * time.Second,
		ShutdownGrace:  15 * time.Second,
	}
	var err error
	if cfg.AutoCreateDB, err = strconv.ParseBool(env("BILLING_AUTO_CREATE_DB", "false")); err != nil {
		return cfg, fmt.Errorf("BILLING_AUTO_CREATE_DB: %w", err)
	}
	if cfg.AuthDisabled, err = strconv.ParseBool(env("BILLING_AUTH_DISABLED", "false")); err != nil {
		return cfg, fmt.Errorf("BILLING_AUTH_DISABLED: %w", err)
	}
	for _, k := range strings.Split(os.Getenv("BILLING_API_KEYS"), ",") {
		if k = strings.TrimSpace(k); k != "" {
			cfg.APIKeys = append(cfg.APIKeys, k)
		}
	}
	if err := cfg.LogLevel.UnmarshalText([]byte(env("LOG_LEVEL", "info"))); err != nil {
		return cfg, fmt.Errorf("LOG_LEVEL: %w", err)
	}
	return cfg, cfg.Validate()
}

func (c Config) Validate() error {
	switch c.Store {
	case "spanner", "memory":
	default:
		return fmt.Errorf("BILLING_STORE must be spanner or memory, got %q", c.Store)
	}
	if len(c.APIKeys) == 0 && !c.AuthDisabled {
		return errors.New("set BILLING_API_KEYS (comma separated) or BILLING_AUTH_DISABLED=true")
	}
	return nil
}
