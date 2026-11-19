// Command billing-server runs the billing gRPC service and its REST gateway.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Rishi-creator-11/billing-service/internal/app"
	"github.com/Rishi-creator-11/billing-service/internal/interceptor"
	"github.com/Rishi-creator-11/billing-service/internal/telemetry"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		os.Exit(healthcheck())
	}
	if err := run(); err != nil {
		slog.Error("fatal", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := app.ConfigFromEnv()
	if err != nil {
		return err
	}
	log := slog.New(interceptor.LogHandler{Handler: slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: cfg.LogLevel})})
	slog.SetDefault(log)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	shutdownTracing, err := telemetry.Setup(ctx, "billing-service")
	if err != nil {
		return err
	}
	defer func() { _ = shutdownTracing(context.Background()) }()

	srv, err := app.New(ctx, cfg, log)
	if err != nil {
		return err
	}
	return srv.Serve(ctx)
}

// healthcheck probes the admin /readyz endpoint; it exists so the distroless
// image, which has no shell or curl, can define a container health check.
func healthcheck() int {
	addr := os.Getenv("BILLING_ADMIN_ADDR")
	if addr == "" {
		addr = ":9090"
	}
	if strings.HasPrefix(addr, ":") {
		addr = "127.0.0.1" + addr
	}
	c := http.Client{Timeout: 3 * time.Second}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+"/readyz", nil)
	if err != nil {
		return 1
	}
	resp, err := c.Do(req)
	if err != nil {
		return 1
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return 1
	}
	return 0
}
