// Command billing-server runs the billing gRPC service and its REST gateway.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/Rishi-creator-11/billing-service/internal/app"
	"github.com/Rishi-creator-11/billing-service/internal/interceptor"
	"github.com/Rishi-creator-11/billing-service/internal/telemetry"
)

func main() {
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
