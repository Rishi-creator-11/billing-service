package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	billingv1 "github.com/Rishi-creator-11/billing-service/gen/billing/v1"
	"github.com/Rishi-creator-11/billing-service/internal/interceptor"
	"github.com/Rishi-creator-11/billing-service/internal/metrics"
	"github.com/Rishi-creator-11/billing-service/internal/service"
	"github.com/Rishi-creator-11/billing-service/internal/store"
	"github.com/Rishi-creator-11/billing-service/internal/store/memory"
	"github.com/Rishi-creator-11/billing-service/internal/store/spanner"
)

// Server is a running billing service: gRPC, REST gateway and admin listeners.
type Server struct {
	cfg     Config
	log     *slog.Logger
	store   store.Store
	metrics *metrics.Metrics
	health  *health.Server

	grpcLn, httpLn, adminLn net.Listener
	grpcSrv                 *grpc.Server
	httpSrv, adminSrv       *http.Server
	svcOpts                 []service.Option
}

// Option customises a Server, mainly for tests.
type Option func(*Server)

// WithStore injects a store instead of opening one from the config.
func WithStore(s store.Store) Option { return func(srv *Server) { srv.store = s } }

// WithServiceOptions passes options to the billing service.
func WithServiceOptions(o ...service.Option) Option {
	return func(srv *Server) { srv.svcOpts = append(srv.svcOpts, o...) }
}

// New opens the store, binds the listeners and builds the servers.
func New(ctx context.Context, cfg Config, log *slog.Logger, opts ...Option) (*Server, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	s := &Server{cfg: cfg, log: log, metrics: metrics.New(), health: health.NewServer()}
	for _, o := range opts {
		o(s)
	}
	if s.store == nil {
		st, err := openStore(ctx, cfg, log)
		if err != nil {
			return nil, err
		}
		s.store = st
	}

	var err error
	lc := net.ListenConfig{}
	if s.grpcLn, err = lc.Listen(ctx, "tcp", cfg.GRPCAddr); err != nil {
		return nil, fmt.Errorf("listen grpc: %w", err)
	}
	if s.httpLn, err = lc.Listen(ctx, "tcp", cfg.HTTPAddr); err != nil {
		return nil, fmt.Errorf("listen http: %w", err)
	}
	if s.adminLn, err = lc.Listen(ctx, "tcp", cfg.AdminAddr); err != nil {
		return nil, fmt.Errorf("listen admin: %w", err)
	}

	svc := service.New(s.store, append([]service.Option{
		service.WithMetrics(s.metrics), service.WithLogger(log),
	}, s.svcOpts...)...)

	unary := []grpc.UnaryServerInterceptor{
		interceptor.RequestIDUnary(),
		interceptor.Metrics(s.metrics),
		interceptor.Logging(log),
		interceptor.Recovery(log),
		interceptor.DefaultDeadline(cfg.DefaultTimeout),
	}
	serverOpts := []grpc.ServerOption{grpc.StatsHandler(otelgrpc.NewServerHandler())}
	if !cfg.AuthDisabled {
		unary = append(unary, interceptor.Auth(cfg.APIKeys))
		serverOpts = append(serverOpts, grpc.ChainStreamInterceptor(interceptor.AuthStream(cfg.APIKeys)))
	}
	serverOpts = append(serverOpts, grpc.ChainUnaryInterceptor(unary...))

	s.grpcSrv = grpc.NewServer(serverOpts...)
	billingv1.RegisterBillingServiceServer(s.grpcSrv, svc)
	billingv1.RegisterCustomerServiceServer(s.grpcSrv, svc)
	billingv1.RegisterSubscriptionServiceServer(s.grpcSrv, svc)
	healthpb.RegisterHealthServer(s.grpcSrv, s.health)
	reflection.Register(s.grpcSrv)

	gw, err := s.gateway(ctx)
	if err != nil {
		return nil, err
	}
	s.httpSrv = &http.Server{Handler: gw, ReadHeaderTimeout: 10 * time.Second}

	admin := http.NewServeMux()
	admin.Handle("/metrics", s.metrics.Handler())
	admin.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok\n")) })
	admin.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		resp, err := s.health.Check(r.Context(), &healthpb.HealthCheckRequest{})
		if err != nil || resp.Status != healthpb.HealthCheckResponse_SERVING {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("ready\n"))
	})
	s.adminSrv = &http.Server{Handler: admin, ReadHeaderTimeout: 10 * time.Second}
	return s, nil
}

func openStore(ctx context.Context, cfg Config, log *slog.Logger) (store.Store, error) {
	if cfg.Store == "memory" {
		log.Warn("using the in-memory store: data is lost on restart")
		return memory.New(), nil
	}
	if cfg.AutoCreateDB {
		parts := strings.Split(cfg.SpannerDB, "/") // projects/P/instances/I/databases/D
		if len(parts) != 6 {
			return nil, fmt.Errorf("SPANNER_DATABASE %q must look like projects/P/instances/I/databases/D", cfg.SpannerDB)
		}
		log.Info("ensuring spanner instance, database and schema", "database", cfg.SpannerDB)
		if err := ensureWithRetry(ctx, log, parts[1], parts[3], parts[5]); err != nil {
			return nil, err
		}
	}
	return spanner.Open(ctx, cfg.SpannerDB)
}

// gateway builds the REST/JSON handler. It calls the gRPC server over
// loopback so REST traffic passes through the same interceptors.
func (s *Server) gateway(ctx context.Context) (http.Handler, error) {
	mux := runtime.NewServeMux(
		runtime.WithMarshalerOption(runtime.MIMEWildcard, &runtime.JSONPb{
			MarshalOptions: protojsonMarshal(),
		}),
		runtime.WithIncomingHeaderMatcher(func(k string) (string, bool) {
			switch strings.ToLower(k) {
			case "x-api-key", "x-request-id":
				return strings.ToLower(k), true
			}
			return runtime.DefaultHeaderMatcher(k)
		}),
	)
	target := s.grpcLn.Addr().String()
	if strings.HasPrefix(target, "[::]") {
		target = "localhost" + target[len("[::]"):]
	}
	dial := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	for _, register := range []func(context.Context, *runtime.ServeMux, string, []grpc.DialOption) error{
		billingv1.RegisterBillingServiceHandlerFromEndpoint,
		billingv1.RegisterCustomerServiceHandlerFromEndpoint,
		billingv1.RegisterSubscriptionServiceHandlerFromEndpoint,
	} {
		if err := register(ctx, mux, target, dial); err != nil {
			return nil, err
		}
	}
	return mux, nil
}

// Addrs returns the bound gRPC, HTTP and admin addresses.
func (s *Server) Addrs() (grpcAddr, httpAddr, adminAddr string) {
	return s.grpcLn.Addr().String(), s.httpLn.Addr().String(), s.adminLn.Addr().String()
}

// Serve runs until ctx is cancelled, then drains: health flips to NOT_SERVING,
// the listeners stop accepting, and in-flight requests get ShutdownGrace to finish.
func (s *Server) Serve(ctx context.Context) error {
	s.health.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	for _, desc := range []grpc.ServiceDesc{billingv1.BillingService_ServiceDesc, billingv1.CustomerService_ServiceDesc, billingv1.SubscriptionService_ServiceDesc} {
		s.health.SetServingStatus(desc.ServiceName, healthpb.HealthCheckResponse_SERVING)
	}

	g, gctx := errgroup.WithContext(context.Background())
	serve := func(name string, fn func() error) {
		g.Go(func() error {
			if err := fn(); err != nil && !errors.Is(err, http.ErrServerClosed) && !errors.Is(err, grpc.ErrServerStopped) {
				return fmt.Errorf("%s: %w", name, err)
			}
			return nil
		})
	}
	serve("grpc", func() error { return s.grpcSrv.Serve(s.grpcLn) })
	serve("http", func() error { return s.httpSrv.Serve(s.httpLn) })
	serve("admin", func() error { return s.adminSrv.Serve(s.adminLn) })
	g1, h1, a1 := s.Addrs()
	s.log.Info("billing service started", "grpc", g1, "http", h1, "admin", a1, "store", s.cfg.Store)

	select {
	case <-ctx.Done():
		s.log.Info("shutdown requested, draining")
	case <-gctx.Done():
	}
	s.health.Shutdown() // marks every service NOT_SERVING
	shutCtx, cancel := context.WithTimeout(context.Background(), s.cfg.ShutdownGrace)
	defer cancel()

	// The gateway first (it is a client of the gRPC server), then gRPC.
	_ = s.httpSrv.Shutdown(shutCtx)
	stopped := make(chan struct{})
	go func() { s.grpcSrv.GracefulStop(); close(stopped) }()
	select {
	case <-stopped:
	case <-shutCtx.Done():
		s.log.Warn("grace period elapsed, forcing stop")
		s.grpcSrv.Stop()
	}
	_ = s.adminSrv.Shutdown(shutCtx)
	s.store.Close()
	s.log.Info("shutdown complete")
	return g.Wait()
}

// ensureWithRetry waits for the Spanner endpoint, which may still be starting
// when the service container comes up.
func ensureWithRetry(ctx context.Context, log *slog.Logger, project, instance, db string) error {
	deadline := time.Now().Add(60 * time.Second)
	for {
		actx, cancel := context.WithTimeout(ctx, 10*time.Second)
		err := spanner.EnsureDatabase(actx, project, instance, db)
		cancel()
		if err == nil || time.Now().After(deadline) || ctx.Err() != nil {
			return err
		}
		log.Warn("spanner not ready, retrying", "error", err)
		time.Sleep(2 * time.Second)
	}
}
