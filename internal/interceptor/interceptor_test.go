package interceptor

import (
	"context"
	"log/slog"
	"strings"
	"testing"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/Rishi-creator-11/billing-service/internal/metrics"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func ok(context.Context, any) (any, error) { return "ok", nil }

func TestAuth(t *testing.T) {
	intercept := Auth([]string{"secret", "other"})
	billing := &grpc.UnaryServerInfo{FullMethod: billingPrefix + "ChargeInvoice"}
	health := &grpc.UnaryServerInfo{FullMethod: "/grpc.health.v1.Health/Check"}
	with := func(kv ...string) context.Context {
		return metadata.NewIncomingContext(context.Background(), metadata.Pairs(kv...))
	}
	tests := []struct {
		name string
		ctx  context.Context
		info *grpc.UnaryServerInfo
		want codes.Code
	}{
		{"bearer ok", with("authorization", "Bearer secret"), billing, codes.OK},
		{"second key ok", with("x-api-key", "other"), billing, codes.OK},
		{"missing", context.Background(), billing, codes.Unauthenticated},
		{"wrong", with("authorization", "Bearer nope"), billing, codes.Unauthenticated},
		{"prefix of key rejected", with("x-api-key", "secre"), billing, codes.Unauthenticated},
		{"health is open", context.Background(), health, codes.OK},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := intercept(tt.ctx, nil, tt.info, ok)
			if status.Code(err) != tt.want {
				t.Fatalf("got %v want %v", err, tt.want)
			}
		})
	}
}

func TestRecoveryTurnsPanicIntoInternal(t *testing.T) {
	log := slog.New(slog.NewTextHandler(&strings.Builder{}, nil))
	_, err := Recovery(log)(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/x/y"},
		func(context.Context, any) (any, error) { panic("boom") })
	if status.Code(err) != codes.Internal {
		t.Fatalf("got %v", err)
	}
	if strings.Contains(err.Error(), "boom") {
		t.Fatal("panic value leaked to the client")
	}
}

func TestRequestIDPropagation(t *testing.T) {
	var seen string
	h := func(ctx context.Context, _ any) (any, error) { seen = RequestID(ctx); return nil, nil }
	info := &grpc.UnaryServerInfo{}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-request-id", "abc123"))
	_, _ = RequestIDUnary()(ctx, nil, info, h)
	if seen != "abc123" {
		t.Fatalf("incoming id not kept: %q", seen)
	}
	_, _ = RequestIDUnary()(context.Background(), nil, info, h)
	if len(seen) != 16 {
		t.Fatalf("generated id %q", seen)
	}
}

func TestMetricsRecorded(t *testing.T) {
	m := metrics.New()
	info := &grpc.UnaryServerInfo{FullMethod: billingPrefix + "ChargeInvoice"}
	_, _ = Metrics(m)(context.Background(), nil, info, ok)
	_, _ = Metrics(m)(context.Background(), nil, info, func(context.Context, any) (any, error) {
		return nil, status.Error(codes.NotFound, "x")
	})
	if got := testutil.ToFloat64(m.RPCRequests.WithLabelValues("ChargeInvoice", "OK")); got != 1 {
		t.Fatalf("OK count %v", got)
	}
	if got := testutil.ToFloat64(m.RPCRequests.WithLabelValues("ChargeInvoice", "NotFound")); got != 1 {
		t.Fatalf("NotFound count %v", got)
	}
}

func TestDefaultDeadline(t *testing.T) {
	_, _ = DefaultDeadline(5e9)(context.Background(), nil, nil, func(ctx context.Context, _ any) (any, error) {
		if _, ok := ctx.Deadline(); !ok {
			t.Error("no deadline applied")
		}
		return nil, nil
	})
}

func TestLoggingOmitsErrorMessage(t *testing.T) {
	var buf strings.Builder
	log := slog.New(slog.NewJSONHandler(&buf, nil))
	st, _ := status.New(codes.AlreadyExists, "customer jane@example.com already exists").
		WithDetails(&errdetails.ErrorInfo{Reason: "CUSTOMER_EXISTS", Domain: "billing.v1"})
	_, _ = Logging(log)(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/x/y"},
		func(context.Context, any) (any, error) { return nil, st.Err() })
	out := buf.String()
	if strings.Contains(out, "jane@example.com") {
		t.Fatalf("PII leaked into log: %s", out)
	}
	if !strings.Contains(out, "CUSTOMER_EXISTS") || !strings.Contains(out, "AlreadyExists") {
		t.Fatalf("code/reason missing: %s", out)
	}
}
