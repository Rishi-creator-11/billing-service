// Package interceptor holds the gRPC server middleware: request IDs, panic
// recovery, structured logging, metrics, default deadlines and API-key auth.
package interceptor

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"log/slog"
	"runtime/debug"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"github.com/Rishi-creator-11/billing-service/internal/metrics"
)

const (
	requestIDHeader = "x-request-id"
	billingPrefix   = "/billing.v1.BillingService/"
)

type ctxKey struct{}

// RequestID returns the request ID stored in ctx, or "".
func RequestID(ctx context.Context) string {
	id, _ := ctx.Value(ctxKey{}).(string)
	return id
}

func newRequestID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// LogHandler wraps a slog.Handler so every record logged with a context that
// carries a request ID includes it.
type LogHandler struct{ slog.Handler }

func (h LogHandler) Handle(ctx context.Context, r slog.Record) error {
	if id := RequestID(ctx); id != "" {
		r.AddAttrs(slog.String("request_id", id))
	}
	return h.Handler.Handle(ctx, r)
}

func (h LogHandler) WithAttrs(a []slog.Attr) slog.Handler { return LogHandler{h.Handler.WithAttrs(a)} }
func (h LogHandler) WithGroup(n string) slog.Handler      { return LogHandler{h.Handler.WithGroup(n)} }

// RequestIDUnary honours an incoming x-request-id or generates one, stores it
// in the context and echoes it in the response headers.
func RequestIDUnary() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		id := ""
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			if v := md.Get(requestIDHeader); len(v) > 0 && len(v[0]) <= 64 {
				id = v[0]
			}
		}
		if id == "" {
			id = newRequestID()
		}
		_ = grpc.SetHeader(ctx, metadata.Pairs(requestIDHeader, id))
		return h(context.WithValue(ctx, ctxKey{}, id), req)
	}
}

// DefaultDeadline applies d to requests that arrive without a deadline.
func DefaultDeadline(d time.Duration) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		if _, ok := ctx.Deadline(); !ok {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, d)
			defer cancel()
		}
		return h(ctx, req)
	}
}

// Recovery converts a panic in a handler into an INTERNAL error and logs the stack.
func Recovery(log *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (resp any, err error) {
		defer func() {
			if r := recover(); r != nil {
				log.ErrorContext(ctx, "panic recovered", "method", info.FullMethod, "panic", fmt.Sprint(r), "stack", string(debug.Stack()))
				err = status.Error(codes.Internal, "internal error")
			}
		}()
		return h(ctx, req)
	}
}

// Logging writes one structured line per request.
func Logging(log *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := h(ctx, req)
		code := status.Code(err)
		level := slog.LevelInfo
		if code == codes.Internal || code == codes.Unknown || code == codes.DataLoss {
			level = slog.LevelError
		}
		attrs := []any{"method", info.FullMethod, "code", code.String(), "duration_ms", float64(time.Since(start).Microseconds()) / 1000}
		if p, ok := peer.FromContext(ctx); ok {
			attrs = append(attrs, "peer", p.Addr.String())
		}
		if err != nil && level == slog.LevelInfo {
			attrs = append(attrs, "error", status.Convert(err).Message())
		}
		log.Log(ctx, level, "rpc", attrs...)
		return resp, err
	}
}

// Metrics records request counts, latency and in-flight gauge.
func Metrics(m *metrics.Metrics) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		m.RPCInFlight.Inc()
		start := time.Now()
		resp, err := h(ctx, req)
		m.RPCInFlight.Dec()
		method := info.FullMethod[strings.LastIndex(info.FullMethod, "/")+1:]
		m.RPCLatency.WithLabelValues(method).Observe(time.Since(start).Seconds())
		m.RPCRequests.WithLabelValues(method, status.Code(err).String()).Inc()
		return resp, err
	}
}

// Auth checks a static API key from the "authorization: Bearer <key>" or
// "x-api-key" metadata. Only billing.v1.BillingService methods require it;
// the health and reflection services are open.
func Auth(keys []string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		if strings.HasPrefix(info.FullMethod, billingPrefix) {
			if err := checkKey(ctx, keys); err != nil {
				return nil, err
			}
		}
		return h(ctx, req)
	}
}

// AuthStream rejects streaming calls to the billing service (it has none) and
// leaves health and reflection streams open.
func AuthStream(keys []string) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, h grpc.StreamHandler) error {
		if strings.HasPrefix(info.FullMethod, billingPrefix) {
			if err := checkKey(ss.Context(), keys); err != nil {
				return err
			}
		}
		return h(srv, ss)
	}
}

func checkKey(ctx context.Context, keys []string) error {
	md, _ := metadata.FromIncomingContext(ctx)
	var presented string
	if v := md.Get("authorization"); len(v) > 0 {
		presented = strings.TrimPrefix(v[0], "Bearer ")
	} else if v := md.Get("x-api-key"); len(v) > 0 {
		presented = v[0]
	}
	if presented == "" {
		return status.Error(codes.Unauthenticated, "missing API key: send 'authorization: Bearer <key>' or 'x-api-key'")
	}
	ok := false
	for _, k := range keys {
		if subtle.ConstantTimeCompare([]byte(presented), []byte(k)) == 1 {
			ok = true
		}
	}
	if !ok {
		return status.Error(codes.Unauthenticated, "invalid API key")
	}
	return nil
}
