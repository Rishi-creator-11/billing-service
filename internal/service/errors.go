package service

import (
	"context"
	"errors"
	"log/slog"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Rishi-creator-11/billing-service/internal/billing"
	"github.com/Rishi-creator-11/billing-service/internal/store"
)

func errInfo(reason string) *errdetails.ErrorInfo {
	return &errdetails.ErrorInfo{Reason: reason, Domain: "billing.v1"}
}

func invalid(msg string) error { return status.Error(codes.InvalidArgument, msg) }

// toStatus converts any error into a gRPC status error. Errors that are
// already statuses pass through; storage and unexpected errors are logged and
// returned as INTERNAL without leaking details.
func toStatus(ctx context.Context, log *slog.Logger, err error) error {
	if err == nil {
		return nil
	}
	if _, ok := status.FromError(err); ok && status.Code(err) != codes.Unknown {
		return err
	}
	var te *billing.TransitionError
	switch {
	case errors.As(err, &te):
		return withReason(codes.FailedPrecondition, "INVALID_STATE_TRANSITION", "%s", te.Error())
	case errors.Is(err, store.ErrNotFound):
		return status.Error(codes.NotFound, "resource not found")
	case errors.Is(err, store.ErrAlreadyExists):
		return status.Error(codes.AlreadyExists, "resource already exists")
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		return status.FromContextError(err).Err()
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return status.FromContextError(ctxErr).Err()
	}
	log.ErrorContext(ctx, "internal error", "error", err)
	return status.Error(codes.Internal, "internal error")
}
