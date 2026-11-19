package service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/Rishi-creator-11/billing-service/internal/billing"
	"github.com/Rishi-creator-11/billing-service/internal/store"
)

const maxKeyLen = 255

func validateKey(key string) error {
	if key == "" {
		return errors.New("idempotency_key is required")
	}
	if len(key) > maxKeyLen {
		return fmt.Errorf("idempotency_key must be at most %d characters", maxKeyLen)
	}
	for _, r := range key {
		if r < 0x21 || r > 0x7e {
			return errors.New("idempotency_key must contain printable ASCII without spaces")
		}
	}
	return nil
}

// fingerprint hashes the method and the request fields that define "the same
// request". Fields are length-prefixed so ("ab","c") and ("a","bc") differ.
func fingerprint(method string, fields ...string) []byte {
	h := sha256.New()
	for _, f := range append([]string{method}, fields...) {
		var n [8]byte
		binary.BigEndian.PutUint64(n[:], uint64(len(f)))
		h.Write(n[:])
		h.Write([]byte(f))
	}
	return h.Sum(nil)
}

var errKeyReuse = errors.New("idempotency key reused with a different request")

// runIdempotent executes exec at most once per key.
//
// In a single serializable transaction it (1) looks the key up, (2) replays
// the stored response if the fingerprint matches, rejects the request if it
// does not, or (3) runs exec and records the response together with exec's
// own writes. Because the effects and the record commit atomically, there is
// no window in which a charge exists without its key, and concurrent
// duplicates serialize on the key row: one commits, the rest replay.
//
// Only successful results are recorded. A request that fails validation or a
// precondition leaves no trace, so the client may fix the problem and retry
// with the same key.
func runIdempotent[R proto.Message](
	ctx context.Context, s *Service, method, key string, fp []byte,
	newResp func() R, exec func(tx store.Tx) (R, error),
) (resp R, replayed bool, err error) {
	ctx, span := otel.Tracer("billing-service").Start(ctx, "idempotent."+method)
	defer span.End()
	err = s.store.ReadWrite(ctx, func(tx store.Tx) error {
		resp, replayed = newResp(), false // reset: the function may be retried
		rec, err := tx.GetIdempotency(key)
		switch {
		case err == nil:
			if rec.Method != method || !bytes.Equal(rec.Fingerprint, fp) {
				return errKeyReuse
			}
			replayed = true
			return proto.Unmarshal(rec.Response, resp)
		case !errors.Is(err, store.ErrNotFound):
			return err
		}
		out, err := exec(tx)
		if err != nil {
			return err
		}
		blob, err := proto.Marshal(out)
		if err != nil {
			return err
		}
		resp = out
		return tx.PutIdempotency(billing.IdempotencyRecord{
			Key: key, Method: method, Fingerprint: fp, Response: blob, CreatedAt: s.clock().UTC(),
		})
	})
	span.SetAttributes(attribute.Bool("billing.idempotent_replay", replayed))
	if errors.Is(err, errKeyReuse) {
		s.metrics.IdempotencyConflicts.WithLabelValues(method).Inc()
		return resp, false, withReason(codes.FailedPrecondition, "IDEMPOTENCY_KEY_REUSED",
			"idempotency_key %q was already used with a different request; use a new key for a new request", key)
	}
	if replayed && err == nil {
		s.metrics.IdempotentReplays.WithLabelValues(method).Inc()
	}
	return resp, replayed, err
}

func withReason(c codes.Code, reason, format string, args ...any) error {
	st := status.Newf(c, format, args...)
	if d, err := st.WithDetails(errInfo(reason)); err == nil {
		return d.Err()
	}
	return st.Err()
}
