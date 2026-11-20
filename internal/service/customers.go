package service

import (
	"context"
	"errors"
	"net/mail"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	billingv1 "github.com/Rishi-creator-11/billing-service/gen/billing/v1"
	"github.com/Rishi-creator-11/billing-service/internal/billing"
	"github.com/Rishi-creator-11/billing-service/internal/store"
)

func validateEmail(email string) error {
	addr, err := mail.ParseAddress(email)
	if err != nil || addr.Address != email {
		return invalid("email is not a valid address")
	}
	return nil
}

func validateName(name string) error {
	if len(name) > 255 {
		return invalid("name must be at most 255 characters")
	}
	return nil
}

func customerExists(email string) error {
	return withReason(codes.AlreadyExists, "CUSTOMER_EXISTS", "a customer with email %q already exists", email)
}

func (s *Service) CreateCustomer(ctx context.Context, req *billingv1.CreateCustomerRequest) (*billingv1.CreateCustomerResponse, error) {
	if err := validateEmail(req.GetEmail()); err != nil {
		return nil, err
	}
	if err := validateName(req.GetName()); err != nil {
		return nil, err
	}
	c := billing.Customer{ID: newID(), Email: strings.ToLower(req.GetEmail()), Name: req.GetName(), CreatedAt: s.clock().UTC()}
	err := s.rw(ctx, func(tx store.Tx) error { return tx.CreateCustomer(c) })
	if status.Code(err) == codes.AlreadyExists {
		return nil, customerExists(c.Email)
	}
	if err != nil {
		return nil, err
	}
	return &billingv1.CreateCustomerResponse{Customer: customerPB(c)}, nil
}

func (s *Service) GetCustomer(ctx context.Context, req *billingv1.GetCustomerRequest) (*billingv1.GetCustomerResponse, error) {
	var c billing.Customer
	err := s.ro(ctx, func(tx store.Tx) (err error) {
		c, err = getCustomer(tx, req.GetCustomerId())
		return err
	})
	if err != nil {
		return nil, err
	}
	return &billingv1.GetCustomerResponse{Customer: customerPB(c)}, nil
}

// getCustomer reads a customer, mapping a missing one to NOT_FOUND.
func getCustomer(tx store.Tx, id string) (billing.Customer, error) {
	c, err := tx.GetCustomer(id)
	if errors.Is(err, store.ErrNotFound) {
		return c, notFound("customer", id)
	}
	return c, err
}

func (s *Service) UpdateCustomer(ctx context.Context, req *billingv1.UpdateCustomerRequest) (*billingv1.UpdateCustomerResponse, error) {
	paths, err := maskPaths(req.GetUpdateMask(), "email", "name")
	if err != nil {
		return nil, err
	}
	if paths["email"] {
		if err := validateEmail(req.GetEmail()); err != nil {
			return nil, err
		}
	}
	if paths["name"] {
		if err := validateName(req.GetName()); err != nil {
			return nil, err
		}
	}
	var c billing.Customer
	err = s.rw(ctx, func(tx store.Tx) (err error) {
		if c, err = getCustomer(tx, req.GetCustomerId()); err != nil {
			return err
		}
		if paths["email"] {
			c.Email = strings.ToLower(req.GetEmail())
		}
		if paths["name"] {
			c.Name = req.GetName()
		}
		return tx.UpdateCustomer(c)
	})
	if status.Code(err) == codes.AlreadyExists {
		return nil, customerExists(strings.ToLower(req.GetEmail()))
	}
	if err != nil {
		return nil, err
	}
	return &billingv1.UpdateCustomerResponse{Customer: customerPB(c)}, nil
}

// DeleteCustomer refuses to delete a customer that has any subscription (in any
// state) or any ledger entry. The ledger is append-only and its entries point
// at the customer, so deleting would orphan financial history. The checks and
// the delete run in one serializable transaction, so a subscription created
// concurrently cannot slip past them.
func (s *Service) DeleteCustomer(ctx context.Context, req *billingv1.DeleteCustomerRequest) (*billingv1.DeleteCustomerResponse, error) {
	id := req.GetCustomerId()
	err := s.rw(ctx, func(tx store.Tx) error {
		if _, err := getCustomer(tx, id); err != nil {
			return err
		}
		subs, err := tx.ListSubscriptions(id, nil, 1)
		if err != nil {
			return err
		}
		entries, err := tx.ListLedger(id, nil, 1)
		if err != nil {
			return err
		}
		if len(subs) > 0 || len(entries) > 0 {
			return withReason(codes.FailedPrecondition, "CUSTOMER_HAS_ACTIVITY",
				"customer %q has subscriptions or ledger entries and cannot be deleted", id)
		}
		return tx.DeleteCustomer(id)
	})
	if err != nil {
		return nil, err
	}
	return &billingv1.DeleteCustomerResponse{}, nil
}

func (s *Service) ListCustomers(ctx context.Context, req *billingv1.ListCustomersRequest) (*billingv1.ListCustomersResponse, error) {
	size, err := pageSize(req.GetPageSize())
	if err != nil {
		return nil, err
	}
	cursor, err := decodeListCursor(req.GetPageToken())
	if err != nil {
		return nil, invalid("page_token is invalid")
	}
	var rows []billing.Customer
	err = s.ro(ctx, func(tx store.Tx) (err error) {
		rows, err = tx.ListCustomers(cursor, size+1) // one extra row tells us whether a next page exists
		return err
	})
	if err != nil {
		return nil, err
	}
	resp := &billingv1.ListCustomersResponse{}
	if len(rows) > size {
		rows = rows[:size]
		last := rows[size-1]
		resp.NextPageToken = encodeToken(last.CreatedAt, last.ID)
	}
	for _, c := range rows {
		resp.Customers = append(resp.Customers, customerPB(c))
	}
	return resp, nil
}
