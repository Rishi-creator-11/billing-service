package spanner

import (
	"context"
	_ "embed"
	"fmt"
	"strings"

	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

//go:embed schema.sql
var schemaSQL string

// Statements returns the DDL statements of the schema.
func Statements() []string {
	var out []string
	for _, s := range strings.Split(schemaSQL, ";") {
		if s = strings.TrimSpace(s); s != "" {
			out = append(out, s)
		}
	}
	return out
}

// EnsureDatabase creates the instance and database with the schema if they do
// not exist. It is meant for the emulator and local development; production
// schema changes belong in a migration pipeline.
func EnsureDatabase(ctx context.Context, project, instanceID, databaseID string) error {
	ia, err := instance.NewInstanceAdminClient(ctx)
	if err != nil {
		return fmt.Errorf("instance admin client: %w", err)
	}
	defer func() { _ = ia.Close() }()
	op, err := ia.CreateInstance(ctx, &instancepb.CreateInstanceRequest{
		Parent:     "projects/" + project,
		InstanceId: instanceID,
		Instance: &instancepb.Instance{
			Config:      fmt.Sprintf("projects/%s/instanceConfigs/emulator-config", project),
			DisplayName: instanceID,
			NodeCount:   1,
		},
	})
	if err == nil {
		_, err = op.Wait(ctx)
	}
	if err != nil && status.Code(err) != codes.AlreadyExists {
		return fmt.Errorf("create instance: %w", err)
	}

	da, err := database.NewDatabaseAdminClient(ctx)
	if err != nil {
		return fmt.Errorf("database admin client: %w", err)
	}
	defer func() { _ = da.Close() }()
	dop, err := da.CreateDatabase(ctx, &databasepb.CreateDatabaseRequest{
		Parent:          fmt.Sprintf("projects/%s/instances/%s", project, instanceID),
		CreateStatement: "CREATE DATABASE `" + databaseID + "`",
		ExtraStatements: Statements(),
	})
	if err == nil {
		_, err = dop.Wait(ctx)
	}
	if err != nil && status.Code(err) != codes.AlreadyExists {
		return fmt.Errorf("create database: %w", err)
	}
	return nil
}
