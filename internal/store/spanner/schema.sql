CREATE TABLE Customers (
  customer_id STRING(36) NOT NULL,
  email STRING(320) NOT NULL,
  name STRING(255) NOT NULL,
  create_time TIMESTAMP NOT NULL
) PRIMARY KEY (customer_id);

CREATE UNIQUE INDEX CustomersByEmail ON Customers(email);

CREATE TABLE Subscriptions (
  subscription_id STRING(36) NOT NULL,
  customer_id STRING(36) NOT NULL,
  plan_id STRING(128) NOT NULL,
  price_minor INT64 NOT NULL,
  currency STRING(3) NOT NULL,
  billing_interval STRING(16) NOT NULL,
  state STRING(16) NOT NULL,
  create_time TIMESTAMP NOT NULL,
  update_time TIMESTAMP NOT NULL,
  cancel_time TIMESTAMP,
  metadata STRING(MAX)
) PRIMARY KEY (subscription_id);

CREATE INDEX SubscriptionsByCustomer ON Subscriptions(customer_id, create_time, subscription_id);

CREATE TABLE Charges (
  charge_id STRING(36) NOT NULL,
  customer_id STRING(36) NOT NULL,
  subscription_id STRING(36) NOT NULL,
  invoice_id STRING(128) NOT NULL,
  amount_minor INT64 NOT NULL,
  currency STRING(3) NOT NULL,
  status STRING(32) NOT NULL,
  refunded_minor INT64 NOT NULL,
  decline_reason STRING(256) NOT NULL,
  create_time TIMESTAMP NOT NULL
) PRIMARY KEY (charge_id);

CREATE TABLE Refunds (
  refund_id STRING(36) NOT NULL,
  charge_id STRING(36) NOT NULL,
  amount_minor INT64 NOT NULL,
  currency STRING(3) NOT NULL,
  reason STRING(256) NOT NULL,
  create_time TIMESTAMP NOT NULL
) PRIMARY KEY (refund_id);

CREATE TABLE LedgerEntries (
  customer_id STRING(36) NOT NULL,
  create_time TIMESTAMP NOT NULL,
  entry_id STRING(36) NOT NULL,
  transaction_id STRING(36) NOT NULL,
  account STRING(64) NOT NULL,
  direction STRING(8) NOT NULL,
  amount_minor INT64 NOT NULL,
  currency STRING(3) NOT NULL,
  reference_id STRING(36) NOT NULL
) PRIMARY KEY (customer_id, create_time, entry_id);

CREATE TABLE IdempotencyKeys (
  idempotency_key STRING(255) NOT NULL,
  method STRING(64) NOT NULL,
  fingerprint BYTES(32) NOT NULL,
  response BYTES(MAX) NOT NULL,
  create_time TIMESTAMP NOT NULL
) PRIMARY KEY (idempotency_key);
