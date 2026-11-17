-- +goose Up
-- Demo data for `go run ./examples/modulith/cmd/modulith`. A real module
-- would not seed business data in a migration.
INSERT INTO inventory.stock (sku, available) VALUES
	('sku-apple', 50),
	('sku-pear', 20),
	('sku-plum', 3)
ON CONFLICT (sku) DO NOTHING;

-- +goose Down
SELECT 1;
