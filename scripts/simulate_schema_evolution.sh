#!/usr/bin/env bash
# simulate_schema_evolution.sh
# ─────────────────────────────────────────────────────────────────────────────
# Run AFTER docker-compose up.
# Applies two ALTER TABLE statements to simulate schema evolution, then
# inserts test rows so the pipeline can process them with the new schemas.
#
# Usage:
#   ./scripts/simulate_schema_evolution.sh
# ─────────────────────────────────────────────────────────────────────────────

set -euo pipefail

CONTAINER="cdc-mysql"
DB="inventory"
ROOT_PW="${MYSQL_ROOT_PASSWORD:-rootpassword}"

run_sql() {
    docker exec -i "$CONTAINER" mysql -uroot -p"$ROOT_PW" "$DB" -e "$1"
}

echo "=== Schema Evolution Simulation ==="
echo ""
echo "Step 1 – Rename products.description → product_description"
run_sql "ALTER TABLE products RENAME COLUMN description TO product_description;"
echo "  ✓ Done"

echo ""
echo "Step 2 – Insert a new product row (triggers v2 schema capture)"
run_sql "INSERT INTO products (name, product_description, price) VALUES ('Schema-V2 Widget', 'New column name test', 9.99);"
echo "  ✓ Done"

echo ""
echo "Sleeping 30s to let the pipeline process schema change 1…"
sleep 30

echo ""
echo "Step 3 – Add NOT NULL column to customers (country_code)"
run_sql "ALTER TABLE customers ADD COLUMN country_code VARCHAR(3) NOT NULL DEFAULT 'USA';"
echo "  ✓ Done"

echo ""
echo "Step 4 – Insert a new customer row (triggers v2 schema capture)"
run_sql "INSERT INTO customers (first_name, last_name, email, country_code) VALUES ('Test', 'User', 'test.evolution@example.com', 'GBR');"
echo "  ✓ Done"

echo ""
echo "=== Simulation complete ==="
echo "Check ./data_lake/ for Parquet files with updated schemas."
echo "Check ./output/lineage_report.json for the lineage report."
