-- Migrated from DuckDB test: test/sql/join/right_outer/ pattern tests  
-- Tests right join patterns

CREATE TABLE inventory(item_id INTEGER, item_name VARCHAR, stock_quantity INTEGER, ts TIMESTAMP TIME INDEX);

CREATE TABLE purchase_orders(po_id INTEGER, item_id INTEGER, ordered_qty INTEGER, order_date DATE, ts TIMESTAMP TIME INDEX);

INSERT INTO inventory VALUES 
(1, 'Widget A', 100, 1000), (2, 'Widget B', 50, 2000), (3, 'Widget C', 0, 3000);

INSERT INTO purchase_orders VALUES 
(1, 1, 25, '2023-01-01', 1000), (2, 2, 30, '2023-01-02', 2000),
(3, 4, 15, '2023-01-03', 3000), (4, 5, 40, '2023-01-04', 4000),
(5, 1, 10, '2023-01-05', 5000);

-- Right join to show all orders (including for non-inventory items)
SELECT 
  po.po_id, po.ordered_qty, po.order_date,
  COALESCE(i.item_name, 'Unknown Item') as item_name,
  COALESCE(i.stock_quantity, 0) as current_stock
FROM inventory i
RIGHT JOIN purchase_orders po ON i.item_id = po.item_id
ORDER BY po.order_date, po.po_id;

-- Right join with aggregation
SELECT 
  po.item_id,
  COUNT(po.po_id) as order_count,
  SUM(po.ordered_qty) as total_ordered,
  COALESCE(MAX(i.stock_quantity), 0) as stock_level,
  CASE WHEN i.item_id IS NULL THEN 'Not In Inventory' ELSE 'In Stock' END as inventory_status
FROM inventory i
RIGHT JOIN purchase_orders po ON i.item_id = po.item_id
GROUP BY po.item_id, i.item_id
ORDER BY order_count DESC;

-- Right join to identify missing inventory records
SELECT 
  po.item_id as missing_item_id,
  COUNT(*) as orders_for_missing_item,
  SUM(po.ordered_qty) as total_qty_ordered
FROM inventory i
RIGHT JOIN purchase_orders po ON i.item_id = po.item_id
WHERE i.item_id IS NULL
GROUP BY po.item_id
ORDER BY total_qty_ordered DESC;

-- Right join with filtering and conditions
SELECT 
  po.po_id,
  po.item_id,
  po.ordered_qty,
  COALESCE(i.item_name, 'Missing from inventory') as item_description,
  CASE 
    WHEN i.item_id IS NULL THEN 'Order for unknown item'
    WHEN i.stock_quantity < po.ordered_qty THEN 'Insufficient stock'
    ELSE 'Can fulfill'
  END as fulfillment_status
FROM inventory i
RIGHT JOIN purchase_orders po ON i.item_id = po.item_id
ORDER BY po.order_date;

DROP TABLE inventory;

DROP TABLE purchase_orders;