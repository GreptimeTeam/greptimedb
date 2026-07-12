-- Migrated from DuckDB test: test/sql/join/left_outer/ pattern tests
-- Tests common left join patterns

CREATE TABLE accounts(acc_id INTEGER, acc_name VARCHAR, balance DOUBLE, ts TIMESTAMP TIME INDEX);

CREATE TABLE transactions(txn_id INTEGER, acc_id INTEGER, amount DOUBLE, txn_type VARCHAR, txn_date DATE, ts TIMESTAMP TIME INDEX);

INSERT INTO accounts VALUES
(1, 'Checking', 1500.00, 1000), (2, 'Savings', 5000.00, 2000),
(3, 'Credit', -250.00, 3000), (4, 'Investment', 10000.00, 4000);

INSERT INTO transactions VALUES
(1, 1, -100.00, 'withdrawal', '2023-01-01', 1000),
(2, 1, 500.00, 'deposit', '2023-01-02', 2000),
(3, 2, 1000.00, 'deposit', '2023-01-01', 3000),
(4, 3, -50.00, 'purchase', '2023-01-03', 4000),
(5, 1, -25.00, 'fee', '2023-01-04', 5000);

-- Left join to find accounts with/without transactions
SELECT
  a.acc_name, a.balance,
  COUNT(t.txn_id) as transaction_count,
  COALESCE(SUM(t.amount), 0) as total_activity
FROM accounts a
LEFT JOIN transactions t ON a.acc_id = t.acc_id
GROUP BY a.acc_id, a.acc_name, a.balance
ORDER BY transaction_count DESC, total_activity DESC;

-- Left join with date filtering
SELECT
  a.acc_name,
  COUNT(t.txn_id) as recent_transactions,
  SUM(CASE WHEN t.amount > 0 THEN t.amount ELSE 0 END) as deposits,
  SUM(CASE WHEN t.amount < 0 THEN t.amount ELSE 0 END) as withdrawals
FROM accounts a
LEFT JOIN transactions t ON a.acc_id = t.acc_id AND t.txn_date >= '2023-01-02'
GROUP BY a.acc_id, a.acc_name
ORDER BY recent_transactions DESC, a.acc_name ASC;

-- Left join NULL handling
SELECT
  a.acc_name,
  a.balance,
  t.txn_id,
  COALESCE(t.amount, 0) as transaction_amount,
  CASE WHEN t.txn_id IS NULL THEN 'No Activity' ELSE 'Has Activity' END as status
FROM accounts a
LEFT JOIN transactions t ON a.acc_id = t.acc_id
ORDER BY a.acc_id, t.txn_date;

-- Left join with complex conditions
SELECT
  a.acc_name,
  COUNT(large_txn.txn_id) as large_transaction_count,
  AVG(large_txn.amount) as avg_large_amount
FROM accounts a
LEFT JOIN (
  SELECT * FROM transactions WHERE ABS(amount) > 100.00
) large_txn ON a.acc_id = large_txn.acc_id
GROUP BY a.acc_id, a.acc_name
ORDER BY large_transaction_count DESC, a.acc_name ASC;

DROP TABLE accounts;

DROP TABLE transactions;
