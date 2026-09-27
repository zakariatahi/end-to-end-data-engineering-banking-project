-- Small, synthetic example for learning and checking joins.
INSERT INTO customers (first_name, last_name, email) VALUES
    ('Amina', 'Bennani', 'amina@example.test'),
    ('Youssef', 'El Idrissi', 'youssef@example.test');

INSERT INTO accounts (customer_id, account_type, balance, currency) VALUES
    (1, 'CHECKING', 700.00, 'USD'),
    (1, 'SAVINGS', 200.00, 'USD'),
    (2, 'CHECKING', 500.00, 'USD');

INSERT INTO transactions (account_id, txn_type, amount, related_account_id) VALUES
    (1, 'DEPOSIT', 1000.00, NULL),
    (1, 'WITHDRAWAL', 100.00, NULL),
    (1, 'TRANSFER', 200.00, 2),
    (3, 'DEPOSIT', 500.00, NULL);
