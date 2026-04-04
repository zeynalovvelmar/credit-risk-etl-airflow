CREATE TABLE credit_applications (
    row_id BIGSERIAL PRIMARY KEY,
    application_id      VARCHAR(50),
    customer_id         VARCHAR(50),
    application_date    DATE,
    requested_amount    NUMERIC(14,2),
    monthly_income      NUMERIC(14,2),
    existing_debt       NUMERIC(14,2),
    employment_years    NUMERIC(5,2),
    age                 INT,
    region              VARCHAR(50),
    loan_type           VARCHAR(30)
);


INSERT INTO credit_applications (
    application_id,
    customer_id,
    application_date,
    requested_amount,
    monthly_income,
    existing_debt,
    employment_years,
    age,
    region,
    loan_type
) VALUES
('APP1001', 'CUST001', '2026-04-03', 12000, 2500, 500, 3, 29, 'Baku',   'cash'),
('APP1002', 'CUST002', '2026-04-03', 150000, 10000, 1000, 5, 35, 'Ganja', 'mortgage'),
('APP1003', 'CUST003', '2026-04-03', 8000, 4000, 3200, 4, 31, 'Sumgait', 'cash'),
('APP1004', 'CUST004', '2026-04-03', 6000, 2200, 300, 0.5, 26, 'Baku',   'consumer'),
('APP1005', 'CUST005', '2026-04-03', 3000, 1800, 200, 2, 19, 'Shaki',  'cash'),
('APP1006', 'CUST006', '2026-04-03', 10000, 3500, 400, 6, 40, 'Baku',   'auto'),
('APP1007', 'CUST007', '2026-04-02', 9000, 3000, 500, 2, 30, 'Lankaran', 'cash'),
('APP1008', 'CUST008', '2026-04-03', 7000, 2800, 600, 3, 33, 'Mingachevir', 'consumer'),
('APP1008', 'CUST008', '2026-04-03', 7500, 2800, 600, 3, 33, 'Mingachevir', 'consumer'),
('APP1009', NULL, '2026-04-03', 5000, 2500, 300, 2, 28, 'Baku', 'cash'),
('APP1010', 'CUST010', '2026-04-03', 4000, 2000, 100, 1, 17, 'Ganja', 'consumer');
