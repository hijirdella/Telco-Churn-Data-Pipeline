COPY telco FROM '/data/telco_customer.csv' DELIMITER AS ',' CSV HEADER;
SELECT * FROM telco LIMIT 5;