DROP TABLE IF EXISTS telco;
CREATE TABLE IF NOT EXISTS telco (
    CustomerID TEXT,
    Gender TEXT,
    SeniorCitizen INTEGER,
    Partner TEXT,
    Dependents TEXT,
    Tenure INTEGER,
    PhoneService TEXT,
    MultipleLines TEXT,
    InternetService TEXT,
    OnlineSecurity TEXT,
    OnlineBackup TEXT,
    DeviceProtection TEXT,
    TechSupport TEXT,
    StreamingTV TEXT,
    StreamingMovies TEXT,
    Contract TEXT,
    PaperlessBilling TEXT,
    PaymentMethod TEXT,
    MonthlyCharges FLOAT,
    TotalCharges FLOAT,
    Churn TEXT
);
