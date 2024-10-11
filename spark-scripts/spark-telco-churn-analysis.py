import os
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, sum as _sum, countDistinct, when, avg

# Load environment variables from .env file (if required)
# dotenv_path = Path('/opt/app/.env')
# load_dotenv(dotenv_path=dotenv_path)

# Fetch environment variables for PostgreSQL
postgres_host = os.getenv('POSTGRES_CONTAINER_NAME')
postgres_dw_db = 'warehouse'
postgres_user = os.getenv('POSTGRES_USER')
postgres_password = os.getenv('POSTGRES_PASSWORD')

# Initialize Spark session with PostgreSQL JDBC driver using packages
try:
    spark = (
        SparkSession.builder
        .appName("Dibimbing")
        .master("local")  
        .config("spark.jars.packages", "org.postgresql:postgresql:42.2.18")  # Include PostgreSQL JDBC driver
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    print("SparkSession created successfully.")
except Exception as e:
    print(f"Error creating SparkSession: {e}")
    raise

# Set up PostgreSQL connection details
jdbc_url = f'jdbc:postgresql://{postgres_host}/{postgres_dw_db}'
jdbc_properties = {
    'user': postgres_user,
    'password': postgres_password,
    'driver': 'org.postgresql.Driver',
    'stringtype': 'unspecified'
}
print(jdbc_url)
print(jdbc_properties)

# Load telco data from PostgreSQL
try:
    telco_df = spark.read.jdbc(
        jdbc_url,
        'public.telco',
        properties=jdbc_properties
    )
    print("Data loaded successfully from PostgreSQL.")
except Exception as e:
    print(f"Error loading data from PostgreSQL: {e}")
    raise

# Perform analysis

# 1. Analisis: Distribusi Pelanggan Berdasarkan Gender
gender_distribution = telco_df.groupBy("gender").count().orderBy(F.desc("count"))
print("Distribusi Pelanggan Berdasarkan Gender:")
gender_distribution.show()

# 2. Analisis: Churn Berdasarkan Internet Service
telco_df = telco_df.withColumn("ChurnNumeric", when(col("Churn") == "Yes", 1).otherwise(0))
churn_by_internet_service = telco_df.groupBy("InternetService").agg(
    _sum("ChurnNumeric").alias("TotalChurnedCustomers"),
    countDistinct("customerID").alias("TotalCustomers")
).withColumn(
    "ChurnRate", col("TotalChurnedCustomers") / col("TotalCustomers")
).orderBy(F.desc("ChurnRate"))
print("Churn Berdasarkan Internet Service:")
churn_by_internet_service.show()

# 3. Analisis: Churn Berdasarkan Contract Type
churn_by_contract_type = telco_df.groupBy("Contract").agg(
    _sum("ChurnNumeric").alias("TotalChurnedCustomers"),
    countDistinct("customerID").alias("TotalCustomers")
).withColumn(
    "ChurnRate", col("TotalChurnedCustomers") / col("TotalCustomers")
).orderBy(F.desc("ChurnRate"))
print("Churn Berdasarkan Contract Type:")
churn_by_contract_type.show()

# 4. Analisis: Rata-rata Monthly Charges Berdasarkan Contract Type
avg_monthly_charges_by_contract = telco_df.groupBy("Contract").agg(
    avg("MonthlyCharges").alias("AvgMonthlyCharges")
).orderBy(F.desc("AvgMonthlyCharges"))
print("Rata-rata Monthly Charges Berdasarkan Contract Type:")
avg_monthly_charges_by_contract.show()

# 5. Analisis: Churn Berdasarkan Tenure
churn_by_tenure = telco_df.groupBy("tenure").agg(
    _sum("ChurnNumeric").alias("TotalChurnedCustomers"),
    countDistinct("customerID").alias("TotalCustomers")
).withColumn(
    "ChurnRate", col("TotalChurnedCustomers") / col("TotalCustomers")
).orderBy(col("tenure").asc())
print("Churn Berdasarkan Tenure:")
churn_by_tenure.show()

# 6. Analisis: Distribusi Pelanggan Berdasarkan Metode Pembayaran
payment_method_distribution = telco_df.groupBy("PaymentMethod").count().orderBy(F.desc("count"))
print("Distribusi Pelanggan Berdasarkan Metode Pembayaran:")
payment_method_distribution.show()

# 7. Analisis: Rata-rata Total Charges Berdasarkan Senior Citizen
avg_total_charges_by_senior_citizen = telco_df.groupBy("SeniorCitizen").agg(
    avg("TotalCharges").alias("AvgTotalCharges")
).orderBy(F.desc("AvgTotalCharges"))
print("Rata-rata Total Charges Berdasarkan Senior Citizen:")
avg_total_charges_by_senior_citizen.show()

# Update path untuk penyimpanan CSV di dalam container Docker
output_dir = "/opt/airflow/results"

# Pastikan direktori output hasil ada
os.makedirs(output_dir, exist_ok=True)

# Save the results back to PostgreSQL or as CSV files
gender_distribution.write.csv(f"{output_dir}/gender_distribution.csv", header=True, mode="overwrite")
churn_by_internet_service.write.csv(f"{output_dir}/churn_by_internet_service.csv", header=True, mode="overwrite")
churn_by_contract_type.write.csv(f"{output_dir}/churn_by_contract_type.csv", header=True, mode="overwrite")
avg_monthly_charges_by_contract.write.csv(f"{output_dir}/avg_monthly_charges_by_contract.csv", header=True, mode="overwrite")
churn_by_tenure.write.csv(f"{output_dir}/churn_by_tenure.csv", header=True, mode="overwrite")
payment_method_distribution.write.csv(f"{output_dir}/payment_method_distribution.csv", header=True, mode="overwrite")
avg_total_charges_by_senior_citizen.write.csv(f"{output_dir}/avg_total_charges_by_senior_citizen.csv", header=True, mode="overwrite")

print("Results successfully saved to CSV files in results directory.")


# Stop the Spark session
try:
    spark.stop()
    print("Spark session stopped successfully.")
except Exception as e:
    print(f"Error stopping Spark session: {e}")
