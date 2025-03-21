from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, count

# ✅ Step 1: Create a Spark Session
spark = SparkSession.builder.appName("EcommerceSalesAnalysis").getOrCreate()

# ✅ Step 2: Load the dataset
df = spark.read.csv("data/ecommerce_sales.csv", header=True, inferSchema=True)

# ✅ Step 3: Display initial data
print("🔹 Sample Data:")
df.show(5)

# ✅ Step 4: Data Cleaning
df = df.dropna()  # Remove null values
df = df.withColumnRenamed("InvoiceNo", "Invoice_ID")
df = df.withColumnRenamed("StockCode", "Product_ID")

# ✅ Step 5: Register DataFrame as SQL Table
df.createOrReplaceTempView("sales")

# ✅ Step 6: Analysis Queries
print("\n🔹 Top 10 Best-Selling Products:")
spark.sql("""
    SELECT Product_ID, COUNT(*) as Total_Sales
    FROM sales
    GROUP BY Product_ID
    ORDER BY Total_Sales DESC
    LIMIT 10
""").show()

print("\n🔹 Total Revenue by Country:")
spark.sql("""
    SELECT Country, SUM(UnitPrice * Quantity) as Total_Revenue
    FROM sales
    GROUP BY Country
    ORDER BY Total_Revenue DESC
""").show()

# ✅ Step 7: Save Processed Data
df.write.csv("output/cleaned_ecommerce_sales.csv", header=True)

# ✅ Stop Spark Session
spark.stop()
print("✅ Analysis Completed & Results Saved!")
