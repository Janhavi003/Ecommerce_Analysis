from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import StringIndexer

# Initialize Spark session
spark = SparkSession.builder.appName("EcommerceAnalysis").getOrCreate()

# Read the dataset
df = spark.read.csv(r"E:\Kalvium\Semester-4\PySpark_Projects\Ecommerce_Analysis\data\ecommerce_sales.csv", header=True, inferSchema=True)

# Show first few rows to check the data
df.show(5)

# Convert CustomerID to IntegerType (StockCode needs to be indexed as it is likely a string)
df = df.withColumn("CustomerID", col("CustomerID").cast(IntegerType()))

# Use StringIndexer to convert StockCode to a numeric type
indexer = StringIndexer(inputCol="StockCode", outputCol="StockCodeIndex")
df = indexer.fit(df).transform(df)

# Create the 'Total_Sales' column (assuming it's Quantity * UnitPrice)
df = df.withColumn("Total_Sales", col("Quantity") * col("UnitPrice"))

# Drop rows with null values in important columns
df = df.dropna(subset=["StockCodeIndex", "CustomerID", "Total_Sales"])

# Check the schema to ensure the columns are numeric
df.printSchema()

# Split the dataset into training and test sets (80% training, 20% test)
(training, test) = df.randomSplit([0.8, 0.2], seed=1234)

# Initialize the ALS model
als = ALS(userCol="CustomerID", itemCol="StockCodeIndex", ratingCol="Total_Sales", nonnegative=True, implicitPrefs=False)

# Fit the model on the training data
model = als.fit(training)

# Make predictions on the test data
predictions = model.transform(test)

# Show some predictions
predictions.show(5)

# Evaluate the model using RMSE (Root Mean Squared Error)
evaluator = RegressionEvaluator(predictionCol="prediction", labelCol="Total_Sales", metricName="rmse")
rmse = evaluator.evaluate(predictions)
print(f"Root-mean-square error (RMSE) on test data = {rmse}")

# Show top 10 best-selling products (adjust the aggregation if needed)
top_products = df.groupBy("StockCodeIndex").sum("Total_Sales").withColumnRenamed("sum(Total_Sales)", "Total_Sales") \
    .orderBy(col("Total_Sales").desc()).limit(10)
top_products.show()

# Show total revenue by country
revenue_by_country = df.groupBy("Country").sum("Total_Sales").withColumnRenamed("sum(Total_Sales)", "Total_Revenue") \
    .orderBy(col("Total_Revenue").desc())
revenue_by_country.show()

# Stop the Spark session
spark.stop()
