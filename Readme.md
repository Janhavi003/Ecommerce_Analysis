# 🎯 PySpark E-commerce Sales Analysis

## 📌 Overview

This project analyzes e-commerce sales data using **PySpark** to gain insights into business trends. The analysis focuses on identifying **top-selling products**, **revenue trends**, and other key metrics from large datasets.

---

## 📊 Dataset Information

* **Dataset**: [E-commerce Sales Dataset](https://www.kaggle.com/datasets/carrie1/ecommerce-data)
* **Columns Used**:

  * `InvoiceNo`: Transaction ID
  * `StockCode`: Product ID
  * `Quantity`: Number of items sold
  * `UnitPrice`: Price per unit
  * `CustomerID`: Customer identifier
  * `Country`: Country where the sale occurred
  * `Total_Sales`: Derived from `Quantity` \* `UnitPrice`, representing the total sales value of the transaction

---

## 📈 Analysis and Insights

The project uses **PySpark** for data cleaning, transformation, and analysis, focusing on the following objectives:

* **Data Preprocessing**: Cleaning the dataset by handling missing values, casting data types, and preparing the data for analysis.
* **Product Performance**: Identifying top-selling products based on total sales.
* **Revenue Trends**: Calculating and visualizing total revenue by country.
* **Recommendation System**: Using **ALS (Alternating Least Squares)** to create a recommendation model for personalized product suggestions.

---

## 🛠️ Technologies Used

* **PySpark**: For distributed data processing and machine learning.
* **Python**: Programming language for data manipulation and model building.
* **Spark MLlib**: For creating and evaluating recommendation models.

---

## 📥 How to Run the Project

1. **Clone the Repository**:

   ```
   git clone <repository-url>
   ```

2. **Install Dependencies**:
   Ensure you have PySpark installed:

   ```
   pip install pyspark
   ```

3. **Run the Analysis**:
   After setting up the environment, run the `ecommerceAnalysis.py` script to perform the analysis:

   ```
   python ecommerceAnalysis.py
   ```

---

## 🔑 Key Features

* **Data Processing**: Handles missing data, data type casting, and feature engineering.
* **Recommendation System**: Recommends products to users based on past purchase behavior.
* **Performance Evaluation**: Evaluates the model using RMSE (Root Mean Squared Error) to assess the accuracy of predictions.

---
