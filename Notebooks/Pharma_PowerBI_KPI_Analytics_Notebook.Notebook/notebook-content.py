# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "7f3feb17-1239-4f7a-b910-f14143f53674",
# META       "default_lakehouse_name": "LH_PHARMA",
# META       "default_lakehouse_workspace_id": "16650313-386a-4ca3-8c9e-ff3ca2b04f6d",
# META       "known_lakehouses": [
# META         {
# META           "id": "7f3feb17-1239-4f7a-b910-f14143f53674"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Pharma Analytics KPI Notebook
# Load Gold tables and run KPI queries for Power BI.

# CELL ********************

from pyspark.sql import functions as F

sales=spark.table("FACT_SALES")
products=spark.table("DIM_PRODUCT")
customers=spark.table("DIM_CUSTOMER")
doctors=spark.table("DIM_DOCTOR")
#regions=spark.table("DIM_REGION")
distributors=spark.table("DIM_DISTRIBUTOR")
dates=spark.table("DIM_DATE")
campaigns=spark.table("DIM_CAMPAIGN")
prescriptions=spark.table("FACT_PRESCRIPTIONS")


sales.createOrReplaceTempView("sales")
products.createOrReplaceTempView("products")
customers.createOrReplaceTempView("customers")
doctors.createOrReplaceTempView("doctors")
#regions.createOrReplaceTempView("regions")
distributors.createOrReplaceTempView("distributors")
dates.createOrReplaceTempView("dates")
campaigns.createOrReplaceTempView("campaigns")
prescriptions.createOrReplaceTempView("prescriptions")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Revenue

# CELL ********************

df = spark.sql("""
    SELECT ROUND(SUM(GROSS_AMOUNT), 2) AS TOTAL_REVENUE
    FROM LH_PHARMA.dbo.fact_sales
""")
display(df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Profit Margin

# CELL ********************

df = spark.sql("""
    SELECT
        ROUND(SUM(PROFIT), 2) AS TOTAL_GROSS_PROFIT,
        ROUND(SUM(GROSS_AMOUNT), 2) AS TOTAL_REVENUE,
        ROUND(SUM(PROFIT) / NULLIF(SUM(GROSS_AMOUNT), 0) * 100, 2) AS GP_MARGIN_PCT
    FROM LH_PHARMA.dbo.fact_sales
""")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Discount & Unit Sold

# CELL ********************

df = spark.sql("""
    SELECT
        ROUND(AVG(DISCOUNT_PCT), 2) AS AVG_DISCOUNT_PCT,
        SUM(QUANTITY) AS TOTAL_UNITS_SOLD
    FROM LH_PHARMA.dbo.fact_sales
""")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Prescriptions

# CELL ********************

df = spark.sql("""
    SELECT
        COUNT(DISTINCT PRESCRIPTION_ID) AS TOTAL_PRESCRIPTIONS,
        SUM(QUANTITY_PRESCRIBED) AS TOTAL_QTY_PRESCRIBED,
        COUNT(DISTINCT DOCTOR_ID) AS ACTIVE_PRESCRIBING_DOCTORS
    FROM LH_PHARMA.dbo.fact_prescriptions
""")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Top 10 Products By Revenue 

# CELL ********************

df = spark.sql("""
    SELECT
        p.PRODUCT_ID,
        p.PRODUCT_NAME,
        p.CATEGORY,
        ROUND(SUM(f.GROSS_AMOUNT), 2) AS TOTAL_REVENUE,
        SUM(f.QUANTITY) AS TOTAL_UNITS_SOLD
    FROM LH_PHARMA.dbo.fact_sales f
    JOIN LH_PHARMA.dbo.dim_product p ON f.PRODUCT_ID = p.PRODUCT_ID
    GROUP BY p.PRODUCT_ID, p.PRODUCT_NAME, p.CATEGORY
    ORDER BY TOTAL_REVENUE DESC
    LIMIT 10
""")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Revenue by Region 

# CELL ********************

df = spark.sql("""
    SELECT
        REGION,
        ROUND(SUM(GROSS_AMOUNT), 2) AS TOTAL_REVENUE,
        COUNT(DISTINCT SALES_ID) AS TOTAL_ORDERS
    FROM LH_PHARMA.dbo.fact_sales
    WHERE IS_VALID = 'Y'
    GROUP BY REGION
    ORDER BY TOTAL_REVENUE DESC
""")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Monthly Sales

# CELL ********************

df=spark.sql("""SELECT d.YEAR,SUM(s.GROSS_AMOUNT) SALES FROM LH_PHARMA.dbo.fact_sales s 
JOIN LH_PHARMA.dbo.dim_date d ON s.DATE_KEY=d.DATE_KEY GROUP BY d.YEAR,d.MONTH ORDER BY d.YEAR,d.MONTH""")
display(df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM LH_PHARMA.dbo.dim_date LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Yearly Sales

# CELL ********************

df=spark.sql("""SELECT d.YEAR,SUM(s.GROSS_AMOUNT) SALES FROM LH_PHARMA.dbo.fact_sales s 
JOIN dim_date d ON s.DATE_KEY=d.DATE_KEY GROUP BY d.YEAR ORDER BY d.YEAR""")
display(df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Sales by Product

# CELL ********************

df=spark.sql("""SELECT p.PRODUCT_NAME,SUM(s.GROSS_AMOUNT) SALES FROM LH_PHARMA.dbo.fact_sales s 
JOIN LH_PHARMA.dbo.dim_product p ON s.PRODUCT_ID=p.PRODUCT_ID GROUP BY p.PRODUCT_NAME ORDER BY SALES DESC""")
display(df)
# df.write.mode("overwrite").saveAsTable("KPI_SALES_BY_PRODUCT")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM LH_PHARMA.dbo.dim_product LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Sales by Brand

# CELL ********************

df=spark.sql("""SELECT p.BRAND_NAME,SUM(s.GROSS_REVENUE) SALES FROM sales s JOIN products p ON s.PRODUCT_CODE=p.PRODUCT_CODE GROUP BY p.BRAND_NAME""")
display(df)
# df.write.mode("overwrite").saveAsTable("KPI_SALES_BY_BRAND")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Sales by Therapeutic Area

# CELL ********************

df=spark.sql("""SELECT p.THERAPEUTIC_AREA,SUM(s.GROSS_REVENUE) SALES FROM sales s JOIN products p ON s.PRODUCT_CODE=p.PRODUCT_CODE GROUP BY p.THERAPEUTIC_AREA""")
display(df)
# df.write.mode("overwrite").saveAsTable("KPI_SALES_BY_THERAPEUTIC_AREA")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Sales by Customer

# CELL ********************

df=spark.sql("""SELECT c.CUSTOMER_NAME,SUM(s.GROSS_REVENUE) SALES FROM sales s JOIN customers c ON s.CUSTOMER_ID=c.CUSTOMER_ID GROUP BY c.CUSTOMER_NAME ORDER BY SALES DESC""")
display(df)
# df.write.mode("overwrite").saveAsTable("KPI_SALES_BY_CUSTOMER")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Sales by Customer Type

# CELL ********************

df=spark.sql("""SELECT c.CUSTOMER_TYPE,SUM(s.GROSS_REVENUE) SALES FROM sales s JOIN customers c ON s.CUSTOMER_ID=c.CUSTOMER_ID GROUP BY c.CUSTOMER_TYPE""")
display(df)
# df.write.mode("overwrite").saveAsTable("KPI_SALES_BY_CUSTOMER_TYPE")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Sales by State

# CELL ********************

df=spark.sql("""SELECT r.STATE,SUM(s.GROSS_REVENUE) SALES FROM sales s JOIN regions r ON s.REGION_ID=r.REGION_ID GROUP BY r.STATE""")
display(df)
# df.write.mode("overwrite").saveAsTable("KPI_SALES_BY_STATE")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Sales by Zone

# CELL ********************

df=spark.sql("""SELECT r.ZONE,SUM(s.GROSS_REVENUE) SALES FROM sales s JOIN regions r ON s.REGION_ID=r.REGION_ID GROUP BY r.ZONE""")
display(df)
# df.write.mode("overwrite").saveAsTable("KPI_SALES_BY_ZONE")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Sales by Distributor

# CELL ********************

df=spark.sql("""SELECT d.DISTRIBUTOR_NAME,SUM(s.GROSS_REVENUE) SALES FROM sales s JOIN distributors d ON s.DISTRIBUTOR_ID=d.DISTRIBUTOR_ID GROUP BY d.DISTRIBUTOR_NAME""")
display(df)
# df.write.mode("overwrite").saveAsTable("KPI_SALES_BY_DISTRIBUTOR")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Sales by Campaign

# CELL ********************

df=spark.sql("""SELECT c.CAMPAIGN_NAME,SUM(s.GROSS_REVENUE) SALES FROM sales s JOIN campaigns c ON s.CAMPAIGN_ID=c.CAMPAIGN_ID GROUP BY c.CAMPAIGN_NAME""")
display(df)
# df.write.mode("overwrite").saveAsTable("KPI_SALES_BY_CAMPAIGN")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Sales by Payment Status

# CELL ********************

df=spark.sql("""SELECT PAYMENT_STATUS,SUM(GROSS_REVENUE) SALES FROM sales GROUP BY PAYMENT_STATUS""")
display(df)
# df.write.mode("overwrite").saveAsTable("KPI_SALES_BY_PAYMENT_STATUS")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Sales by Order Channel

# CELL ********************

df=spark.sql("""SELECT ORDER_CHANNEL,SUM(GROSS_REVENUE) SALES FROM sales GROUP BY ORDER_CHANNEL""")
display(df)
# df.write.mode("overwrite").saveAsTable("KPI_SALES_BY_ORDER_CHANNEL")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Top 10 Products

# CELL ********************

df=spark.sql("""SELECT p.PRODUCT_NAME,SUM(s.GROSS_REVENUE) SALES FROM sales s JOIN products p ON s.PRODUCT_CODE=p.PRODUCT_CODE GROUP BY p.PRODUCT_NAME ORDER BY SALES DESC LIMIT 10""")
display(df)
# df.write.mode("overwrite").saveAsTable("KPI_TOP_10_PRODUCTS")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Profit Analysis

# CELL ********************

df=spark.sql("""SELECT SUM(GROSS_PROFIT) TOTAL_PROFIT,ROUND(SUM(GROSS_PROFIT)/SUM(GROSS_REVENUE)*100,2) GP_MARGIN FROM sales""")
display(df)
# df.write.mode("overwrite").saveAsTable("KPI_PROFIT_ANALYSIS")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Prescription by Doctor

# CELL ********************

df=spark.sql("""SELECT d.DOCTOR_NAME,COUNT(*) RX FROM prescriptions p JOIN doctors d ON p.DOCTOR_ID=d.DOCTOR_ID GROUP BY d.DOCTOR_NAME ORDER BY RX DESC""")
display(df)
# df.write.mode("overwrite").saveAsTable("KPI_PRESCRIPTION_BY_DOCTOR")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Inventory by Warehouse

# CELL ********************

df=spark.sql("""SELECT WAREHOUSE_LOCATION,SUM(CLOSING_STOCK) STOCK FROM inventory GROUP BY WAREHOUSE_LOCATION""")
display(df)
# df.write.mode("overwrite").saveAsTable("KPI_INVENTORY_BY_WAREHOUSE")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Stockout Products

# CELL ********************

df=spark.sql("""SELECT PRODUCT_CODE,COUNT(*) STOCKOUTS FROM inventory WHERE STOCKOUT_FLAG='Y' GROUP BY PRODUCT_CODE""")
display(df)
# df.write.mode("overwrite").saveAsTable("KPI_STOCKOUT_PRODUCTS")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Reorder Products

# CELL ********************

df=spark.sql("""SELECT PRODUCT_CODE,COUNT(*) REORDER FROM inventory WHERE REORDER_FLAG='Y' GROUP BY PRODUCT_CODE""")
display(df)
# df.write.mode("overwrite").saveAsTable("KPI_REORDER_PRODUCTS")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
