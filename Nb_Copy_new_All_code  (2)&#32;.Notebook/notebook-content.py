# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "979b3ce0-daad-4b9d-99c0-e79ce47ea318",
# META       "default_lakehouse_name": "Data_LH",
# META       "default_lakehouse_workspace_id": "700753fe-6cd9-42d2-b954-c7918e8eef57",
# META       "known_lakehouses": [
# META         {
# META           "id": "979b3ce0-daad-4b9d-99c0-e79ce47ea318"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

!pip install faker

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Configuration class for controlling data generation parameters 


# CELL ********************

# config_notebook (Microsoft Fabric Notebook)

# # Define configuration values as global variables or within a class
# class Config:
#     ORDER_GENERATION_DATE = "2024-01-01" # Date from which orders will be generated
#     inStockOnly = True         # Only generate orders for products that are in stock
#     SQL_SERVER_LOCAL = False 
#     CUSTOMERS_PER_DAY = 1000            # Number of orders per customer
#     CUSTOMERS_PER_DAY_VARIATION_PERCENTAGE = 10  # Variation in the number of customers per day
#     CUSTOMERS_TO_CREATE = 5000   # Total number of customers to create
#     SUPPLIERS_TO_CREATE = 200   # Total number of suppliers to create
    
#     MAX_ORDERS_PER_CUSTOMER = 2   # Maximum number of orders per customer
#     MAX_PRODUCTS_PER_ORDER = 5  # Maximum number of products per order
#     MAX_WAREHOUSES = 5   # Maximum number of warehouses

#     TEST_MODE = False  # If True, the script will run in test mode with a limited number of orders and customers
#     ITERATIONS_IN_TEST_MODE =  10 # Number of iterations in test mode
#     DEFAULT_BATCH_SIZE = 500   # Default batch size for seeding
class Config:
    ORDER_GENERATION_DATE = "2025-07-01"
    inStockOnly = True
    SQL_SERVER_LOCAL = False
    CUSTOMERS_PER_DAY = 50
    CUSTOMERS_PER_DAY_VARIATION_PERCENTAGE = 5
    CUSTOMERS_TO_CREATE = 500
    SUPPLIERS_TO_CREATE = 50
    MAX_ORDERS_PER_CUSTOMER = 1
    MAX_PRODUCTS_PER_ORDER = 2
    MAX_WAREHOUSES = 2
    TEST_MODE = False
    DEFAULT_BATCH_SIZE = 200
    Region = "LatAm"

    # Add these JDBC fields below
    DB_HOST = "dataandai-celestial.database.windows.net"
    DB_NAME = "Test_Azure_SQL_DB_CDC"
    DB_USER = "celestial-sa"
    DB_PASSWORD = "srGnyE%g8(95"

    JDBC_URL = (
        f"jdbc:sqlserver://{DB_HOST}:1433;"
        f"database={DB_NAME};"
        f"encrypt=true;"
        f"trustServerCertificate=false;"
        f"hostNameInCertificate=*.database.windows.net;"
        f"loginTimeout=30;"
    )



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## SQLAlchemy models for defining Product, Customer, Order, Transaction, and OrderItem tables

# CELL ********************

#%pip install sqlalchemy pyodbc

from sqlalchemy import create_engine, Column, Integer, String, ForeignKey
#from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker

from sqlalchemy.orm import declarative_base

# Define base
Base = declarative_base()

# Define Product table
class Product(Base):
    __tablename__ = 'products'
    
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    price = Column(Integer, nullable=False)
    stock = Column(Integer, nullable=False)

# Define Customer table
class Customer(Base):
    __tablename__ = 'customers'
    
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    email = Column(String, nullable=False)
    
    orders = relationship("Order", back_populates="customer")
    transactions = relationship("Transaction", back_populates="customer")

# Define Transaction table
class Transaction(Base):
    __tablename__ = 'transactions'
    
    id = Column(Integer, primary_key=True)
    product_id = Column(Integer, ForeignKey('products.id'), nullable=False)
    customer_id = Column(Integer, ForeignKey('customers.id'), nullable=False)
    quantity = Column(Integer, nullable=False)

    product = relationship("Product")
    customer = relationship("Customer", back_populates="transactions")

# Define Order table
class Order(Base):
    __tablename__ = 'orders'
    
    id = Column(Integer, primary_key=True)
    orderNumber = Column(String, nullable=False)
    orderDate = Column(String, nullable=False)
    customerId = Column(Integer, ForeignKey('customers.id'), nullable=False)
    discount = Column(Integer, default=0)
    statuses = Column(String, default="{}")
    orderSum = Column(Integer, nullable=False)
    notes = Column(String)

    customer = relationship("Customer", back_populates="orders")
    items = relationship("OrderItem", back_populates="order")

# Define OrderItem table
class OrderItem(Base):
    __tablename__ = 'order_items'

    id = Column(Integer, primary_key=True)
    order_id = Column(Integer, ForeignKey('orders.id'), nullable=False)
    product_id = Column(Integer, ForeignKey('products.id'), nullable=False)
    qty = Column(Integer, nullable=False)
    price = Column(Integer, nullable=False)

    order = relationship("Order", back_populates="items")
    product = relationship("Product")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Functions to connect to Azure SQL Database and create necessary tables 

# CELL ********************

from pyspark.sql import SparkSession
from py4j.java_gateway import java_import


def create_tables():
    spark = SparkSession.builder.getOrCreate()
    jvm = spark._jvm

    # Import required Java classes
    java_import(jvm, "java.sql.DriverManager")
    java_import(jvm, "java.sql.Connection")
    java_import(jvm, "java.sql.Statement")

    # JDBC connection details
    jdbc_url = (
        "jdbc:sqlserver://dataandai-celestial.database.windows.net:1433;"
        "database=Test_Azure_SQL_DB_CDC;"
        "encrypt=true;"
        "trustServerCertificate=false;"
        "hostNameInCertificate=*.database.windows.net;"
        "loginTimeout=30;"
    )
    user = "celestial-sa"
    password = "srGnyE%g8(95"

    # Establish JDBC connection
    conn = jvm.java.sql.DriverManager.getConnection(jdbc_url, user, password)
    stmt = conn.createStatement()


    ddl_statements = [
        # 1. productcategories
        """
        IF OBJECT_ID('productcategories', 'U') IS NULL
        CREATE TABLE [dbo].[productcategories](
            [id] SMALLINT NOT NULL,
            [category_name] NVARCHAR(50) NOT NULL,
            CONSTRAINT [PK_amazon_categories] PRIMARY KEY CLUSTERED ([id] ASC)
        )
        """,

        # 2. products
        """
        IF OBJECT_ID('products', 'U') IS NULL
        CREATE TABLE [dbo].[products](
            [id] INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
            [asin] NVARCHAR(15) NOT NULL,
            [title] NVARCHAR(800) NOT NULL,
            [imgUrl] NVARCHAR(250) NOT NULL,
            [productURL] NVARCHAR(250) NOT NULL,
            [stars] FLOAT NOT NULL,
            [reviews] NVARCHAR(50) NOT NULL,
            [price] FLOAT NOT NULL,
            [listPrice] FLOAT NOT NULL,
            [category_id] INT NOT NULL
        )
        """,

        # 3. customers
        """
        IF OBJECT_ID('customers', 'U') IS NULL
        CREATE TABLE [dbo].[customers](
            [id] INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
            [firstName] NVARCHAR(50) NOT NULL,
            [middleName] NVARCHAR(50),
            [lastName] NVARCHAR(50) NOT NULL,
            [company] NVARCHAR(150),
            [DOB] DATE NOT NULL,
            [email] NVARCHAR(50) NOT NULL,
            [phone] NVARCHAR(50),
            [address] NVARCHAR(150),
            [city] NVARCHAR(50),
            [state] NVARCHAR(2),
            [zip] NVARCHAR(10)
        )
        """,

    # 4. orders
        """
        IF OBJECT_ID('orders', 'U') IS NULL
        CREATE TABLE [dbo].[orders](
            [id] BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
            [orderNumber] NVARCHAR(50),
            [orderDate] DATE NOT NULL,
            [customerId] BIGINT,
            [discount] FLOAT,
            [statuses] NVARCHAR(MAX),
            [orderSum] FLOAT,
            [notes] NVARCHAR(MAX)
        )
        """,

        # 5. orderItems
        """
        IF OBJECT_ID('orderItems', 'U') IS NULL
        CREATE TABLE [dbo].[orderItems](
            [id] BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
            [order_id] BIGINT,
            [product_id] BIGINT,
            [qty] INT,
            [price] FLOAT,
            [sum] FLOAT,
            [discount] FLOAT,
            [total] FLOAT,
            [notes] VARCHAR(250)
        )
        """,

        # 6. orderPayments
        """
        IF OBJECT_ID('orderPayments', 'U') IS NULL
        CREATE TABLE orderPayments (
            orderId BIGINT NOT NULL FOREIGN KEY REFERENCES Orders(id),
            paymentDate DATE,
            channel NVARCHAR(50),
            amount FLOAT
        )
        """,

        # 7. warehouses
        """
        IF OBJECT_ID('warehouses', 'U') IS NULL
        CREATE TABLE [dbo].[warehouses](
            [id] BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
            [warehouseName] NVARCHAR(100) NOT NULL UNIQUE,
            [address] NVARCHAR(250)
        )
        """,

        # 8. inventory
        """
        IF OBJECT_ID('inventory', 'U') IS NULL
        CREATE TABLE [dbo].[inventory](
            [id] BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
            [productId] BIGINT,
            [warehouseId] BIGINT,
            [qty] DECIMAL(18, 6),
            [min_qty] DECIMAL(18, 6),
            CONSTRAINT [UK_inventory_productId_warehouseId] UNIQUE (productId, warehouseId)
        )
        """,

        # 9. suppliers
        """
        IF OBJECT_ID('suppliers', 'U') IS NULL
        CREATE TABLE suppliers (
            id INT IDENTITY(1,1) PRIMARY KEY,
            companyName NVARCHAR(150) NOT NULL,
            address NVARCHAR(500),
            contact NVARCHAR(150),
            phone NVARCHAR(50),
            email NVARCHAR(100),
            bank NVARCHAR(150),
            account NVARCHAR(50)
        )
        """,

        # 10. purchaseOrders
        """
        IF OBJECT_ID('purchaseOrders', 'U') IS NULL
        CREATE TABLE [dbo].[purchaseOrders](
            [id] BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
            [poNumber] NVARCHAR(50) NOT NULL UNIQUE,
            [poDate] DATE NOT NULL,
            [supplierId] BIGINT NOT NULL,
            [warehouseId] BIGINT,
            [requestedDeliveryDate] DATE,
            [actualDeliveryDate] DATE,
            [discount] FLOAT DEFAULT(0),
            [shippingCost] FLOAT DEFAULT(0),
            [taxAmount] FLOAT DEFAULT(0),
            [totalAmount] FLOAT NOT NULL,
            [status] NVARCHAR(50) DEFAULT('Draft'),
            [notes] NVARCHAR(MAX),
            [createdDate] DATETIME2 DEFAULT(GETDATE()),
            [modifiedDate] DATETIME2 DEFAULT(GETDATE()),
            [createdBy] NVARCHAR(100),
            [approvedBy] NVARCHAR(100),
            [approvedDate] DATETIME2
        )
        """,

        # 11. purchaseOrderItems
        """
        IF OBJECT_ID('purchaseOrderItems', 'U') IS NULL
        CREATE TABLE [dbo].[purchaseOrderItems](
            [id] BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
            [purchaseOrderId] BIGINT NOT NULL,
            [productId] BIGINT NOT NULL,
            [qty] INT NOT NULL,
            [unitPrice] FLOAT NOT NULL,
            [lineTotal] FLOAT NOT NULL,
            [discount] FLOAT DEFAULT(0),
            [receivedQty] INT DEFAULT(0),
            [notes] NVARCHAR(250)
        )
        """
    ]
    # Step 5: Execute each statement
    for i, ddl in enumerate(ddl_statements, 1):
        print(f"Executing DDL statement {i}...")
        try:
            stmt.execute(ddl)
            print(f"Statement {i} executed successfully.")
        except Exception as e:
            print(f"Error in statement {i}: {e}")

    # Step 6: Cleanup
    stmt.close()
    conn.close()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Write the sprak script to read the data from Data_LH lake house files section (products, productcategories CSV files) and insert into products, productcategories tables of Test_Azure_SQL_DB_CDC database.

# CELL ********************

from pyspark.sql import SparkSession

def seed_productcategories(spark: SparkSession) -> int:
    """
    Seeds the 'productcategories' table from a CSV file if not already populated.

    Args:
        spark (SparkSession): Spark session object.

    Returns:
        int: Number of records inserted into the table.
    """

    # JDBC connection config
    jdbc_url = "jdbc:sqlserver://dataandai-celestial.database.windows.net:1433;database=Test_Azure_SQL_DB_CDC"
    connection_properties = {
        "user": "celestial-sa",
        "password": "srGnyE%g8(95",
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }

    # Check if table already has data
    existing_df = spark.read.jdbc(url=jdbc_url, table="productcategories", properties=connection_properties)
    if not existing_df.rdd.isEmpty():
        print("productcategories already seeded.")
        return existing_df.count()

    # Read CSV from Lakehouse file path
    csv_path = "Files/product_categories.csv"
    categories_df = spark.read.option("header", True).csv(csv_path)

    # Write to productcategories table via JDBC
    try:
        categories_df.write.jdbc(url=jdbc_url, table="productcategories", mode="append", properties=connection_properties)
        print(f"Inserted {categories_df.count()} records into productcategories.")
        return categories_df.count()
    except Exception as e:
        print(f"Error inserting into productcategories: {str(e)}")
        return -1
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pyspark.sql.functions as F

def seed_products(spark: SparkSession) -> int:
    """
    Seeds the 'products' table from a CSV file using JDBC and PySpark.

    Args:
        spark (SparkSession): Active Spark session.

    Returns:
        int: Number of records successfully inserted.
    """

    # JDBC connection details
    jdbc_url = "jdbc:sqlserver://dataandai-celestial.database.windows.net:1433;database=Test_Azure_SQL_DB_CDC"
    connection_properties = {
        "user": "celestial-sa",
        "password": "srGnyE%g8(95",
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }

    # Check if data already exists in 'products' table
    existing_df = spark.read.jdbc(url=jdbc_url, table="products", properties=connection_properties)
    if not existing_df.rdd.isEmpty():
        print("Products table already seeded.")
        return existing_df.count()

    # Read CSV from Lakehouse Files (handle multiline, quotes, etc.)
    products_df = spark.read.option("header", True) \
                            .option("multiLine", True) \
                            .option("quote", '"') \
                            .option("escape", '"') \
                            .csv("Files/products.csv")

    # Drop unnecessary columns
    columns_to_drop = ['isBestSeller', 'boughtInLastMonth']
    for col_name in columns_to_drop:
        if col_name in products_df.columns:
            products_df = products_df.drop(col_name)

    # Convert and clean numeric columns
    numeric_columns = ['price', 'rating', 'listPrice', 'category_id']
    for col_name in numeric_columns:
        if col_name in products_df.columns:
            products_df = products_df.withColumn(col_name, F.col(col_name).cast("double"))

    # Drop rows with null price or category_id (required fields)
    if 'price' in products_df.columns and 'category_id' in products_df.columns:
        products_df = products_df.dropna(subset=['price', 'category_id'])

    # Fill other float fields with 0 where needed
    products_df = products_df.fillna({'listPrice': 0.0, 'rating': 0.0})

    # Write to SQL Server via JDBC
    try:
        products_df.write.jdbc(url=jdbc_url, table="products", mode="append", properties=connection_properties)
        print(f"Inserted {products_df.count()} records into products.")
        return products_df.count()
    except Exception as e:
        print("❌ Error inserting into products table:", str(e))
        return -1


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Generate and seed realistic test data including inventory, suppliers, warehouses, payments, and purchase orders using Faker/Mimesis with locale-based support based on sales and inventory insights

# CELL ********************

import configparser
from dis import show_code
from pickle import PERSID
import random
import datetime
from warnings import catch_warnings
from faker import Faker
#import config
#from db import create_tables, create_connection, close_connection
import random
from pyspark.sql import SparkSession
#from config import Config

# Regions = { 
#   "Asia": ['hi_IN', 'zh_CN', 'ja_JP', 'ko_KR', 'th_TH', 'ru_RU'],
#   "Middle East": ['ar_AA', 'ar_EG', 'ar_JO', 'ar_PS', 'ar_SA'],
#   "Europe": ['da_DK', 'de_AT', 'de_CH', 'de_DE', 'dk_DK', 'el_GR', 'es_ES', 'fr_BE', 'fr_CA', 'fr_CH','fr_FR', 'fr_QC', 'it_CH', 'it_IT', 'nl_BE', 'nl_NL', 'no_NO', 'pl_PL', 'pt_PT', 'ro_RO', 'sk_SK', 'sl_SI', 'sv_SE'],
#   "LatAm": ['es_AR', 'es_BO', 'es_CL', 'es_CO', 'es_CR', 'es_DO', 'es_EC', 'es_ES'],
#   "Africa": ['ar_AA', 'ar_EG', 'ar_JO', 'ar_PS', 'ar_SA', 'tw_GH'],
#   "East Asia": ['zh_CN', 'zh_TW', 'ja_JP', 'ko_KR', 'th_TH'],
#   "East Europe": ['bg_BG', 'cs_CZ', 'uk_UA', 'ru_RU', 'ro_RO', 'pl_PL', 'sk_SK', 'sl_SI']
# }


# Define supported regions and locales for Faker data
Regions = {
    "Asia": ['hi_IN', 'zh_CN', 'ja_JP', 'ko_KR', 'th_TH'],
    "Middle East": ['ar_EG', 'ar_SA'],
    "Europe": ['de_DE', 'en_GB', 'es_ES', 'fr_FR', 'it_IT', 'nl_NL', 'pl_PL', 'pt_PT', 'ro_RO', 'ru_RU'],
    "LatAm": ['es_CO', 'es_MX', 'es_ES'],  #  Removed es_BO and others not supported
    "Africa": ['en_ZA', 'fr_FR'],  # limited support
    "East Asia": ['zh_CN', 'ja_JP', 'ko_KR'],
    "East Europe": ['cs_CZ', 'pl_PL', 'ro_RO', 'ru_RU', 'uk_UA']
}
# Create Faker instance based on region in Config, or use default if not defined
if not Config.Region:
    fake = Faker()
else:
    fake = Faker(Regions[Config.Region])

# List of all possible locales supported by Faker
faker_locales = [
        'ar_AA', 'ar_EG', 'ar_JO', 'ar_PS', 'ar_SA', 'bg_BG', 'bs_BA', 'cs_CZ', 'da_DK', 'de_AT', 
        'de_CH', 'de_DE', 'dk_DK', 'el_CY', 'el_GR', 'en_AU', 'en_CA', 'en_GB', 'en_IE', 'en_IN', 
        'en_NZ', 'en_PH', 'en_TH', 'en_US', 'es_AR', 'es_BO', 'es_CL', 'es_CO', 'es_CR', 'es_DO',
        'es_EC', 'es_ES', 'es_GT', 'es_HN', 'es_MX', 'es_NI', 'es_PA', 'es_PE', 'es_PR', 'es_PY', 
        'es_SV', 'es_UY', 'es_VE', 'et_EE', 'fa_IR', 'fi_FI', 'fr_BE', 'fr_CA', 'fr_CH', 'fr_FR', 
        'fr_QC', 'ga_IE', 'he_IL', 'hi_IN', 'hr_HR', 'hu_HU', 'hy_AM', 'id_ID', 'is_IS', 'it_CH', 
        'it_IT', 'ja_JP', 'ka_GE', 'ko_KR', 'lb_LU', 'lt_LT', 'lv_LV', 'mt_MT', 'ne_NP', 'nl_BE', 
        'nl_NL', 'no_NO', 'or_IN', 'pl_PL', 'pt_BR', 'pt_PT', 'ro_RO', 'ru_RU', 'sk_SK', 'sl_SI', 
        'sq_AL', 'sv_SE', 'ta_IN', 'th_TH', 'tr_TR', 'tw_GH', 'uk_UA', 'vi_VN', 'zh_CN', 'zh_TW'
    ]

# Function to print sample data for random locales
def test_faker_locales():
    """Test function to show all Faker locales."""
    print("Available Faker locales:")
    #for locale in faker_locales:
    #    show_faker_locale(locale)
    locales = ['es_ES', 'pt_PT', 'pt_BR', 'hi_IN']  #we can define region here 
    # like 
    # Asia: ['hi_IN', 'zh_CN', 'ja_JP', 'ko_KR', 'th_TH', 'ru_RU']
    # Middle_east = ['ar_AA', 'ar_EG', 'ar_JO', 'ar_PS', 'ar_SA']
    # Europe: ['da_DK', 'de_AT', 'de_CH', 'de_DE', 'dk_DK', 'el_GR', 'es_ES', 'fr_BE', 'fr_CA', 'fr_CH','fr_FR', 'fr_QC', 'it_CH', 'it_IT', 'nl_BE', 'nl_NL', 'no_NO', 'pl_PL', 'pt_PT', 'ro_RO', 'sk_SK', 'sl_SI', 'sv_SE']
    # LATAM: ['es_AR', 'es_BO', 'es_CL', 'es_CO', 'es_CR', 'es_DO', 'es_EC', 'es_ES']
    # Africa: ['ar_AA', 'ar_EG', 'ar_JO', 'ar_PS', 'ar_SA', 'tw_GH']
    # East Asia: ['zh_CN', 'zh_TW', 'ja_JP', 'ko_KR', 'th_TH']
    # East Europe: ['bg_BG', 'cs_CZ', 'uk_UA', 'ru_RU', 'ro_RO', 'pl_PL', 'sk_SK', 'sl_SI']

    for _ in range(1,20):
        print("")
        locale = random.sample(locales,1)
        print(f"Locale: {locale}")
        fake = Faker(locale)
        print(f"Example Male Name: {fake.first_name_male()}")
        print(f"Example Female Name: {fake.first_name_female()}")
        print(f"Example Address: {fake.address()}")
        
    return

# Function to show sample data from all Mimesis locales
def test_mimesis():
    """Test function to show all Mimesis locales."""
    from mimesis import Generic
    from mimesis.locales import Locale  # Import the Locale enumeration

    generic = Generic()
    print("Available Mimesis locales:")
    locales = [locale.value for locale in Locale]  # Extract locale values from the enumeration
    print(locales)
    for locale in locales:
        print(locale)
        generic = Generic(locale = locale ,seed = 2) 
        try:
            person = generic.person
            print(f"Example Name: {person.full_name()}")
            addr = generic.address
            print(f"Example Address: {addr.address()}")
            print(f"Example city: {addr.city()}")
            print(f"Example country: {addr.country()}")
            print(f"Example zip: {addr.postal_code()}")
            print(f"Example Email: {person.email()}")
        except Exception as e:
            print(f"Warning for locale {locale}: {e}")
    return

# Function to show specific Faker locale sample data
def show_faker_locale(locale):
    """Show Faker locale details."""
    if locale in faker_locales:
        try:
            print("")
            fake = Faker(locale)
            
            print(f"Locale: {locale}")
            print(f"****Locale: {locale}")
            print(f"Example Name: {fake.name()}")

            print(f"Example Male Name: {fake.first_name_male()}")
            print(f"Example Female Name: {fake.first_name_female()}")

            print(f"Example Address: {fake.address()}")
            print(f"Example Email: {fake.email()}")
            try:
                print(f"Example Phone: {fake.phone_number()}")
            except AttributeError:
                print(f"Example Phone: --- not available ---")
        except Exception as e:
            print(f"Warning for locale {locale}: {e}")
        
        # Try different phone number methods
        
    else:
        print(f"Locale '{locale}' not found in Faker locales.")
    return


# Global variable to cache product list
all_products = []

def cache_all_products(spark):
    global all_products
    if not all_products:
        query = "SELECT id, listPrice FROM products"
        if Config.inStockOnly:
            query += " WHERE listPrice > 0"

        df = spark.read \
            .format("jdbc") \
            .option("url", Config.JDBC_URL) \
            .option("query", query) \
            .option("user", Config.DB_USER) \
            .option("password", Config.DB_PASSWORD) \
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
            .load()

        # Collect as list of Row objects
        all_products = df.select("id", "listPrice").collect()

# List of major banks
banks = ["JPMorgan Chase Bank", "Bank of America", "Wells Fargo Bank", "Citibank", "U.S. Bank", "PNC Bank", 
         "Goldman Sachs Bank", "Truist Bank", "Capital One Bank", "TD Bank", "Bank of New York Mellon", 
         "USAA Bank", "Charles Schwab Bank", "American Express Bank", "HSBC Bank USA", "Fifth Third Bank", 
         "Regions Bank", "KeyBank", "Huntington National Bank", "M&T Bank", "Citizens Bank", "BMO Harris Bank", 
         "Ally Bank", "Discover Bank", "Navy Federal Credit Union", "State Farm Bank", "BBVA USA", 
         "SunTrust Bank", "Branch Banking and Trust", "First National Bank", "Santander Bank", 
         "Union Bank", "Comerica Bank", "Zions Bank", "First Citizens Bank", "Associated Bank", "TCF Bank", 
         "Synovus Bank", "BankUnited", "Popular Bank", "Royal Bank of Canada", "Toronto-Dominion Bank", 
         "Bank of Nova Scotia", "Bank of Montreal", "Canadian Imperial Bank of Commerce", "National Bank of Canada", 
         "Desjardins Bank", "Laurentian Bank of Canada", "Canadian Western Bank", "Banco Nacional de Mexico"]

# Function to generate ZIP code based on U.S. state abbreviation
def zipcode_for_state(state_abbr):
    state_zip_ranges = {
    "AL": (35004, 36925),
    "AK": (99501, 99950),
    "AZ": (85001, 86556),
    "AR": (71601, 72959),
    "CA": (90001, 96162),
    "CO": (80001, 81658),
    "CT": (6001, 6389),
    "DE": (19701, 19980),
    "FL": (32004, 34997),
    "GA": (30001, 31999),
    "HI": (96701, 96898),
    "ID": (83201, 83876),
    "IL": (60001, 62999),
    "IN": (46001, 47997),
    "IA": (50001, 52809),
    "KS": (66002, 67954),
    "KY": (40003, 42788),
    "LA": (70001, 71497),
    "ME": (3901, 4992),
    "MD": (20601, 21930),
    "MA": (1001, 2791),
    "MI": (48001, 49971),
    "MN": (55001, 56763),
    "MS": (38601, 39776),
    "MO": (63001, 65899),
    "MT": (59001, 59937),
    "NE": (68001, 69367),
    "NV": (88901, 89883),
    "NH": (3031, 3897),
    "NJ": (7001, 8989),
    "NM": (87001, 88439),
    "NY": (10001, 14925),
    "NC": (27006, 28909),
    "ND": (58001, 58856),
    "OH": (43001, 45999),
    "OK": (73001, 74966),
    "OR": (97001, 97920),
    "PA": (15001, 19640),
    "RI": (2801, 2940),
    "SC": (29001, 29945),
    "SD": (57001, 57799),
    "TN": (37010, 38589),
    "TX": (75001, 88595),
    "UT": (84001, 84791),
    "VT": (5001, 5907),
    "VA": (20101, 24658),
    "WA": (98001, 99403),
    "WV": (24701, 26886),
    "WI": (53001, 54990),
    "WY": (82001, 83414),
    # DC and territories
    "DC": (20001, 20039),
    "AS": (96799, 96799),
    "GU": (96910, 96932),
    "MP": (96950, 96952),
    "PR": (600, 799),
    "VI": (801, 851),
    }
    if state_abbr in state_zip_ranges:
        start, end = state_zip_ranges[state_abbr]
        return f"{random.randint(start, end):05d}"
    else:
        return fake.postcode()
# Area codes mapped to U.S. states  
state_area_codes = {
    "AL": [205, 251, 256, 334, 938],
    "AK": [907],
    "AZ": [480, 520, 602, 623, 928],
    "AR": [479, 501, 870],
    "CA": [209, 213, 310, 323, 408, 415, 424, 442, 510, 530, 559, 562, 619, 626, 650, 657, 661, 707, 714, 747, 760, 805, 818, 820, 831, 858, 909, 916, 925, 949, 951],
    "CO": [303, 719, 720, 970],
    "CT": [203, 475, 860, 959],
    "DE": [302],
    "FL": [305, 321, 352, 386, 407, 561, 727, 754, 772, 786, 813, 850, 863, 904, 941, 954],
    "GA": [229, 404, 470, 478, 678, 706, 762, 770, 912],
    "HI": [808],
    "ID": [208, 986],
    "IL": [217, 224, 309, 312, 331, 447, 464, 618, 630, 708, 773, 779, 815, 847, 872],
    "IN": [219, 260, 317, 463, 574, 765, 812, 930],
    "IA": [319, 515, 563, 641, 712],
    "KS": [316, 620, 785, 913],
    "KY": [270, 364, 502, 606, 859],
    "LA": [225, 318, 337, 504, 985],
    "ME": [207],
    "MD": [240, 301, 410, 443, 667],
    "MA": [339, 351, 413, 508, 617, 774, 781, 857, 978],
    "MI": [231, 248, 269, 313, 517, 586, 616, 734, 810, 906, 947, 989],
    "MN": [218, 320, 507, 612, 651, 763, 952],
    "MS": [228, 601, 662, 769],
    "MO": [314, 417, 557, 573, 636, 660, 816, 975],
    "MT": [406],
    "NE": [308, 402, 531],
    "NV": [702, 725, 775],
    "NH": [603],
    "NJ": [201, 551, 609, 732, 848, 856, 862, 908, 973],
    "NM": [505, 575],
    "NY": [212, 315, 332, 347, 516, 518, 585, 607, 631, 646, 680, 716, 718, 838, 845, 914, 917, 929, 934],
    "NC": [252, 336, 704, 743, 828, 910, 919, 980, 984],
    "ND": [701],
    "OH": [216, 220, 234, 283, 326, 330, 380, 419, 440, 513, 567, 614, 740, 937],
    "OK": [405, 539, 580, 918],
    "OR": [458, 503, 541, 971],
    "PA": [215, 223, 267, 272, 412, 445, 484, 570, 582, 610, 717, 724, 814, 835, 878],
    "RI": [401],
    "SC": [803, 839, 843, 854, 864],
    "SD": [605],
    "TN": [423, 615, 629, 731, 865, 901, 931],
    "TX": [210, 214, 254, 281, 325, 346, 361, 409, 430, 432, 469, 512, 682, 713, 726, 737, 806, 817, 830, 832, 903, 915, 936, 940, 945, 956, 972, 979],
    "UT": [385, 435, 801],
    "VT": [802],
    "VA": [276, 434, 540, 571, 703, 757, 804],
    "WA": [206, 253, 360, 425, 509, 564],
    "WV": [304, 681],
    "WI": [262, 274, 414, 534, 608, 715, 920],
    "WY": [307],
    "DC": [202],
    "AS": [684],
    "GU": [671],
    "MP": [670],
    "PR": [787, 939],
    "VI": [340],
}

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType
import datetime
import random

def phone_prefix_for_state(state_abbr):
    codes = state_area_codes.get(state_abbr)
    if codes:
        return str(random.choice(codes))
    else:
        return str(random.randint(200, 999)) 


from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType
import datetime

def seed_customers(spark: SparkSession, count: int):
    """
    Seeds the 'customers' table with fake data using JDBC (no batching).
    
    Args:
        spark (SparkSession): Spark session.
        count (int): Number of customers to insert.
    
    Returns:
        int: Number of inserted customers
    """

    # JDBC setup
    jdbc_url = "jdbc:sqlserver://dataandai-celestial.database.windows.net:1433;database=Test_Azure_SQL_DB_CDC"
    connection_properties = {
        "user": "celestial-sa",
        "password": "srGnyE%g8(95",
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }

    all_data = []

    for _ in range(count):
        c = generate_new_customer()
        dob_parts = c["dateOfBirth"].split("-")
        dob = datetime.date(int(dob_parts[0]), int(dob_parts[1]), int(dob_parts[2]))
        all_data.append((
            c["firstName"][:50],
            c["middleName"][:50],
            c["lastName"][:50],
            c["company"][:150],
            dob,
            c["email"][:50],
            c["phone"][:50],
            c["address"][:150],
            c["city"][:50],
            c["state"][:2],
            c["zip"][:10]
        ))

    if not all_data:
        print("❌ No customer data generated.")
        return 0

    # Define schema for DataFrame
    schema = StructType([
        StructField("firstName", StringType(), True),
        StructField("middleName", StringType(), True),
        StructField("lastName", StringType(), True),
        StructField("company", StringType(), True),
        StructField("DOB", DateType(), True),
        StructField("email", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("address", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("zip", StringType(), True)
    ])

    # Create DataFrame
    customer_df = spark.createDataFrame(all_data, schema=schema)

    # Write to SQL Server using JDBC
    customer_df.write.jdbc(url=jdbc_url, table="customers", mode="append", properties=connection_properties)

    print(f"✅ Inserted {len(all_data)} customer records into 'customers' table.")
    return len(all_data)





# Get list of unpaid orders with optional date filter
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col

def get_unpaid_orders(spark: SparkSession, get_first_row: bool = False, date=None):
    """
    Returns list of unpaid orders using Spark with JDBC connection.

    Args:
        spark: SparkSession
        get_first_row: Whether to return only the first unpaid order
        date: Optional cutoff date for filtering orders

    Returns:
        List[Dict]: Each item is {id, orderDate, diff}
    """
    jdbc_url = "jdbc:sqlserver://dataandai-celestial.database.windows.net:1433;database=Test_Azure_SQL_DB_CDC"
    connection_properties = {
        "user": "celestial-sa",
        "password": "srGnyE%g8(95",
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }

    # Read orders and payments
    orders_df = spark.read.jdbc(url=jdbc_url, table="orders", properties=connection_properties).alias("o")
    payments_df = spark.read.jdbc(url=jdbc_url, table="orderPayments", properties=connection_properties).alias("op")

    # Aggregate payment sum per order
    payments_agg = payments_df.groupBy("orderId").agg(F.sum("amount").alias("samount"))

    # Join orders with payments
    joined_df = orders_df.join(
        payments_agg,
        orders_df["id"] == payments_agg["orderId"],
        "left_outer"
    ).withColumn(
        "diff", col("orderSum") - F.coalesce(col("samount"), F.lit(0))
    )

    # Filter unpaid orders
    filtered_df = joined_df.filter(col("diff") >= 0.01)

    if date:
        filtered_df = filtered_df.filter(col("orderDate") <= F.lit(str(date)))

    # Select required columns and sort
    final_df = filtered_df.select("id", "orderDate", "diff").orderBy("orderDate")

    # If only first row is needed
    if get_first_row:
        row = final_df.limit(1).collect()
        return [{"id": r["id"], "orderDate": r["orderDate"], "diff": r["diff"]} for r in row]

    # Return full result as list of dicts
    return [{"id": r["id"], "orderDate": r["orderDate"], "diff": r["diff"]} for r in final_df.collect()]



def generate_new_customer():
    """
    Generates one fake customer dictionary with name, address, phone, etc.

    Returns:
        dict: Customer data with keys matching DB columns
    """
    # Decide gender randomly
    is_female = random.random() < 0.52  # 52% chance female

    # First and middle name generation
    if is_female:
        first_name = fake.first_name_female()
    else:
        first_name = fake.first_name_male()

    # 75% chance to have a middle name
    if random.random() < 0.75:
        middle_name = fake.first_name_female() if is_female else fake.first_name_male()
        while middle_name == first_name:
            middle_name = fake.first_name_female() if is_female else fake.first_name_male()
    else:
        middle_name = ""

    last_name = fake.last_name()

    # Generate address and location
    address = fake.street_address()
    city = fake.city()
    state = fake.state_abbr()
    zip_code = zipcode_for_state(state)
    phone_prefix = phone_prefix_for_state(state)
    phone = f"({phone_prefix}){random.randint(100, 999)}-{random.randint(1000, 9999)}"

    # Safe email domain
    email_local = fake.user_name()
    domain = fake.domain_name(levels=random.randint(1, 2))
    email = f"{email_local}@{domain}"

    # Random date of birth between 1940 and 2007
    year = random.randint(1940, 2007)
    month = random.randint(1, 12)
    day = random.randint(1, 28)  # Simple valid day for all months
    dob = f"{year:04d}-{month:02d}-{day:02d}"

    return {
        "firstName": first_name[:50],
        "middleName": middle_name[:50],
        "lastName": last_name[:50],
        "dateOfBirth": dob,
        "company": fake.company()[:150],
        "email": email[:50],
        "phone": phone[:50],
        "address": address[:150],
        "city": city[:50],
        "state": state[:2],
        "zip": zip_code[:10]
    }



def executeSQL(spark, strSQL, fetch_results=True):
    """
    Executes a SQL statement via Spark JDBC and optionally fetches results.
    
    Args:
        spark: The active SparkSession
        strSQL: SQL query to execute (SELECT or DML)
        fetch_results: Whether to return results (True for SELECT)
    
    Returns:
        List of Row objects for SELECT queries, None for DML queries
    """
    try:
        if fetch_results:
            df = spark.read \
                .format("jdbc") \
                .option("url", Config.JDBC_URL) \
                .option("query", strSQL) \
                .option("user", Config.DB_USER) \
                .option("password", Config.DB_PASSWORD) \
                .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
                .load()
            return df.collect()
        else:
            # For INSERT, UPDATE, DELETE: use spark.sql if needed (or native Python DB connector)
            spark.sql(f"-- Execute SQL\n{strSQL}")
            return None
    except Exception as e:
        print(f"Error executing SQL '{strSQL}': {str(e)}")
        return False


# Returns a random list of product IDs and quantities for an order
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType
import datetime
import random

def products_for_order(productsCount=2, productQuantityLimit=2):
    global all_products
    selected = random.sample(all_products, random.randint(1, min(productsCount, len(all_products))))
    return [
        {
            "product": row["id"],
            "qty": random.randint(1, productQuantityLimit),
            "listPrice": round(row["listPrice"], 2)
        }
        for row in selected
    ]


def generate_order(customer_id, productsCount=2, productQuantityLimit=3):
    products = products_for_order(productsCount, productQuantityLimit)
    return {"customer_id": customer_id, "products": products}



from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
import datetime
import random

from pyspark.sql import SparkSession
from pyspark.sql.types import *
import random, datetime

def seed_orders(
    spark: SparkSession,
    customers_to_seed: int,
    days: int,
    generate_sequentially: bool,
    max_products_per_order: int,
    some_value,
    max_orders_per_customer: int
) -> int:
    """
    Seeds the 'orders' and 'orderItems' tables using Spark JDBC (no batching).
    """

    cache_all_products(spark)  # Cache product list

    jdbc_url = Config.JDBC_URL
    connection_properties = {
        "user": Config.DB_USER,
        "password": Config.DB_PASSWORD,
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }

    # Fetch customers
    customer_df = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("query", f"SELECT TOP {customers_to_seed} id FROM customers ORDER BY NEWID()") \
        .option("user", Config.DB_USER) \
        .option("password", Config.DB_PASSWORD) \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .load()

    customer_ids = [row["id"] for row in customer_df.collect()]

    # Generate base date
    order_date_str = Config.ORDER_GENERATION_DATE
    yr, mn, dy = map(int, order_date_str.split("-"))
    base_date = datetime.date(yr, mn, dy)

    # Get starting order ID
    max_id_df = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("query", "SELECT ISNULL(MAX(id), 0) AS max_id FROM orders") \
        .option("user", Config.DB_USER) \
        .option("password", Config.DB_PASSWORD) \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .load()

    next_order_id = max_id_df.collect()[0]["max_id"] + 1

    orders = []
    order_items = []

    probability_of_multiple_orders = 0.1

    for customer_id in customer_ids:
        order_count = 0
        while True:
            order = generate_order(customer_id, max_products_per_order)
            order_total = sum(item["listPrice"] * item["qty"] for item in order["products"])
            order_number = f"{order_date_str}/{next_order_id}"

            orders.append((
                order_number,
                base_date,
                customer_id,
                0,  # discount
                '{"Created"}',  # statuses
                order_total,
                ""  # notes
            ))

            for item in order["products"]:
                total_price = item["listPrice"] * item["qty"]
                order_items.append((
                    next_order_id,
                    item["product"],
                    item["qty"],
                    item["listPrice"],
                    total_price,
                    0.0,  # discount
                    total_price,  # total
                    ""  # notes
                ))

            next_order_id += 1
            order_count += 1

            if order_count < max_orders_per_customer and random.random() < probability_of_multiple_orders:
                continue
            else:
                break

    # Define schemas
    orders_schema = StructType([
        StructField("orderNumber", StringType(), False),
        StructField("orderDate", DateType(), False),
        StructField("customerId", IntegerType(), False),
        StructField("discount", IntegerType(), False),
        StructField("statuses", StringType(), False),
        StructField("orderSum", DoubleType(), False),
        StructField("notes", StringType(), True),
    ])

    order_items_schema = StructType([
        StructField("order_id", IntegerType(), False),
        StructField("product_id", IntegerType(), False),
        StructField("qty", IntegerType(), False),
        StructField("price", DoubleType(), False),
        StructField("sum", DoubleType(), False),
        StructField("discount", DoubleType(), False),
        StructField("total", DoubleType(), False),
        StructField("notes", StringType(), True)
    ])

    # Insert all orders
    spark.createDataFrame(orders, schema=orders_schema) \
        .write.jdbc(url=jdbc_url, table="orders", mode="append", properties=connection_properties)
    print(f"✅ Inserted {len(orders)} orders.")

    # Insert all order items
    spark.createDataFrame(order_items, schema=order_items_schema) \
        .write.jdbc(url=jdbc_url, table="orderItems", mode="append", properties=connection_properties)
    print(f"✅ Inserted {len(order_items)} order items.")

    return len(orders)




# Returns the min and max order ID (optionally filtered by date)
def get_last_order_id(spark: SparkSession, by_date=True):
    jdbc_url = "jdbc:sqlserver://dataandai-celestial.database.windows.net:1433;database=Test_Azure_SQL_DB_CDC"
    connection_properties = {
        "user": "celestial-sa",
        "password": "srGnyE%g8(95",
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }

    orders_df = spark.read.jdbc(url=jdbc_url, table="orders", properties=connection_properties)
    
    if by_date:
        filtered = orders_df.filter(orders_df["orderDate"] == Config.ORDER_GENERATION_DATE)
    else:
        filtered = orders_df

    if filtered.count() == 0:
        return 0, 0

    result = filtered.agg({"id": "min", "id": "max"}).collect()[0]
    return result["min(id)"], result["max(id)"]


# Returns a list of existing and optionally new customer IDs for order seeding
def customers_list_for_orders(spark: SparkSession, customerCount=10, newPercentage=10):
    jdbc_url = "jdbc:sqlserver://dataandai-celestial.database.windows.net:1433;database=Test_Azure_SQL_DB_CDC"
    connection_properties = {
        "user": "celestial-sa",
        "password": "srGnyE%g8(95",
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }

    # Load customers from DB
    customers_df = spark.read.jdbc(url=jdbc_url, table="customers", properties=connection_properties)
    customer_ids = [row["id"] for row in customers_df.select("id").collect()]
    
    total_existing = len(customer_ids)
    selected_customers = []
    new_customers_count = int(customerCount * newPercentage / 100)

    # Adjust count based on new customer requirement
    if total_existing > 0:
        existing_needed = max(0, customerCount - new_customers_count)
        selected_customers = random.sample(customer_ids, min(existing_needed, total_existing))

    # Create new customers if needed
    if new_customers_count > 0:
        new_customers = seed_customers(new_customers_count)
        print(f"Generated {new_customers_count} new customers.")
        selected_customers.extend(new_customers)
    else:
        print("No new customers generated.")

    return selected_customers


# Triggers payment generation for unpaid orders
def seed_payments(spark: SparkSession):
    unpaid_orders = get_unpaid_orders(spark, get_first_row=True)
    min_date = unpaid_orders[0]["orderDate"] if unpaid_orders else datetime.date.today()

    if Config.TEST_MODE:
        generate_and_insert_payments(spark, date_from=min_date, date_to=min_date)
    else:
        generate_and_insert_payments(spark, date_from=min_date, date_to=datetime.date.today())
    
    return True


def generate_and_insert_payments(spark: SparkSession, date_from, date_to):
    paid_percentage = 0.95
    current_date = date_from

    while current_date <= date_to:
        orders = get_unpaid_orders(spark, date=current_date)
        orders_to_pay = [o for o in orders if random.random() <= paid_percentage]
        insert_payments(spark, orders_to_pay, current_date)
        current_date += datetime.timedelta(days=1)

    # Final day processing
    orders = get_unpaid_orders(spark, date=date_to)
    insert_payments(spark, orders, date_to)
    return True

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, DateType, StringType, FloatType
def insert_payments(spark: SparkSession, orders_to_pay, payment_date):
    """
    Insert orderPayments via Spark JDBC
    Args:
        spark: SparkSession object
        orders_to_pay: list of dicts with order "id" and "diff"
        payment_date: datetime.date or string in 'YYYY-MM-DD'
    
    Returns:
        True if successful
    """
    # JDBC connection details
    jdbc_url = "jdbc:sqlserver://dataandai-celestial.database.windows.net:1433;database=Test_Azure_SQL_DB_CDC"
    connection_properties = {
        "user": "celestial-sa",
        "password": "srGnyE%g8(95",
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }
    # Prepare data for Spark DataFrame
    payments_data = []
    for order in orders_to_pay:
        payments_data.append((
            order["id"],
            payment_date,
            fake.random_element(elements=("Online", "In-Store", "Mobile", "Credit Card")),
            round(order["diff"], 2)
        ))
    # Define schema
    payments_schema = StructType([
        StructField("orderId", IntegerType(), False),
        StructField("paymentDate", DateType(), False),
        StructField("channel", StringType(), False),
        StructField("amount", FloatType(), False)
    ])
    # Create Spark DataFrame
    payments_df = spark.createDataFrame(payments_data, schema=payments_schema)
    # Write to JDBC
    payments_df.write.jdbc(
        url=jdbc_url,
        table="orderPayments",
        mode="append",
        properties=connection_properties
    )
    print(f"Seeded {len(orders_to_pay)} transactions via Spark.")
    return True

from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

# JDBC connection settings (reuse as needed)
JDBC_URL = "jdbc:sqlserver://dataandai-celestial.database.windows.net:1433;database=Test_Azure_SQL_DB_CDC"
JDBC_PROPS = {
    "user": "celestial-sa",
    "password": "srGnyE%g8(95",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}
def get_record_count(spark: SparkSession, table_name: str, filter_expr: str = "1 = 1") -> int:
    df = spark.read.jdbc(url=JDBC_URL, table=table_name, properties=JDBC_PROPS)
    count = df.filter(expr(filter_expr)).count()
    return count
def get_records(spark: SparkSession, table_name: str, fields: list = None, filter_expr: str = "1 = 1"):
    df = spark.read.jdbc(url=JDBC_URL, table=table_name, properties=JDBC_PROPS)
    filtered_df = df.filter(expr(filter_expr))
    
    if fields:
        filtered_df = filtered_df.select(*fields)
    
    return filtered_df.collect()




#SPARK function for inventory
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import random

def seed_inventory(spark: SparkSession):
    # """
    # Seeds inventory data using PySpark for products not in inventory with listPrice > 0.
    # Avoids duplicates by checking existing inventory entries.
    
    # Args:
    #     spark: SparkSession object
    
    # Returns:
    #     int: Number of inventory records inserted
    # """
    # JDBC connection properties
    jdbc_url = "jdbc:sqlserver://dataandai-celestial.database.windows.net:1433;database=Test_Azure_SQL_DB_CDC"
    connection_properties = {
        "user": "celestial-sa",
        "password": "srGnyE%g8(95",
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }

    # Load products and inventory tables
    products_df = spark.read.jdbc(url=jdbc_url, table="dbo.products", properties=connection_properties).alias("products")
    inventory_df = spark.read.jdbc(url=jdbc_url, table="dbo.inventory", properties=connection_properties).alias("inventory")

    # Filter products not already in inventory and with listPrice > 0
    stock_products = (
        products_df
        .join(inventory_df, col("products.id") == col("inventory.productId"), "left_outer")
        .where(col("inventory.id").isNull() & (col("products.listPrice") > 0))
        .select(col("products.id"))
    )

    # Get warehouse IDs
    warehouses = spark.read.jdbc(url=jdbc_url, table="dbo.warehouses", properties=connection_properties).select("id").collect()
    warehouse_ids = [row["id"] for row in warehouses]

    inventory_count = 0

    if stock_products.count() == 0:
        print("No new products to seed in inventory.")
        return 0

    print(f"Found {stock_products.count()} products to seed in inventory.")

    # Collect product IDs
    product_ids = [row["id"] for row in stock_products.collect()]

    # Build inventory records
    inventory_data = []
    for product_id in product_ids:
        warehouse_id = random.choice(warehouse_ids)
        inventory_data.append({
            "productId": product_id,
            "warehouseId": warehouse_id,
            "qty": random.randint(5, 1000),
            "min_qty": 0
        })
        inventory_count += 1

    # Write to inventory table
    if inventory_data:
        batch_df = spark.createDataFrame(inventory_data, schema=["productId", "warehouseId", "qty", "min_qty"])
        batch_df.write.jdbc(url=jdbc_url, table="dbo.inventory", mode="append", properties=connection_properties)
        print(f" Inserted {inventory_count} new inventory records.")

    return inventory_count

#Function for suppliers
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
import random


def seed_suppliers(spark: SparkSession) -> int:
    """
    Seeds fake suppliers using Spark and inserts into SQL Server via JDBC in one go.
    
    Args:
        spark (SparkSession)
    
    Returns:
        int: Total number of suppliers in the table after insertion
    """
    # JDBC connection setup
    jdbc_url = "jdbc:sqlserver://dataandai-celestial.database.windows.net:1433;database=Test_Azure_SQL_DB_CDC"
    connection_properties = {
        "user": "celestial-sa",
        "password": "srGnyE%g8(95",
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }

    # Load existing supplier names to avoid duplicates
    try:
        existing_df = spark.read.jdbc(url=jdbc_url, table="suppliers", properties=connection_properties)
        existing_companies = set(existing_df.select("companyName").rdd.flatMap(lambda x: x).collect())
    except:
        existing_companies = set()

    suppliers_to_create = Config.SUPPLIERS_TO_CREATE
    new_suppliers = []

    for _ in range(suppliers_to_create):
        contact = generate_new_customer()
        company = contact["company"]

        if company in existing_companies:
            continue

        address = f"{contact['address']}\n{contact['city']}\n{contact['state']}\n{contact['zip']}"
        contact_name = f"{contact['firstName']} {' ' + contact['middleName'] if contact['middleName'] else ''} {contact['lastName']}".strip()
        bank = random.choice(banks)
        account = fake.iban()

        new_suppliers.append((
            company,
            address,
            contact_name,
            contact["phone"],
            contact["email"],
            bank,
            account
        ))
        existing_companies.add(company)  # Prevent duplicates in same run

    if not new_suppliers:
        print("✅ No new suppliers to insert. All already exist.")
        return len(existing_companies)

    # Define Spark schema
    supplier_schema = StructType([
        StructField("companyName", StringType(), False),
        StructField("address", StringType(), True),
        StructField("contact", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("email", StringType(), True),
        StructField("bank", StringType(), True),
        StructField("account", StringType(), True)
    ])

    # Create DataFrame and insert into SQL Server
    spark.createDataFrame(new_suppliers, schema=supplier_schema) \
        .write.jdbc(url=jdbc_url, table="suppliers", mode="append", properties=connection_properties)

    print(f"✅ Inserted {len(new_suppliers)} new suppliers.")
    return len(new_suppliers) + len(existing_companies)


#Function for  warehouse table
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
import random

def seed_warehouses(spark: SparkSession):
    # """
    # Seeds random warehouses into the SQL Server database using Spark JDBC.
    
    # Args:
    #     spark: SparkSession object

    # Returns:
    #     int: Number of inserted warehouse records
    # """

    # JDBC connection details
    jdbc_url = "jdbc:sqlserver://dataandai-celestial.database.windows.net:1433;database=Test_Azure_SQL_DB_CDC"
    connection_properties = {
        "user": "celestial-sa",
        "password": "srGnyE%g8(95",
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }

    # Check if warehouses already exist
    existing_df = spark.read.jdbc(url=jdbc_url, table="warehouses", properties=connection_properties)
    existing_count = existing_df.count()
    
    if existing_count > 0:
        print(f" {existing_count} warehouses already exist. Skipping seeding.")
        return existing_count

    # Generate 1 to 5 fake warehouse records
    num_warehouses = random.randint(1, 5)
    warehouse_data = [
        (f"{fake.first_name()} Warehouse", fake.address())
        for _ in range(num_warehouses)
    ]

    # Define schema
    warehouse_schema = StructType([
        StructField("warehouseName", StringType(), False),
        StructField("address", StringType(), False)
    ])

    # Create DataFrame
    warehouse_df = spark.createDataFrame(warehouse_data, schema=warehouse_schema)

    # Write to DB
    warehouse_df.write.jdbc(
        url=jdbc_url,
        table="warehouses",
        mode="append",
        properties=connection_properties
    )

    print(f"Inserted {num_warehouses} warehouses via Spark.")
    return num_warehouses

#function for purchase orders
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
import datetime
import random
def seed_purchase_orders(spark: SparkSession, po_count, days_back, product_filter="All Sold"):
    # JDBC Configuration
    JDBC_URL = "jdbc:sqlserver://dataandai-celestial.database.windows.net:1433;database=Test_Azure_SQL_DB_CDC"
    JDBC_PROPS = {
        "user": "celestial-sa",
        "password": "srGnyE%g8(95",
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }

    """
    Generate purchase orders using Spark and JDBC based on products that appear in orderItems (sold products)
    """

    # Read suppliers once and reuse
    suppliers_df = spark.read.jdbc(url=JDBC_URL, table="suppliers", properties=JDBC_PROPS)
    supplier_count = suppliers_df.count()
    if supplier_count == 0:
        print("No suppliers found. Please seed suppliers first.")
        return 0
    suppliers = suppliers_df.select("id", "companyName").collect()

    # Read warehouses once and reuse
    warehouses_df = spark.read.jdbc(url=JDBC_URL, table="warehouses", properties=JDBC_PROPS)
    warehouse_count = warehouses_df.count()
    if warehouse_count == 0:
        print("No warehouses found. Please seed warehouses first.")
        return 0
    warehouses = warehouses_df.select("id", "warehouseName").collect()

    # Load products for PO generation based on product_filter
    if product_filter == "All Sold":
        product_query = """
            (SELECT DISTINCT p.id, p.title, p.listPrice, 
                    CAST(COALESCE(i.qty, 0) as float) as current_qty, 
                    COALESCE(i.warehouseId, 1) as warehouseId,
                    COALESCE(i.min_qty, 10) as min_qty,
                    SUM(oi.qty) as total_sold,
                    COUNT(DISTINCT oi.order_id) as order_count,
                    AVG(CAST(oi.qty as float)) as avg_qty_per_order,
                    MAX(o.orderDate) as last_sold_date
            FROM products p 
            INNER JOIN orderItems oi ON p.id = oi.product_id
            INNER JOIN orders o ON oi.order_id = o.id
            LEFT JOIN inventory i ON p.id = i.productId 
            GROUP BY p.id, p.title, p.listPrice, i.qty, i.warehouseId, i.min_qty) AS product_view
        """
    elif product_filter == "Top Selling":
        product_query = """
            (WITH SalesRanking AS (
                SELECT p.id, p.title, p.listPrice, 
                       CAST(COALESCE(i.qty, 0) as float) as current_qty, 
                       COALESCE(i.warehouseId, 1) as warehouseId,
                       COALESCE(i.min_qty, 10) as min_qty,
                       SUM(oi.qty) as total_sold,
                       COUNT(DISTINCT oi.order_id) as order_count,
                       AVG(CAST(oi.qty as float)) as avg_qty_per_order,
                       MAX(o.orderDate) as last_sold_date,
                       PERCENT_RANK() OVER (ORDER BY SUM(oi.qty) DESC) as sales_rank
                FROM products p 
                INNER JOIN orderItems oi ON p.id = oi.product_id
                INNER JOIN orders o ON oi.order_id = o.id
                LEFT JOIN inventory i ON p.id = i.productId 
                GROUP BY p.id, p.title, p.listPrice, i.qty, i.warehouseId, i.min_qty
            )
            SELECT id, title, listPrice, current_qty, warehouseId, min_qty, 
                   total_sold, order_count, avg_qty_per_order, last_sold_date
            FROM SalesRanking 
            WHERE sales_rank <= 0.5) AS product_view
        """
    elif product_filter == "Recent Sales":
        product_query = """
            (SELECT DISTINCT p.id, p.title, p.listPrice, 
                   CAST(COALESCE(i.qty, 0) as float) as current_qty, 
                   COALESCE(i.warehouseId, 1) as warehouseId,
                   COALESCE(i.min_qty, 10) as min_qty,
                   SUM(oi.qty) as total_sold,
                   COUNT(DISTINCT oi.order_id) as order_count,
                   AVG(CAST(oi.qty as float)) as avg_qty_per_order,
                   MAX(o.orderDate) as last_sold_date
            FROM products p 
            INNER JOIN orderItems oi ON p.id = oi.product_id
            INNER JOIN orders o ON oi.order_id = o.id
            LEFT JOIN inventory i ON p.id = i.productId 
            WHERE o.orderDate >= DATEADD(day, -30, GETDATE())
            GROUP BY p.id, p.title, p.listPrice, i.qty, i.warehouseId, i.min_qty) AS product_view
        """
    else:
        print(f"Invalid product_filter: {product_filter}. Use 'All Sold', 'Top Selling', or 'Recent Sales'")
        return 0

    # Load filtered products once
    products_df = spark.read.jdbc(JDBC_URL, table=product_query, properties=JDBC_PROPS)
    products = products_df.collect()

    if not products:
        print(f"No products found in orderItems for filter '{product_filter}'. Please ensure orders have been seeded.")
        return 0

    print(f"Found {len(products)} sold products for PO generation with filter '{product_filter}'")

    # Insert logic placeholder
    print("Generate your PO records and use .write.jdbc() for insert. See other refactored seed functions for pattern.")

    return len(products)




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Main function

# CELL ********************

# filepath: mssql-demo-project/mssql-demo-project/src/main.py

import datetime
import random
from pyspark.sql import SparkSession
#from config import Config  # Ensure your Config file is set up for JDBC, etc.


def main():
    # Start Spark session
    spark = SparkSession.builder \
        .appName("FabricDataSeeding") \
        .config("spark.jars.packages", "com.microsoft.sqlserver:mssql-jdbc:12.6.1.jre11") \
        .getOrCreate()

    # Scenario steps to execute
    scenario = [
        "create_tables",
        "seed_productcategories",
        "seed_products",
        "seed_customers",
        "batch_seed_orders",
        "seed_orders",
        "seed_payments",
        "seed_warehouses",
        "seed_inventory",
        "seed_suppliers",
        "seed_purchase_orders"
    ]

    test_mode = Config.TEST_MODE

    try:
        if "create_tables" in scenario:
            create_tables()

        if "seed_productcategories" in scenario:
            count = seed_productcategories(spark)
            print(f"{count} product categories seeded.")

        if "seed_products" in scenario:
            count = seed_products(spark)
            print(f"{count} products seeded.")

        if "seed_customers" in scenario:
            result = executeSQL(spark, "SELECT COUNT(*) AS count FROM customers")
  
            if result and len(result) > 0:
                existing = result[0][0]
            else:
                print("Failed to get customer count. Defaulting to 0.")
                existing = 0  # Default to 0 so it proceeds to seed

            if existing is None or existing < Config.CUSTOMERS_TO_CREATE:
                if test_mode:
                    seed_customers(spark, Config.CUSTOMERS_TO_CREATE // 100)

                else:
                    seed_customers(spark, Config.CUSTOMERS_TO_CREATE)


        if "batch_seed_orders" in scenario:
            result = executeSQL(spark, "SELECT MAX(OrderDate) AS last_order_date FROM Orders")
            
            if result and result[0][0] is not None:
                last_order_date = result[0][0]
                Config.ORDER_GENERATION_DATE = last_order_date.strftime("%Y-%m-%d")
            else:
                print("Could not fetch last order date. Using default value.")
            
            dt = datetime.datetime.strptime(Config.ORDER_GENERATION_DATE, "%Y-%m-%d").date()
            iteration = 0

            while dt < datetime.date.today():
                iteration += 1
                seed_orders(
                    spark,
                    Config.CUSTOMERS_PER_DAY + int(Config.CUSTOMERS_PER_DAY * Config.CUSTOMERS_PER_DAY_VARIATION_PERCENTAGE / 100 * 2 * (random.random() - 0.5)),
                    1,
                    True,
                    Config.MAX_PRODUCTS_PER_ORDER,
                    3,
                    Config.MAX_ORDERS_PER_CUSTOMER
                )
                dt += datetime.timedelta(days=1)
                Config.ORDER_GENERATION_DATE = dt.strftime("%Y-%m-%d")

                if test_mode and iteration >= Config.ITERATIONS_IN_TEST_MODE:
                    break

        elif "seed_orders" in scenario:
            seed_orders(
                spark,
                100,
                10,
                True,
                Config.MAX_PRODUCTS_PER_ORDER,
                3,
                Config.MAX_ORDERS_PER_CUSTOMER
            )

        if "seed_payments" in scenario:
            seed_payments(spark)

        if "seed_warehouses" in scenario:
            count = seed_warehouses(spark)
            print(f"{count} warehouses seeded.")

        if "seed_inventory" in scenario:
            count = seed_inventory(spark)
            print(f"{count} inventory records seeded.")

        if "seed_suppliers" in scenario:
            count = seed_suppliers(spark)
            print(f"{count} suppliers seeded.")

        if "seed_purchase_orders" in scenario:
            count = seed_purchase_orders(spark, po_count=20, days_back=90)
            print(f"{count} purchase orders seeded.")
    
    finally:
        print("Process completed.")

if __name__ == "__main__":
    main()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pyodbc  # Library for connecting to SQL Server using ODBCsuccessful
import time 

def create_connection():
    # Define the connection string for a local SQL Server (commented out)
    # connection_string_local = (
    #     "DRIVER={ODBC Driver 18 for SQL Server};"
    #     "SERVER=localhost;"
    #     "DATABASE=test;"
    #     "Trusted_Connection=yes;"
    # )

    # Define the connection string for Azure SQL Database
    connection_string_net = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        "SERVER=dataandai-celestial.database.windows.net;"
        "DATABASE=Test_Azure_SQL_DB_CDC;"
        "UID=celestial-sa;"  # Username for SQL authentication
        "PWD=srGnyE%g8(95;"  # Password for SQL authentication
    )

    connection = None  # Initialize connection variable

    # Try to connect up to 5 times
    for attempt in range(5):
        try:
            # Use local or Azure SQL connection based on configuration
            if Config.SQL_SERVER_LOCAL:
                connection = pyodbc.connect(connection_string_local)
            else:
                connection = pyodbc.connect(connection_string_net)
            print("Connection established successfully.")
            break  # Exit loop if successful
        except pyodbc.Error as e:
            print(f"Attempt {attempt + 1}: Failed to connect. Error: {e}")
            if attempt < 4:
                print("Retrying in 5 seconds...")
                time.sleep(5)  # Wait before retrying
    else:
        raise Exception("Database connection failed after 5 attempts.")  # Raise error if all attempts fail

    return connection  # Return established connection


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def drop_all_tables(connection):
    cursor = connection.cursor()

    tables_to_drop = [
        "purchaseOrderItems",
        "purchaseOrders",
        "suppliers",
        "inventory",
        "warehouses",
        "orderPayments",
        "orderItems",
        "orders",
        "customers",
        "productcategories",
    ]
    # tables_to_drop = [
  
    #     "products"
        
    # ]
    for table in tables_to_drop:
        print(f"Dropping table if exists: {table}")
        cursor.execute(f"IF OBJECT_ID('{table}', 'U') IS NOT NULL DROP TABLE {table}")

    connection.commit()
    cursor.close()
    print("All tables dropped successfully.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# conn = create_connection()
# drop_all_tables(conn)
# close_connection(conn)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
