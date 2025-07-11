# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

!pip install faker

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# config_notebook (Microsoft Fabric Notebook)

# Define configuration values as global variables or within a class
class Config:
    ORDER_GENERATION_DATE = "2024-01-01"  # Date from which orders will be generated
    inStockOnly = True                    # Only generate orders for products that are in stock
    SQL_SERVER_LOCAL = False

    CUSTOMERS_PER_DAY = 1000              # Number of customers per day
    CUSTOMERS_PER_DAY_VARIATION_PERCENTAGE = 10  # Variation in customers/day
    CUSTOMERS_TO_CREATE = 100000          # Total number of customers to create

    MAX_ORDERS_PER_CUSTOMER = 2
    MAX_PRODUCTS_PER_ORDER = 5

    TEST_MODE = False                     # If True, run with limited customers/orders
    ITERATIONS_IN_TEST_MODE = 10
    DEFAULT_BATCH_SIZE = 1000             # Default batch size for seeding


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# --- 1. Define Spark-compatible schema for each table ---

product_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), False),
    StructField("price", IntegerType(), False),
    StructField("stock", IntegerType(), False)
])

customer_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), False),
    StructField("email", StringType(), False)
])

transaction_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("product_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("quantity", IntegerType(), False)
])

order_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("orderNumber", StringType(), False),
    StructField("orderDate", StringType(), False),
    StructField("customerId", IntegerType(), False),
    StructField("discount", IntegerType(), True),
    StructField("statuses", StringType(), True),
    StructField("orderSum", IntegerType(), False),
    StructField("notes", StringType(), True)
])

order_item_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("order_id", IntegerType(), False),
    StructField("product_id", IntegerType(), False),
    StructField("qty", IntegerType(), False),
    StructField("price", IntegerType(), False)
])


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#db_setup_notebook
# ------------------------------
#%python

import pyodbc
import time

# Directly define whether to use local or Azure SQL
USE_LOCAL_DB = False  # Set to True if connecting to localhost instead of Azure SQL

def create_connection():
    connection_string_local = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        "SERVER=localhost;"
        "DATABASE=test;"
        "Trusted_Connection=yes;"
    )
    connection_string_net = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        "SERVER=dataandai-celestial.database.windows.net;"
        "DATABASE=Test_Mirror_Fabric_Database;"
        "UID=celestial-sa;"
        "PWD=srGnyE%g8(95;"
    )

    connection = None
    for attempt in range(5):
        try:
            if USE_LOCAL_DB:
                connection = pyodbc.connect(connection_string_local)
            else:   
                connection = pyodbc.connect(connection_string_net)
            print("Connected to SQL database.")
            break
        except pyodbc.Error as e:
            print(f"Attempt {attempt + 1} failed: {e}")
            if attempt < 4:
                print("Retrying in 5 seconds...")
                time.sleep(5)
    else:
        raise Exception("Failed to connect to the database after 5 attempts.")
    return connection

def close_connection(connection):
    if connection:
        connection.close()
# Table Creation Logic
# ------------------------------
def create_tables(connection):
    cursor = connection.cursor()

    cursor.execute("""
        IF OBJECT_ID('productCategories', 'U') IS NULL
        CREATE TABLE productCategories (
            id SMALLINT NOT NULL PRIMARY KEY,
            category_name NVARCHAR(50) NOT NULL
        )
    """)

    cursor.execute("""
        IF OBJECT_ID('products', 'U') IS NULL
        CREATE TABLE products (
            id INT IDENTITY(1,1) PRIMARY KEY,
            asin NVARCHAR(15) NOT NULL,
            title NVARCHAR(800) NOT NULL,
            imgUrl NVARCHAR(250) NOT NULL,
            productURL NVARCHAR(250) NOT NULL,
            stars FLOAT NOT NULL,
            reviews NVARCHAR(50) NOT NULL,
            price FLOAT NOT NULL,
            listPrice FLOAT NOT NULL,
            category_id INT NOT NULL
        )
    """)

    cursor.execute("""
        IF OBJECT_ID('customers', 'U') IS NULL
        CREATE TABLE customers (
            id INT IDENTITY(1,1) PRIMARY KEY,
            firstName NVARCHAR(50) NOT NULL,
            middleName NVARCHAR(50),
            lastName NVARCHAR(50) NOT NULL,
            company NVARCHAR(150),
            DOB DATE NOT NULL,
            email NVARCHAR(50) NOT NULL,
            phone NVARCHAR(50),
            address NVARCHAR(150),
            city NVARCHAR(50),
            state NVARCHAR(2),
            zip NVARCHAR(10)
        )
    """)

    cursor.execute("""
        IF OBJECT_ID('orders', 'U') IS NULL
        CREATE TABLE orders (
            id BIGINT IDENTITY(1,1) PRIMARY KEY,
            orderNumber NVARCHAR(50),
            orderDate DATE NOT NULL,
            customerId BIGINT,
            discount FLOAT,
            statuses NVARCHAR(MAX),
            orderSum FLOAT,
            notes NVARCHAR(MAX)
        )
    """)

    cursor.execute("""
        IF OBJECT_ID('orderItems', 'U') IS NULL
        CREATE TABLE orderItems (
            id BIGINT IDENTITY(1,1) PRIMARY KEY,
            order_id BIGINT,
            product_id BIGINT,
            qty INT,
            price FLOAT,
            sum FLOAT,
            discount FLOAT,
            total FLOAT,
            notes VARCHAR(250)
        )
    """)

    cursor.execute("""
        IF OBJECT_ID('orderPayments', 'U') IS NULL
        CREATE TABLE orderPayments (
    orderId BIGINT NOT NULL FOREIGN KEY REFERENCES Orders(id),
    paymentDate DATE,
    channel NVARCHAR(50),
    amount FLOAT
        )
    """)

    cursor.execute("""
        IF OBJECT_ID('warehouses', 'U') IS NULL
        CREATE TABLE warehouses (
            id BIGINT IDENTITY(1,1) PRIMARY KEY,
            warehouse_name NVARCHAR(100),
            address NVARCHAR(250)
        )
    """)

    cursor.execute("""
        IF OBJECT_ID('inventory', 'U') IS NULL
        CREATE TABLE inventory (
            id BIGINT IDENTITY(1,1) PRIMARY KEY,
            productId BIGINT,
            warehouseId BIGINT,
            qty DECIMAL(18, 6),
            min_qty DECIMAL(18, 6),
            CONSTRAINT UK_inventory_productId_warehouseId UNIQUE(productId, warehouseId)
        )
    """)

    connection.commit()
    cursor.close()
    print("All tables created successfully.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ----------------------------------
# Fabric-Compatible Data Seeder Notebook
# ----------------------------------

import random
import datetime
from faker import Faker
from sqlalchemy import create_engine


# ---------------------------
# Execute SQL Utility
# ---------------------------
def executeSQL(sql):
    conn = create_connection()
    cursor = conn.cursor()
    cursor.execute(sql)
    result = cursor.fetchall()
    cursor.close()
    close_connection(conn)
    return result

# ---------------------------
# ZIP Code by State Abbreviation
# ---------------------------
def zipcode_for_state(state_abbr):
    state_zip_ranges = {
        "AL": (35004, 36925), "AK": (99501, 99950), "AZ": (85001, 86556), "AR": (71601, 72959),
        "CA": (90001, 96162), "CO": (80001, 81658), "CT": (6001, 6389), "DE": (19701, 19980),
        "FL": (32004, 34997), "GA": (30001, 31999), "HI": (96701, 96898), "ID": (83201, 83876),
        "IL": (60001, 62999), "IN": (46001, 47997), "IA": (50001, 52809), "KS": (66002, 67954),
        "KY": (40003, 42788), "LA": (70001, 71497), "ME": (3901, 4992), "MD": (20601, 21930),
        "MA": (1001, 2791), "MI": (48001, 49971), "MN": (55001, 56763), "MS": (38601, 39776),
        "MO": (63001, 65899), "MT": (59001, 59937), "NE": (68001, 69367), "NV": (88901, 89883),
        "NH": (3031, 3897), "NJ": (7001, 8989), "NM": (87001, 88439), "NY": (10001, 14925),
        "NC": (27006, 28909), "ND": (58001, 58856), "OH": (43001, 45999), "OK": (73001, 74966),
        "OR": (97001, 97920), "PA": (15001, 19640), "RI": (2801, 2940), "SC": (29001, 29945),
        "SD": (57001, 57799), "TN": (37010, 38589), "TX": (75001, 88595), "UT": (84001, 84791),
        "VT": (5001, 5907), "VA": (20101, 24658), "WA": (98001, 99403), "WV": (24701, 26886),
        "WI": (53001, 54990), "WY": (82001, 83414), "DC": (20001, 20039), "AS": (96799, 96799),
        "GU": (96910, 96932), "MP": (96950, 96952), "PR": (600, 799), "VI": (801, 851),
    }
    if state_abbr in state_zip_ranges:
        start, end = state_zip_ranges[state_abbr]
        return f"{random.randint(start, end):05d}"
    else:
        return fake.zipcode()

# ---------------------------
# Seeder Logic with Details
# ---------------------------
fake = Faker()

def generate_new_customer():
    email = fake.email()
    fname = fake.first_name()
    mname = fake.first_name() if random.random() > 0.25 else None
    lname = fake.last_name()
    address = fake.street_address()
    city = fake.city()
    state = fake.state_abbr()
    zip_code = zipcode_for_state(state)
    phone = fake.phone_number()
    dob = fake.date_of_birth(minimum_age=18, maximum_age=80)

    return (fname, mname, lname, fake.company(), dob, email, phone, address, city, state, zip_code)

def seed_customers(count=10000, batch_size=200):
    print(f"Seeding {count} customers (batch size: {batch_size})")
    conn = create_connection()
    cursor = conn.cursor()
    inserted = 0

    while inserted < count:
        batch = []
        for _ in range(min(batch_size, count - inserted)):
            batch.append(generate_new_customer())

        cursor.executemany("""
            INSERT INTO Customers (firstName, middleName, lastName, company, DOB, email, phone, address, city, state, zip)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", batch)

        conn.commit()
        inserted += len(batch)
        print(f"Inserted {inserted} customers...")

    cursor.close()
    close_connection(conn)
    print("Customers seeded.")

def seed_orders(customer_count, days=1, insert_only=True, max_products=5, warehouse_count=1, max_orders_per_customer=2):
    print("Seeding orders...")
    conn = create_connection()
    cursor = conn.cursor()

    customer_ids = [row[0] for row in executeSQL("SELECT id FROM Customers")]
    product_list = executeSQL("SELECT id, listPrice FROM Products" + (" WHERE listPrice > 0" if Config.inStockOnly else ""))
    order_date = datetime.datetime.strptime(Config.ORDER_GENERATION_DATE, "%Y-%m-%d").date()

    for _ in range(customer_count):
        customer_id = random.choice(customer_ids)
        order_count = random.randint(1, max_orders_per_customer)

        for _ in range(order_count):
            order_number = f"ORD-{random.randint(100000, 999999)}"
            discount = round(random.uniform(0, 10), 2)
            status = random.choice(["Created", "Confirmed", "Shipped"])
            notes = fake.sentence()

            order_sum = 0.0
            cursor.execute("""
                INSERT INTO Orders (orderNumber, orderDate, customerId, discount, statuses, orderSum, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (order_number, order_date, customer_id, discount, status, order_sum, notes))

            order_id = cursor.execute("SELECT @@IDENTITY").fetchval()

            items = []
            for _ in range(random.randint(1, max_products)):
                prod_id, list_price = random.choice(product_list)
                qty = random.randint(1, 5)
                total = round(qty * list_price, 2)
                order_sum += total
                items.append((order_id, prod_id, qty, list_price, total, 0, total, fake.word()))

            cursor.executemany("""
                INSERT INTO OrderItems (order_id, product_id, qty, price, sum, discount, total, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)""", items)

            cursor.execute("UPDATE Orders SET orderSum = ? WHERE id = ?", (order_sum, order_id))

        order_date += datetime.timedelta(days=1)

    conn.commit()
    cursor.close()
    close_connection(conn)
    print("Orders seeded.")

def seed_payments():
    print("Seeding payments...")
    conn = create_connection()
    cursor = conn.cursor()

    unpaid_orders = executeSQL("""
        SELECT o.id, o.orderSum - COALESCE(p.samount, 0) AS due
        FROM Orders o
        LEFT JOIN (
            SELECT orderId, SUM(amount) AS samount FROM OrderPayments GROUP BY orderId
        ) p ON o.id = p.orderId
        WHERE o.orderSum - COALESCE(p.samount, 0) > 0.01
        ORDER BY o.orderDate
    """)

    paid_percentage = 0.95
    payment_channels = ["Online", "In-Store", "Mobile", "Credit Card"]
    payment_date = datetime.date.today()

    payments = []
    for order_id, due in unpaid_orders:
        if random.random() <= paid_percentage:
            payments.append((order_id, payment_date, random.choice(payment_channels), round(due, 2)))

    if payments:
        cursor.executemany("""
            INSERT INTO OrderPayments (orderId, paymentDate, channel, amount)
            VALUES (?, ?, ?, ?)""", payments)

    conn.commit()
    cursor.close()
    close_connection(conn)
    print(f"Seeded {len(payments)} payments.")

    cursor.execute("""
        IF OBJECT_ID('orderItems', 'U') IS NULL
        CREATE TABLE orderItems (
            id BIGINT IDENTITY(1,1) PRIMARY KEY,
            order_id BIGINT,
            product_id BIGINT,
            qty INT,
            price FLOAT,
            sum FLOAT,
            discount FLOAT,
            total FLOAT,
            notes VARCHAR(250)
        )
    """)

    cursor.execute("""
        IF OBJECT_ID('orderPayments', 'U') IS NULL
        CREATE TABLE orderPayments (
            id INT IDENTITY(1,1) PRIMARY KEY,
            product_id INT NOT NULL FOREIGN KEY REFERENCES products(id),
            customer_id INT NOT NULL FOREIGN KEY REFERENCES customers(id),
            quantity INT NOT NULL
        )
    """)

    cursor.execute("""
        IF OBJECT_ID('warehouses', 'U') IS NULL
        CREATE TABLE warehouses (
            id BIGINT IDENTITY(1,1) PRIMARY KEY,
            warehouse_name NVARCHAR(100),
            address NVARCHAR(250)
        )
    """)

    cursor.execute("""
        IF OBJECT_ID('inventory', 'U') IS NULL
        CREATE TABLE inventory (
            id BIGINT IDENTITY(1,1) PRIMARY KEY,
            productId BIGINT,
            warehouseId BIGINT,
            qty DECIMAL(18, 6),
            min_qty DECIMAL(18, 6),
            CONSTRAINT UK_inventory_productId_warehouseId UNIQUE(productId, warehouseId)
        )
    """)

    connection.commit()
    cursor.close()
    print("All tables created successfully.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


import datetime
import random


# ----------------------------
# 3. Main Seeding Function
# ----------------------------
def main():
    scenario = [
        "create_tables",
        "seed_customers",
        "batch_seed_orders",
        "seed_orders",
        "seed_payments"
    ]

    test_mode = Config.TEST_MODE

    try:
        if "create_tables" in scenario:
            connection = create_connection()
            create_tables(connection)
            close_connection(connection)

        if "seed_customers" in scenario:
            if executeSQL("SELECT COUNT(*) FROM Customers")[0][0] is None \
                or executeSQL("SELECT COUNT(*) FROM Customers")[0][0] <= Config.CUSTOMERS_TO_CREATE:
                if test_mode:
                    seed_customers(int(Config.CUSTOMERS_TO_CREATE / 100), Config.DEFAULT_BATCH_SIZE)
                else:
                    seed_customers(Config.CUSTOMERS_TO_CREATE, Config.DEFAULT_BATCH_SIZE)

        if "batch_seed_orders" in scenario:
            last_order_date = executeSQL("SELECT MAX(OrderDate) FROM Orders")[0][0]
            if last_order_date is not None:
                Config.ORDER_GENERATION_DATE = last_order_date.strftime("%Y-%m-%d")

            dt = datetime.datetime.strptime(Config.ORDER_GENERATION_DATE, "%Y-%m-%d").date()
            iteration = 0
            while dt < datetime.datetime.now().date():
                iteration += 1
                seed_orders(
                    Config.CUSTOMERS_PER_DAY + int(
                        Config.CUSTOMERS_PER_DAY * Config.CUSTOMERS_PER_DAY_VARIATION_PERCENTAGE / 100 * 2 * (random.random() - 0.5)),
                    1,
                    True,
                    Config.MAX_PRODUCTS_PER_ORDER,
                    3,
                    Config.MAX_ORDERS_PER_CUSTOMER
                )
                dt = dt + datetime.timedelta(days=1)
                Config.ORDER_GENERATION_DATE = dt.strftime("%Y-%m-%d")
                if test_mode and iteration >= Config.ITERATIONS_IN_TEST_MODE:
                    break

        elif "seed_orders" in scenario:
            seed_orders(
                100,
                10,
                True,
                Config.MAX_PRODUCTS_PER_ORDER,
                3,
                Config.MAX_ORDERS_PER_CUSTOMER
            )

        if "seed_payments" in scenario:
            seed_payments()
            print("Payments seeded.")

    finally:
        print("Process completed.")

# ----------------------------
# 4. Run
# ----------------------------
main()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
