# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# # ------------------------------
# # db_setup_notebook (Refactored)
# # ------------------------------

# import pyodbc
# import time

# # ------------------------------
# # DBConnectionManager (Shared Class)
# # ------------------------------
# class DBConnectionManager:
#     def __init__(self, use_local_db=False):
#         self.use_local_db = use_local_db
#         self.connection_string_local = (
#             "DRIVER={ODBC Driver 17 for SQL Server};"
#             "SERVER=localhost;"
#             "DATABASE=test;"
#             "Trusted_Connection=yes;"
#         )
#         self.connection_string_net = (
#             "DRIVER={ODBC Driver 17 for SQL Server};"
#             "SERVER=dataandai-celestial.database.windows.net;"
#             "DATABASE=Test_Mirror_Fabric_Database;"
#             "UID=celestial-sa;"
#             "PWD=srGnyE%g8(95;"
#         )
#         self.connection = None

#     def connect(self, retries=5, delay=5):
#         for attempt in range(retries):
#             try:
#                 conn_str = self.connection_string_local if self.use_local_db else self.connection_string_net
#                 self.connection = pyodbc.connect(conn_str)
#                 print("✅ Connected to SQL database.")
#                 return self.connection
#             except pyodbc.Error as e:
#                 print(f"Attempt {attempt + 1} failed: {e}")
#                 if attempt < retries - 1:
#                     print(f"Retrying in {delay} seconds...")
#                     time.sleep(delay)
#         raise Exception("❌ Failed to connect to the database after multiple attempts.")

#     def close(self):
#         if self.connection:
#             self.connection.close()
#             print("🔒 Connection closed.")

# # ------------------------------
# # Table Creation Logic
# # ------------------------------
# def create_tables(connection):
#     cursor = connection.cursor()

#     cursor.execute("""
#         IF OBJECT_ID('productCategories', 'U') IS NULL
#         CREATE TABLE productCategories (
#             id SMALLINT NOT NULL PRIMARY KEY,
#             category_name NVARCHAR(50) NOT NULL
#         )
#     """)

#     cursor.execute("""
#         IF OBJECT_ID('products', 'U') IS NULL
#         CREATE TABLE products (
#             id INT IDENTITY(1,1) PRIMARY KEY,
#             asin NVARCHAR(15) NOT NULL,
#             title NVARCHAR(800) NOT NULL,
#             imgUrl NVARCHAR(250) NOT NULL,
#             productURL NVARCHAR(250) NOT NULL,
#             stars FLOAT NOT NULL,
#             reviews NVARCHAR(50) NOT NULL,
#             price FLOAT NOT NULL,
#             listPrice FLOAT NOT NULL,
#             category_id INT NOT NULL
#         )
#     """)

#     cursor.execute("""
#         IF OBJECT_ID('customers', 'U') IS NULL
#         CREATE TABLE customers (
#             id INT IDENTITY(1,1) PRIMARY KEY,
#             firstName NVARCHAR(50) NOT NULL,
#             middleName NVARCHAR(50),
#             lastName NVARCHAR(50) NOT NULL,
#             company NVARCHAR(150),
#             DOB DATE NOT NULL,
#             email NVARCHAR(50) NOT NULL,
#             phone NVARCHAR(50),
#             address NVARCHAR(150),
#             city NVARCHAR(50),
#             state NVARCHAR(2),
#             zip NVARCHAR(10)
#         )
#     """)

#     cursor.execute("""
#         IF OBJECT_ID('orders', 'U') IS NULL
#         CREATE TABLE orders (
#             id BIGINT IDENTITY(1,1) PRIMARY KEY,
#             orderNumber NVARCHAR(50),
#             orderDate DATE NOT NULL,
#             customerId BIGINT,
#             discount FLOAT,
#             statuses NVARCHAR(MAX),
#             orderSum FLOAT,
#             notes NVARCHAR(MAX)
#         )
#     """)

#     cursor.execute("""
#         IF OBJECT_ID('orderItems', 'U') IS NULL
#         CREATE TABLE orderItems (
#             id BIGINT IDENTITY(1,1) PRIMARY KEY,
#             order_id BIGINT,
#             product_id BIGINT,
#             qty INT,
#             price FLOAT,
#             sum FLOAT,
#             discount FLOAT,
#             total FLOAT,
#             notes VARCHAR(250)
#         )
#     """)

#     cursor.execute("""
#         IF OBJECT_ID('orderPayments', 'U') IS NULL
#         CREATE TABLE orderPayments (
#     orderId BIGINT NOT NULL FOREIGN KEY REFERENCES Orders(id),
#     paymentDate DATE,
#     channel NVARCHAR(50),
#     amount FLOAT
#         )
#     """)

#     cursor.execute("""
#         IF OBJECT_ID('warehouses', 'U') IS NULL
#         CREATE TABLE warehouses (
#             id BIGINT IDENTITY(1,1) PRIMARY KEY,
#             warehouse_name NVARCHAR(100),
#             address NVARCHAR(250)
#         )
#     """)

#     cursor.execute("""
#         IF OBJECT_ID('inventory', 'U') IS NULL
#         CREATE TABLE inventory (
#             id BIGINT IDENTITY(1,1) PRIMARY KEY,
#             productId BIGINT,
#             warehouseId BIGINT,
#             qty DECIMAL(18, 6),
#             min_qty DECIMAL(18, 6),
#             CONSTRAINT UK_inventory_productId_warehouseId UNIQUE(productId, warehouseId)
#         )
#     """)

#     connection.commit()
#     cursor.close()
#     print("✅ All tables created successfully.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

import pyodbc
from config import Config

def create_connection():
    connection_string_local = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=localhost;"
        "DATABASE=test;"
        "Trusted_Connection=yes;"
    )
    connection_string_net = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=dataandai-celestial.database.windows.net;"
        "DATABASE=Test_Mirror_Fabric_Database;"
        "UID=celestial-sa;"
        "PWD=srGnyE%g8(95;"
    )
    # 5 attempts to connect to the database
    connection = None
    for attempt in range(5):
        try:
            if Config.SQL_SERVER_LOCAL:
                connection = pyodbc.connect(connection_string_local)
            else:   
                connection = pyodbc.connect(connection_string_net)
            # print("Connection established successfully.")
            break  # Exit loop if connection is successful
        except pyodbc.Error as e:
            print(f"Attempt {attempt + 1}: Failed to connect to the database. Error: {e}")
            if attempt < 4:
                print("Retrying in 5 seconds...")
                import time
                time.sleep(5)
    else:
        print("Failed to connect to the database after 5 attempts.")
        raise Exception("Database connection failed.")      
    return connection

def close_connection(connection):
    if connection:
        connection.close()

def create_tables(connection):
    cursor = connection.cursor()
    #create categories table
    cursor.execute("""
        IF OBJECT_ID('productCategories', 'U') IS NULL
        CREATE TABLE [dbo].[productCategories](
            [id] [smallint] NOT NULL,
            [category_name] [nvarchar](50) NOT NULL,
        CONSTRAINT [PK_amazon_categories] PRIMARY KEY CLUSTERED 
        (
            [id] ASC
        )WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
        ) ON [PRIMARY]
    """)

    # Create products table
    cursor.execute("""
        IF OBJECT_ID('products', 'U') IS NULL
        CREATE TABLE [dbo].[products](
        [id] [int] IDENTITY(1,1) NOT NULL,
        [asin] [nvarchar](15) NOT NULL,
        [title] [nvarchar](800) NOT NULL,
        [imgUrl] [nvarchar](250) NOT NULL,
        [productURL] [nvarchar](250) NOT NULL,
        [stars] [float] NOT NULL,
        [reviews] [nvarchar](50) NOT NULL,
        [price] [float] NOT NULL,
        [listPrice] [float] NOT NULL,
        [category_id] [int] NOT NULL,
    CONSTRAINT [PK_products_id] PRIMARY KEY CLUSTERED 
    (
        [id] ASC
    )WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
    ) ON [PRIMARY]
    """)

    # Create customers table
    cursor.execute("""
        IF OBJECT_ID('customers', 'U') IS NULL
        CREATE TABLE [dbo].[customers](
        [id] [int] IDENTITY(1,1) NOT NULL,
        [firstName] [nvarchar](50) NOT NULL,
        [middleName] [nvarchar](50),
        [lastName] [nvarchar](50) NOT NULL,
        [company] [nvarchar](150),
        [DOB] [date] NOT NULL,
        [email] [nvarchar](50) NOT NULL,
        [phone] [nvarchar](50),
        [address] [nvarchar](150),
        [city] [nvarchar](50),
        [state] [nvarchar](2),
        [zip] [nvarchar](10) 
    PRIMARY KEY CLUSTERED 
    (
        [id] ASC
    )WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
    ) ON [PRIMARY]
    """)

    # Create orders table
    cursor.execute("""
        IF OBJECT_ID('orders', 'U') IS NULL
        CREATE TABLE [dbo].[orders](
            [id] [bigint] IDENTITY(1,1) NOT NULL,
            [orderNumber] [nvarchar](50) NULL,
            [orderDate] [date] NOT NULL,
            [customerId] [bigint] NULL,
            [discount] [float] NULL,
            [statuses] [nvarchar](max) NULL,
            [orderSum] [float] NULL,
            [notes] [nvarchar](max) NULL,
            CONSTRAINT [PK_orders] PRIMARY KEY CLUSTERED 
            (
                [id] ASC
            )WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
        ) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
    """)

    # Create order lineitems table
    cursor.execute("""
        IF OBJECT_ID('orderItems', 'U') IS NULL
        CREATE TABLE [dbo].[orderItems](
            [id] [bigint] IDENTITY(1,1) NOT NULL,
            [order_id] [bigint] NULL,
            [product_id] [bigint] NULL,
            [qty] [int] NULL,
            [price] [float] NULL,
            [sum] [float] NULL,
            [discount] [float] NULL,
            [total] [float] NULL,
            [notes] [varchar](250) NULL,
    CONSTRAINT [PK_orderItems] PRIMARY KEY CLUSTERED 
    (
        [id] ASC
    )WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
    ) ON [PRIMARY]
    """)

    # Create transactions table
    cursor.execute("""
        IF OBJECT_ID('orderPayments', 'U') IS NULL
        CREATE TABLE orderPayments (
            id INT IDENTITY(1,1) PRIMARY KEY,
            product_id INT NOT NULL FOREIGN KEY REFERENCES products(id),
            customer_id INT NOT NULL FOREIGN KEY REFERENCES customers(id),
            quantity INT NOT NULL
        )
    """)

    #create warehouse table
    cursor.execute("""
        IF OBJECT_ID('warehouses', 'U') IS NULL
            CREATE TABLE [dbo].[warehouses](
                [id] [bigint] IDENTITY(1,1) NOT NULL,
                [warehouseName] [nvarchar](100) NOT NULL,
                [address] [nvarchar](250) NULL,
            CONSTRAINT [PK_warehouse] PRIMARY KEY CLUSTERED 
            (
                [id] ASC
            )WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY],
            CONSTRAINT [UK_warehouse_name] UNIQUE NONCLUSTERED 
            (
                [warehouseName]
            )WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
            ) ON [PRIMARY]"""
    )

    #create inventory table
    cursor.execute("""
        IF OBJECT_ID('inventory', 'U') IS NULL
            CREATE TABLE [dbo].[inventory](
                [id] [bigint] IDENTITY(1,1) NOT NULL,
                [productId] [bigint] NULL,
                [warehouseId] [bigint] NULL,
                [qty] [decimal](18, 6) NULL,
                [min_qty] [decimal](18, 6) NULL,
            CONSTRAINT [PK_inventory] PRIMARY KEY CLUSTERED 
            (
                [id] ASC
            )WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY],
            CONSTRAINT [UK_inventory_productId_warehouseId] UNIQUE NONCLUSTERED 
            (
                [productId] ASC,
                [warehouseId] ASC
            )WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
            ) ON [PRIMARY]
    """)
    
    #create suppliers table
    cursor.execute("""
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
            );  
    """)

    # Create purchase orders table
    cursor.execute("""
        IF OBJECT_ID('purchaseOrders', 'U') IS NULL
        CREATE TABLE [dbo].[purchaseOrders](
            [id] [bigint] IDENTITY(1,1) NOT NULL,
            [poNumber] [nvarchar](50) NOT NULL,
            [poDate] [date] NOT NULL,
            [supplierId] [bigint] NOT NULL,
            [warehouseId] [bigint] NULL,
            [requestedDeliveryDate] [date] NULL,
            [actualDeliveryDate] [date] NULL,
            [discount] [float] NULL DEFAULT(0),
            [shippingCost] [float] NULL DEFAULT(0),
            [taxAmount] [float] NULL DEFAULT(0),
            [totalAmount] [float] NOT NULL,
            [status] [nvarchar](50) NOT NULL DEFAULT('Draft'),
            [notes] [nvarchar](max) NULL,
            [createdDate] [datetime2] NOT NULL DEFAULT(GETDATE()),
            [modifiedDate] [datetime2] NOT NULL DEFAULT(GETDATE()),
            [createdBy] [nvarchar](100) NULL,
            [approvedBy] [nvarchar](100) NULL,
            [approvedDate] [datetime2] NULL,
            CONSTRAINT [PK_purchaseOrders] PRIMARY KEY CLUSTERED 
            (
                [id] ASC
            )WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY],
            CONSTRAINT [UK_purchaseOrders_poNumber] UNIQUE NONCLUSTERED 
            (
                [poNumber] ASC
            )WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
        ) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
    """)

    # Create purchase order items table
    cursor.execute("""
        IF OBJECT_ID('purchaseOrderItems', 'U') IS NULL
        CREATE TABLE [dbo].[purchaseOrderItems](
            [id] [bigint] IDENTITY(1,1) NOT NULL,
            [purchaseOrderId] [bigint] NOT NULL,
            [productId] [bigint] NOT NULL,
            [qty] [int] NOT NULL,
            [unitPrice] [float] NOT NULL,
            [lineTotal] [float] NOT NULL,
            [discount] [float] NULL DEFAULT(0),
            [receivedQty] [int] NULL DEFAULT(0),
            [notes] [nvarchar](250) NULL,
            CONSTRAINT [PK_purchaseOrderItems] PRIMARY KEY CLUSTERED 
            (
                [id] ASC
            )WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
        ) ON [PRIMARY]
    """)

    connection.commit()
    cursor.close()
    print("Tables created successfully.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
