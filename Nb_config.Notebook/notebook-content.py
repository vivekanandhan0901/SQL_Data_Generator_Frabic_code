# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# config_notebook (Microsoft Fabric Notebook)

# class Config:
#     ORDER_GENERATION_DATE = "2024-01-01" # Date from which orders will be generated
#     inStockOnly = True # Only generate orders for products that are in stock
#     SQL_SERVER_LOCAL = False 
#     CUSTOMERS_PER_DAY = 1000 # Number of orders per customer
#     CUSTOMERS_PER_DAY_VARIATION_PERCENTAGE = 10 # Variation in the number of customers per day
#     CUSTOMERS_TO_CREATE = 100000 # Total number of customers to create
#     SUPPLIERS_TO_CREATE = 200 # Total number of suppliers to create
    
#     MAX_ORDERS_PER_CUSTOMER = 2 # Maximum number of orders per customer
#     MAX_PRODUCTS_PER_ORDER = 5 # Maximum number of products per order
#     MAX_WAREHOUSES = 5 # Maximum number of warehouses

#     TEST_MODE = False # If True, the script will run in test mode with a limited number of orders and customers
#     ITERATIONS_IN_TEST_MODE = 10 # Number of iterations in test mode
#     DEFAULT_BATCH_SIZE = 500 # Default batch size for seeding
# MyFunctions.ipynb
def greet(name):
    return f"Hello, {name}!"

my_global_variable = 10

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
