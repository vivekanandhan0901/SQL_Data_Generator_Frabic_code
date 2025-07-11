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

!pip install import_ipynb


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# MainNotebook.ipynb
%run Nb_config.ipynb


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************


# Now you can call the function and access the variable
print(greet("Fabric User"))
print(f"Global variable from other notebook: {my_global_variable}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

from fabric import task
import sys

# Append the directory to sys.path if needed
sys.path.append('/home/trusted-service-user/work')

# Import your functions module
import Nb_config

ca=Nb_config.Config()
print(ca)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

import import_ipynb

# Import your notebook
import Nb_config

# Call your function
ca=Nb_config.Config()
print(ca)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

import os
print(os.listdir())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

import os
print(os.getcwd())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# filepath: mssql-demo-project/mssql-demo-project/src/main.py
import datetime
import db
from seed import create_tables, seed_customers, seed_inventory, seed_orders, seed_payments, executeSQL, seed_warehouses, seed_suppliers, seed_purchase_orders, test_faker_locales, test_mimesis
from config import Config
import random

def main():
    # Define the scenario for seeding
    # This can be used to control which seeding functions to run
    # possible values: "seed_products", "seed_customers", "seed_orders", "create_tables"
    scenario = ["create_tables", 
                "!seed_customers",
                "!batch_seed_orders",
                "!seed_orders", 
                "!seed_payments",
                "!seed_warehouses",
                "!seed_inventory",
                "seed_suppliers",
                "seed_purchase_orders"] # switch '-' to '_' to be used in scenario
    test_mode = Config.TEST_MODE  # Use the test mode setting from the config

    try:
        test_faker_locales()

        # Check the scenario and call the appropriate seeding functions
        if "create_tables" in scenario:
            connection = db.create_connection()
            create_tables(connection)
        if "seed_customers" in scenario:   
            #check if customers table is empty before seeding            
            if executeSQL("SELECT COUNT(*) FROM Customers")[0][0] is None \
                or executeSQL("SELECT COUNT(*) FROM Customers")[0][0] <= Config.CUSTOMERS_TO_CREATE:                                
                if test_mode:
                    seed_customers(int(Config.CUSTOMERS_TO_CREATE/100), Config.DEFAULT_BATCH_SIZE)
                else:
                    seed_customers(Config.CUSTOMERS_TO_CREATE, Config.DEFAULT_BATCH_SIZE000)
        
        if "batch_seed_orders" in scenario:
            # Batch seed orders with a specific configuration
            last_order_date = executeSQL("SELECT MAX(OrderDate) FROM Orders")[0][0]
            if last_order_date is not None:
                Config.ORDER_GENERATION_DATE = last_order_date.strftime("%Y-%m-%d")
            
            dt = datetime.datetime.strptime(Config.ORDER_GENERATION_DATE, "%Y-%m-%d").date()
            iteration = 0
            while dt < datetime.datetime.now().date():
                iteration += 1
                seed_orders(Config.CUSTOMERS_PER_DAY + int(Config.CUSTOMERS_PER_DAY * Config.CUSTOMERS_PER_DAY_VARIATION_PERCENTAGE / 100 * 2 *(random.random()-0.5)) , # variation in batch size
                            1, 
                            True, 
                            Config.MAX_PRODUCTS_PER_ORDER, 3, Config.MAX_ORDERS_PER_CUSTOMER) 
                dt = dt + datetime.timedelta(days=1)
                Config.ORDER_GENERATION_DATE = dt.strftime("%Y-%m-%d")
                if test_mode and iteration >= Config.ITERATIONS_IN_TEST_MODE:
                    break

        elif "seed_orders" in scenario:
            seed_orders(100,
                        10, 
                        True, 
                        Config.MAX_PRODUCTS_PER_ORDER, 3, Config.MAX_ORDERS_PER_CUSTOMER)
            
        if "seed_payments" in scenario:
            seed_payments()
        
        if "seed_warehouses" in scenario:
            cnt = seed_warehouses()
            print(f"{cnt} warehouses seeded successfully.")

        if "seed_inventory" in scenario:
            cnt = seed_inventory()    
            print(f"{cnt} inventory records seeded successfully.")

        if "seed_suppliers" in scenario:
            cnt = seed_suppliers()   
            print(f"{cnt} suppliers seeded successfully.") 

        if "seed_purchase_orders" in scenario:
            cnt = seed_purchase_orders(100000,720)
            print(f"{cnt} purchase orders seeded successfully.")
            
        
    finally:
        print("Process completed.")
        

if __name__ == "__main__":
    main()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# # ----------------------------------
# # Fabric-Compatible Data Seeder Notebook (Main Logic)
# # ----------------------------------

# # ✅ Fabric-compatible rewrite of main.py
# # ⚠️ No external file/module imports

# import datetime
# import random

# # ----------------------------
# # 1. Bring in Local Definitions
# # ----------------------------
# # Use %run to bring in table creation and seeding functions from other notebooks
# %run "./db_setup_notebook"
# %run "./sqlalchemy_models_notebook"
# %run "./fabric_data_seeder"

# # ----------------------------
# # 2. Inline Config Settings
# # ----------------------------
# class Config:
#     ORDER_GENERATION_DATE = "2024-01-01"  # Date from which orders will be generated
#     inStockOnly = True                    # Only generate orders for products that are in stock
#     SQL_SERVER_LOCAL = False
#     CUSTOMERS_PER_DAY = 1000             # Number of orders per customer
#     CUSTOMERS_PER_DAY_VARIATION_PERCENTAGE = 10  # Variation in the number of customers per day
#     CUSTOMERS_TO_CREATE = 100000         # Total number of customers to create
#     MAX_ORDERS_PER_CUSTOMER = 2          # Maximum number of orders per customer
#     MAX_PRODUCTS_PER_ORDER = 5           # Maximum number of products per order
#     TEST_MODE = False                    # If True, the script will run in test mode with a limited number of orders and customers
#     ITERATIONS_IN_TEST_MODE = 10         # Number of iterations in test mode
#     DEFAULT_BATCH_SIZE = 1000            # Default batch size for seeding

# # ----------------------------
# # 3. Main Seeding Function
# # ----------------------------
# def main():
#     scenario = [
#         "create_tables",
#         "seed_customers",
#         "batch_seed_orders",
#         "seed_orders",
#         "seed_payments"
#     ]

#     test_mode = Config.TEST_MODE

#     try:
#         if "create_tables" in scenario:
#             connection = create_connection()
#             create_tables(connection)
#             close_connection(connection)

#         if "seed_customers" in scenario:
#             if executeSQL("SELECT COUNT(*) FROM Customers")[0][0] is None \
#                 or executeSQL("SELECT COUNT(*) FROM Customers")[0][0] <= Config.CUSTOMERS_TO_CREATE:
#                 if test_mode:
#                     seed_customers(int(Config.CUSTOMERS_TO_CREATE / 100), Config.DEFAULT_BATCH_SIZE)
#                 else:
#                     seed_customers(Config.CUSTOMERS_TO_CREATE, Config.DEFAULT_BATCH_SIZE)

#         if "batch_seed_orders" in scenario:
#             last_order_date = executeSQL("SELECT MAX(OrderDate) FROM Orders")[0][0]
#             if last_order_date is not None:
#                 Config.ORDER_GENERATION_DATE = last_order_date.strftime("%Y-%m-%d")

#             dt = datetime.datetime.strptime(Config.ORDER_GENERATION_DATE, "%Y-%m-%d").date()
#             iteration = 0
#             while dt < datetime.datetime.now().date():
#                 iteration += 1
#                 seed_orders(
#                     Config.CUSTOMERS_PER_DAY + int(
#                         Config.CUSTOMERS_PER_DAY * Config.CUSTOMERS_PER_DAY_VARIATION_PERCENTAGE / 100 * 2 * (random.random() - 0.5)),
#                     1,
#                     True,
#                     Config.MAX_PRODUCTS_PER_ORDER,
#                     3,
#                     Config.MAX_ORDERS_PER_CUSTOMER
#                 )
#                 dt = dt + datetime.timedelta(days=1)
#                 Config.ORDER_GENERATION_DATE = dt.strftime("%Y-%m-%d")
#                 if test_mode and iteration >= Config.ITERATIONS_IN_TEST_MODE:
#                     break

#         elif "seed_orders" in scenario:
#             seed_orders(
#                 100,
#                 10,
#                 True,
#                 Config.MAX_PRODUCTS_PER_ORDER,
#                 3,
#                 Config.MAX_ORDERS_PER_CUSTOMER
#             )

#         if "seed_payments" in scenario:
#             seed_payments()
#             print("✅ Payments seeded.")

#     finally:
#         print("✅ Process completed.")

# # ----------------------------
# # 4. Run
# # ----------------------------
# main()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
