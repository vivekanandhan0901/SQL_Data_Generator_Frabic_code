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


from sqlalchemy import create_engine, Column, Integer, String, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()

class Product(Base):
    __tablename__ = 'products'
    
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    price = Column(Integer, nullable=False)
    stock = Column(Integer, nullable=False)

class Customer(Base):
    __tablename__ = 'customers'
    
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    email = Column(String, nullable=False)

class Transaction(Base):
    __tablename__ = 'transactions'
    
    id = Column(Integer, primary_key=True)
    product_id = Column(Integer, ForeignKey('products.id'), nullable=False)
    customer_id = Column(Integer, ForeignKey('customers.id'), nullable=False)
    quantity = Column(Integer, nullable=False)

    product = relationship("Product")
    customer = relationship("Customer")

class Order(Base):
    __tablename__ = 'orders'
    
    id = Column(Integer, primary_key=True)
    order_number = Column(String, nullable=False, name="orderNumber")
    order_date = Column(String, nullable=False, name="orderDate")
    customer_id = Column(Integer, ForeignKey('customers.id'), nullable=False, name="customerId")
    discount = Column(Integer,default=0)    
    statuses = Column(String, nullable=True, default="{}")
    order_sum = Column(Integer, nullable=False, name="orderSum")
    notes = Column(String, nullable=True)
    items = relationship("OrderItem", back_populates="order")
    customer = relationship("Customer", back_populates="orders")

class OrderItem(Base):
    __tablename__ = 'order_items'
    id = Column(Integer, primary_key=True)
    order_id = Column(Integer, ForeignKey('orders.id'), nullable=False)
    product_id = Column(Integer, ForeignKey('products.id'), nullable=False)
    quantity = Column(Integer, nullable=False, name="qty")
    price = Column(Integer, nullable=False)

    order = relationship("Order", back_populates="items")
    product = relationship("Product")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# # --- 1. Define Spark-compatible schema for each table ---

# product_schema = StructType([
#     StructField("id", IntegerType(), False),
#     StructField("name", StringType(), False),
#     StructField("price", IntegerType(), False),
#     StructField("stock", IntegerType(), False)
# ])

# customer_schema = StructType([
#     StructField("id", IntegerType(), False),
#     StructField("name", StringType(), False),
#     StructField("email", StringType(), False)
# ])

# transaction_schema = StructType([
#     StructField("id", IntegerType(), False),
#     StructField("product_id", IntegerType(), False),
#     StructField("customer_id", IntegerType(), False),
#     StructField("quantity", IntegerType(), False)
# ])

# order_schema = StructType([
#     StructField("id", IntegerType(), False),
#     StructField("orderNumber", StringType(), False),
#     StructField("orderDate", StringType(), False),
#     StructField("customerId", IntegerType(), False),
#     StructField("discount", IntegerType(), True),
#     StructField("statuses", StringType(), True),
#     StructField("orderSum", IntegerType(), False),
#     StructField("notes", StringType(), True)
# ])

# order_item_schema = StructType([
#     StructField("id", IntegerType(), False),
#     StructField("order_id", IntegerType(), False),
#     StructField("product_id", IntegerType(), False),
#     StructField("qty", IntegerType(), False),
#     StructField("price", IntegerType(), False)
# ])


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
