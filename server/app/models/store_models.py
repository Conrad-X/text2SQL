from app.db import StoreBase
from sqlalchemy import Column, DateTime, ForeignKey, Numeric, String
from sqlalchemy.orm import relationship


class Store(StoreBase):
    __tablename__ = "store"

    storeno = Column(String(10), primary_key=True, index=True)
    storename = Column(String(50), nullable=True)
    location = Column(String(100), nullable=True)

    products = relationship("Product", back_populates="store")
    orders = relationship("Order", back_populates="store")

class Product(StoreBase):
    __tablename__ = "product"

    productno = Column(Numeric(5), primary_key=True)
    storeno = Column(String(10), ForeignKey("store.storeno"), nullable=False)
    productname = Column(String(50), nullable=True)
    price = Column(Numeric(10, 2), nullable=True)
    stock = Column(Numeric(5), nullable=True)

    store = relationship("Store", back_populates="products")
    order_items = relationship("OrderItem", back_populates="product")

class Customer(StoreBase):
    __tablename__ = "customer"

    customerno = Column(Numeric(5), primary_key=True, index=True)
    customername = Column(String(50), nullable=True)
    customeremail = Column(String(50), nullable=True)

    orders = relationship("Order", back_populates="customer")

class Order(StoreBase):
    __tablename__ = "order"

    orderno = Column(Numeric(10), primary_key=True)
    storeno = Column(String(10), ForeignKey("store.storeno"), nullable=False)
    customerno = Column(Numeric(5), ForeignKey("customer.customerno"), nullable=False)
    orderdate = Column(DateTime, nullable=True)

    store = relationship("Store", back_populates="orders")
    customer = relationship("Customer", back_populates="orders")
    order_items = relationship("OrderItem", back_populates="order")
    payment = relationship("Payment", back_populates="order", uselist=False) 


class Payment(StoreBase):
    __tablename__ = "payment"

    paymentno = Column(Numeric(10), primary_key=True)
    orderno = Column(Numeric(10), ForeignKey("order.orderno"), nullable=False)
    paymentdate = Column(DateTime, nullable=True)
    amount = Column(Numeric(10, 2), nullable=True)

    order = relationship("Order", back_populates="payment") 

class OrderItem(StoreBase):
    __tablename__ = "order_item"

    orderno = Column(Numeric(10), ForeignKey("order.orderno"), primary_key=True)
    productno = Column(Numeric(5), ForeignKey("product.productno"), primary_key=True)
    quantity = Column(Numeric(5), nullable=True)

    order = relationship("Order", back_populates="order_items")
    product = relationship("Product", back_populates="order_items")
