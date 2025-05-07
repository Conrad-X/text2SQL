from datetime import datetime

from app.db import SessionLocal, engine
from app.models import store_models as models
from utilities.constants.response_messages import (
    ERROR_DATABASE_CLOSE_FAILURE, ERROR_DATABASE_ROLLBACK_FAILURE,
    UNKNOWN_ERROR)


def seed_db():
    models.StoreBase.metadata.create_all(bind=engine)

    db = SessionLocal()

    try:
        stores = [
            models.Store(storeno='s01', storename='Tech Shop', location='New York'),
            models.Store(storeno='s02', storename='Grocery Store', location='San Francisco'),
            models.Store(storeno='s03', storename='Clothing Store', location='Los Angeles'),
            models.Store(storeno='s04', storename='Furniture Store', location='Chicago'),
            models.Store(storeno='s05', storename='Book Store', location='Boston'),
        ]
        db.add_all(stores)
        db.commit()

        products = [
            models.Product(productno=1, storeno='s01', productname='Laptop', price=999.99, stock=50),
            models.Product(productno=2, storeno='s01', productname='Smartphone', price=699.99, stock=100),
            models.Product(productno=3, storeno='s02', productname='Apple', price=0.99, stock=200),
            models.Product(productno=4, storeno='s02', productname='Banana', price=0.59, stock=150),
            models.Product(productno=5, storeno='s03', productname='T-Shirt', price=19.99, stock=75),
            models.Product(productno=6, storeno='s03', productname='Jeans', price=49.99, stock=60),
            models.Product(productno=7, storeno='s04', productname='Sofa', price=499.99, stock=30),
            models.Product(productno=8, storeno='s04', productname='Dining Table', price=299.99, stock=20),
            models.Product(productno=9, storeno='s05', productname='Book - Python Programming', price=29.99, stock=100),
            models.Product(productno=10, storeno='s05', productname='Book - Machine Learning', price=39.99, stock=80),
        ]
        db.add_all(products)
        db.commit()

        customers = [
            models.Customer(customerno=1, customername='Alice Johnson', customeremail='alice@example.com'),
            models.Customer(customerno=2, customername='Bob Smith', customeremail='bob@example.com'),
            models.Customer(customerno=3, customername='Charlie Brown', customeremail='charlie@example.com'),
            models.Customer(customerno=4, customername='David Lee', customeremail='david@example.com'),
            models.Customer(customerno=5, customername='Eva Green', customeremail='eva@example.com'),
        ]
        db.add_all(customers)
        db.commit()

        orders = [
            models.Order(orderno=1, storeno='s01', customerno=1, orderdate=datetime.now()),
            models.Order(orderno=2, storeno='s02', customerno=2, orderdate=datetime.now()),
            models.Order(orderno=3, storeno='s03', customerno=3, orderdate=datetime.now()),
            models.Order(orderno=4, storeno='s04', customerno=4, orderdate=datetime.now()),
            models.Order(orderno=5, storeno='s05', customerno=5, orderdate=datetime.now()),
        ]
        db.add_all(orders)
        db.commit()

        order_items = [
            models.OrderItem(orderno=1, productno=1, quantity=1),
            models.OrderItem(orderno=2, productno=3, quantity=5),
            models.OrderItem(orderno=3, productno=5, quantity=2),
            models.OrderItem(orderno=4, productno=7, quantity=1),
            models.OrderItem(orderno=5, productno=9, quantity=3),
        ]
        db.add_all(order_items)
        db.commit()

        payments = [
            models.Payment(paymentno=1, orderno=1, paymentdate=datetime.now(), amount=999.99),
            models.Payment(paymentno=2, orderno=2, paymentdate=datetime.now(), amount=4.95),
            models.Payment(paymentno=3, orderno=3, paymentdate=datetime.now(), amount=39.98),
            models.Payment(paymentno=4, orderno=4, paymentdate=datetime.now(), amount=499.99),
            models.Payment(paymentno=5, orderno=5, paymentdate=datetime.now(), amount=89.97),
        ]
        db.add_all(payments)
        db.commit()

    except Exception as e:
        print(UNKNOWN_ERROR.format(error=str(e)))
        try:
            db.rollback()
        except Exception as rollback_error:
            print(ERROR_DATABASE_ROLLBACK_FAILURE.format(error=str(rollback_error)))
    finally:
        try:
            db.close()
        except Exception as close_error:
            print(ERROR_DATABASE_CLOSE_FAILURE.format(error=str(close_error)))

if __name__ == "__main__":
    seed_db()
