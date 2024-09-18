from datetime import datetime
from app.db import SessionLocal, engine
from app import models
from utilities.constants.response_messages import (
    ERROR_DATABASE_DELETE_FAILURE,
    ERROR_DATABASE_ROLLBACK_FAILURE,
    ERROR_DATABASE_CLOSE_FAILURE,
    UNKNOWN_ERROR,
) 

def seed_db():
    # Create tables
    models.Base.metadata.create_all(bind=engine)

    # Create a new session
    db = SessionLocal()

    try:
        # Clear existing data
        try:
            db.query(models.Hotel).delete()
            db.query(models.Room).delete()
            db.query(models.Guest).delete()
            db.query(models.Booking).delete()
            db.commit()
        except Exception as e:
            print(ERROR_DATABASE_DELETE_FAILURE.format(error=str(e)))
            db.rollback()

        # Seed Hotels
        hotels = [
            models.Hotel(hotelno='fb01', hotelname='Grosvenor', city='London'),
            models.Hotel(hotelno='fb02', hotelname='Watergate', city='Paris'),
            models.Hotel(hotelno='ch01', hotelname='Omni Shoreham', city='London'),
            models.Hotel(hotelno='ch02', hotelname='Phoenix Park', city='London'),
            models.Hotel(hotelno='dc01', hotelname='Latham', city='Berlin'),
        ]
        db.add_all(hotels)
        db.commit()

        # Seed Rooms
        rooms = [
            models.Room(roomno=501, hotelno='fb01', type='single', price=19),
            models.Room(roomno=601, hotelno='fb01', type='double', price=29),
            models.Room(roomno=701, hotelno='fb01', type='family', price=39),
            models.Room(roomno=1001, hotelno='fb02', type='single', price=58),
            models.Room(roomno=1101, hotelno='fb02', type='double', price=86),
            models.Room(roomno=1001, hotelno='ch01', type='single', price=29.99),
            models.Room(roomno=1101, hotelno='ch01', type='family', price=59.99),
            models.Room(roomno=701, hotelno='ch02', type='single', price=10),
            models.Room(roomno=801, hotelno='ch02', type='double', price=15),
            models.Room(roomno=901, hotelno='dc01', type='single', price=18),
            models.Room(roomno=1001, hotelno='dc01', type='double', price=30),
            models.Room(roomno=1101, hotelno='dc01', type='family', price=35),
        ]
        db.add_all(rooms)
        db.commit()

        # Seed Guests
        guests = [
            models.Guest(guestno=10001, guestname='John Kay', guestaddress='56 High St, London'),
            models.Guest(guestno=10002, guestname='Mike Ritchie', guestaddress='18 Tain St, London'),
            models.Guest(guestno=10003, guestname='Mary Tregear', guestaddress='5 Tarbot Rd, Aberdeen'),
            models.Guest(guestno=10004, guestname='Joe Keogh', guestaddress='2 Fergus Dr, Aberdeen'),
            models.Guest(guestno=10005, guestname='Carol Farrel', guestaddress='6 Achray St, Glasgow'),
            models.Guest(guestno=10006, guestname='Tina Murphy', guestaddress='63 Well St, Glasgow'),
            models.Guest(guestno=10007, guestname='Tony Shaw', guestaddress='12 Park Pl, Glasgow'),
        ]
        db.add_all(guests)
        db.commit()

        # Seed Bookings
        bookings = [
            models.Booking(hotelno='fb01', guestno=10001, datefrom=datetime(2004, 4, 1), dateto=datetime(2004, 4, 8), roomno=501),
            models.Booking(hotelno='fb01', guestno=10004, datefrom=datetime(2004, 4, 15), dateto=datetime(2004, 5, 15), roomno=601),
            models.Booking(hotelno='fb01', guestno=10005, datefrom=datetime(2004, 5, 2), dateto=datetime(2004, 5, 7), roomno=501),
            models.Booking(hotelno='fb01', guestno=10001, datefrom=datetime(2004, 5, 1), dateto=None, roomno=701),
            models.Booking(hotelno='fb02', guestno=10003, datefrom=datetime(2004, 4, 5), dateto=datetime(2004, 10, 4), roomno=1001),
            models.Booking(hotelno='ch01', guestno=10006, datefrom=datetime(2004, 4, 21), dateto=None, roomno=1101),
            models.Booking(hotelno='ch02', guestno=10002, datefrom=datetime(2004, 4, 25), dateto=datetime(2004, 5, 6), roomno=801),
            models.Booking(hotelno='dc01', guestno=10007, datefrom=datetime(2004, 5, 13), dateto=datetime(2004, 5, 15), roomno=1001),
            models.Booking(hotelno='dc01', guestno=10003, datefrom=datetime(2004, 5, 20), dateto=None, roomno=1001),
        ]
        db.add_all(bookings)
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
