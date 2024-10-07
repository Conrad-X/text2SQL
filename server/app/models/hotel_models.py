from sqlalchemy import Column, String, Numeric, DateTime, ForeignKey, DECIMAL
from sqlalchemy.orm import relationship
from app.db import HotelBase

class Hotel(HotelBase):
    __tablename__ = "hotel"

    hotelno = Column(String(10), primary_key=True, index=True)
    hotelname = Column(String(20), nullable=True)
    city = Column(String(20), nullable=True)

    rooms = relationship("Room", back_populates="hotel")
    bookings = relationship("Booking", back_populates="hotel")

class Room(HotelBase):
    __tablename__ = "room"

    roomno = Column(Numeric(5), primary_key=True)
    hotelno = Column(String(10), ForeignKey("hotel.hotelno"), primary_key=True)
    type = Column(String(10), nullable=True)
    price = Column(DECIMAL(5, 2), nullable=True)

    hotel = relationship("Hotel", back_populates="rooms")
    bookings = relationship("Booking", back_populates="room")

class Guest(HotelBase):
    __tablename__ = "guest"

    guestno = Column(Numeric(5), primary_key=True, index=True)
    guestname = Column(String(20), nullable=True)
    guestaddress = Column(String(50), nullable=True)

    bookings = relationship("Booking", back_populates="guest")

class Booking(HotelBase):
    __tablename__ = "booking"

    hotelno = Column(String(10), ForeignKey("hotel.hotelno"), primary_key=True)
    guestno = Column(Numeric(5), ForeignKey("guest.guestno"), primary_key=True)
    datefrom = Column(DateTime, primary_key=True)
    dateto = Column(DateTime, nullable=True)
    roomno = Column(Numeric(5), ForeignKey("room.roomno"), primary_key=True)

    hotel = relationship("Hotel", back_populates="bookings")
    guest = relationship("Guest", back_populates="bookings")
    room = relationship("Room", back_populates="bookings")
