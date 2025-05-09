from app.db import MusicFestivalBase
from sqlalchemy import (JSON, Column, DateTime, Enum, ForeignKey, Integer,
                        Numeric, String, Text, Time)
from sqlalchemy.orm import relationship


class Artist(MusicFestivalBase):
    __tablename__ = "artists"

    artist_id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False)
    genre = Column(String, nullable=False)
    bio = Column(Text, nullable=True)
    social_media_links = Column(JSON, nullable=True)

    performances = relationship("Performance", back_populates="artist")

class Performance(MusicFestivalBase):
    __tablename__ = "performances"

    performance_id = Column(Integer, primary_key=True, index=True)
    artist_id = Column(Integer, ForeignKey("artists.artist_id"), nullable=False)
    stage_id = Column(Integer, ForeignKey("stages.stage_id"), nullable=False)
    performance_time = Column(DateTime, nullable=False)
    duration = Column(Integer, nullable=False)

    artist = relationship("Artist", back_populates="performances")
    stage = relationship("Stage", back_populates="performances")
    tickets = relationship("Ticket", back_populates="performance")

class Stage(MusicFestivalBase):
    __tablename__ = "stages"

    stage_id = Column(Integer, primary_key=True, index=True)
    stage_name = Column(String, nullable=False)
    capacity = Column(Integer, nullable=False)
    location = Column(String, nullable=False)

    performances = relationship("Performance", back_populates="stage")

class Attendee(MusicFestivalBase):
    __tablename__ = "attendees"

    attendee_id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False)
    email = Column(String, unique=True, nullable=False)
    ticket_type = Column(Enum("VIP", "General", "Student"), nullable=False)
    purchase_date = Column(DateTime, nullable=False)

    tickets = relationship("Ticket", back_populates="attendee")
    feedbacks = relationship("Feedback", back_populates="attendee")
    vendor_sales = relationship("VendorSale", back_populates="attendee")

class Ticket(MusicFestivalBase):
    __tablename__ = "tickets"

    ticket_id = Column(Integer, primary_key=True, index=True)
    attendee_id = Column(Integer, ForeignKey("attendees.attendee_id"), nullable=False)
    performance_id = Column(Integer, ForeignKey("performances.performance_id"), nullable=False)
    ticket_price = Column(Numeric(10, 2), nullable=False)
    status = Column(Enum("Active", "Used", "Cancelled"), nullable=False)

    attendee = relationship("Attendee", back_populates="tickets")
    performance = relationship("Performance", back_populates="tickets")

class Vendor(MusicFestivalBase):
    __tablename__ = "vendors"

    vendor_id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False)
    type = Column(String, nullable=False)  # e.g., Food, Merchandise
    location = Column(String, nullable=False)
    contact_info = Column(String, nullable=True)

    vendor_sales = relationship("VendorSale", back_populates="vendor")

class VendorSale(MusicFestivalBase):
    __tablename__ = "vendor_sales"

    sale_id = Column(Integer, primary_key=True, index=True)
    vendor_id = Column(Integer, ForeignKey("vendors.vendor_id"), nullable=False)
    attendee_id = Column(Integer, ForeignKey("attendees.attendee_id"), nullable=False)
    sale_amount = Column(Numeric(10, 2), nullable=False)
    sale_date = Column(DateTime, nullable=False)

    vendor = relationship("Vendor", back_populates="vendor_sales")
    attendee = relationship("Attendee", back_populates="vendor_sales")

class Sponsorship(MusicFestivalBase):
    __tablename__ = "sponsorships"

    sponsor_id = Column(Integer, primary_key=True, index=True)
    company_name = Column(String, nullable=False)
    sponsorship_amount = Column(Numeric(10, 2), nullable=False)
    sponsorship_type = Column(Enum("Gold", "Silver", "Bronze"), nullable=False)
    contract_date = Column(DateTime, nullable=False)

class Feedback(MusicFestivalBase):
    __tablename__ = "feedback"

    feedback_id = Column(Integer, primary_key=True, index=True)
    attendee_id = Column(Integer, ForeignKey("attendees.attendee_id"), nullable=False)
    comments = Column(Text, nullable=True)
    rating = Column(Integer, nullable=False)  # Assuming rating is between 1 and 5
    submission_date = Column(DateTime, nullable=False)

    attendee = relationship("Attendee", back_populates="feedbacks")

class Schedule(MusicFestivalBase):
    __tablename__ = "schedule"

    schedule_id = Column(Integer, primary_key=True, index=True)
    performance_id = Column(Integer, ForeignKey("performances.performance_id"), nullable=False)
    scheduled_time = Column(DateTime, nullable=False)
    stage_id = Column(Integer, ForeignKey("stages.stage_id"), nullable=False)
    description = Column(Text, nullable=True)

    performance = relationship("Performance")
    stage = relationship("Stage")
