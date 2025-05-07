from app.db import HealthcareBase
from sqlalchemy import (DECIMAL, TEXT, Column, Date, DateTime, Enum,
                        ForeignKey, Integer, String)
from sqlalchemy.orm import relationship


class Patient(HealthcareBase):
    __tablename__ = "patients"

    patient_id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    first_name = Column(String(50), nullable=False)
    last_name = Column(String(50), nullable=False)
    date_of_birth = Column(Date, nullable=False)
    gender = Column(Enum('Male', 'Female', 'Other'), nullable=False)
    contact_number = Column(String(15), nullable=False)
    email = Column(String(100), unique=True)
    address = Column(TEXT)

    appointments = relationship("Appointment", back_populates="patient")
    patient_treatments = relationship("PatientTreatment", back_populates="patient")
    invoices = relationship("Invoice", back_populates="patient")

class Doctor(HealthcareBase):
    __tablename__ = "doctors"

    doctor_id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    first_name = Column(String(50), nullable=False)
    last_name = Column(String(50), nullable=False)
    specialization = Column(String(100), nullable=False)
    contact_number = Column(String(15), nullable=False)
    email = Column(String(100), unique=True)

    department = relationship("Department", back_populates="doctors")

class Department(HealthcareBase):
    __tablename__ = "departments"

    department_id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    department_name = Column(String(100), nullable=False)
    location = Column(String(100))
    head_doctor_id = Column(Integer, ForeignKey('doctors.doctor_id'))

    doctors = relationship("Doctor", back_populates="department")
    staff = relationship("Staff", back_populates="department")

class Appointment(HealthcareBase):
    __tablename__ = "appointments"

    appointment_id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    patient_id = Column(Integer, ForeignKey('patients.patient_id'))
    doctor_id = Column(Integer, ForeignKey('doctors.doctor_id'))
    appointment_date = Column(DateTime, nullable=False)
    status = Column(Enum('Scheduled', 'Completed', 'Cancelled'), default='Scheduled')

    patient = relationship("Patient", back_populates="appointments")
    doctor = relationship("Doctor")

class Treatment(HealthcareBase):
    __tablename__ = "treatments"

    treatment_id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    treatment_name = Column(String(100), nullable=False)
    cost = Column(DECIMAL(10, 2), nullable=False)
    description = Column(TEXT)

class PatientTreatment(HealthcareBase):
    __tablename__ = "patient_treatments"

    patient_treatment_id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    patient_id = Column(Integer, ForeignKey('patients.patient_id'))
    treatment_id = Column(Integer, ForeignKey('treatments.treatment_id'))
    doctor_id = Column(Integer, ForeignKey('doctors.doctor_id'))
    date_administered = Column(Date, nullable=False)
    notes = Column(TEXT)

    patient = relationship("Patient", back_populates="patient_treatments")
    treatment = relationship("Treatment")
    doctor = relationship("Doctor")

class Invoice(HealthcareBase):
    __tablename__ = "invoices"

    invoice_id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    patient_id = Column(Integer, ForeignKey('patients.patient_id'))
    amount_due = Column(DECIMAL(10, 2), nullable=False)
    payment_status = Column(Enum('Paid', 'Pending', 'Cancelled'), default='Pending')

    patient = relationship("Patient", back_populates="invoices")

class Staff(HealthcareBase):
    __tablename__ = "staff"

    staff_id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    first_name = Column(String(50), nullable=False)
    last_name = Column(String(50), nullable=False)
    role = Column(Enum('Nurse', 'Technician', 'Administrator'), nullable=False)
    contact_number = Column(String(15), nullable=False)
    email = Column(String(100), unique=True)
    department_id = Column(Integer, ForeignKey('departments.department_id'))

    department = relationship("Department", back_populates="staff")
