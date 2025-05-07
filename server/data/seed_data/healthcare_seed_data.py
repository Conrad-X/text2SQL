from datetime import datetime, timedelta

from app.db import SessionLocal, engine
from app.models import healthcare_models as models


def seed_db():
    models.HealthcareBase.metadata.create_all(bind=engine)

    db = SessionLocal()

    try:
        departments = [
            models.Department(department_name='Cardiology', location='1st Floor', head_doctor_id=1),
            models.Department(department_name='Neurology', location='2nd Floor', head_doctor_id=2),
            models.Department(department_name='Orthopedics', location='3rd Floor', head_doctor_id=3),
            models.Department(department_name='Pediatrics', location='4th Floor', head_doctor_id=4),
            models.Department(department_name='Emergency', location='Ground Floor', head_doctor_id=5),
            models.Department(department_name='General Surgery', location='1st Floor', head_doctor_id=6),
            models.Department(department_name='Dermatology', location='2nd Floor', head_doctor_id=7),
            models.Department(department_name='Radiology', location='3rd Floor', head_doctor_id=8),
            models.Department(department_name='Oncology', location='4th Floor', head_doctor_id=9),
            models.Department(department_name='Psychiatry', location='5th Floor', head_doctor_id=10),
        ]
        db.add_all(departments)
        db.commit()

        doctors = [
            models.Doctor(first_name='John', last_name='Smith', specialization='Cardiology', contact_number='1234567890', email='john.smith@example.com'),
            models.Doctor(first_name='Alice', last_name='Johnson', specialization='Neurology', contact_number='1234567891', email='alice.johnson@example.com'),
            models.Doctor(first_name='Michael', last_name='Brown', specialization='Orthopedics', contact_number='1234567892', email='michael.brown@example.com'),
            models.Doctor(first_name='Sarah', last_name='Connor', specialization='Pediatrics', contact_number='1234567893', email='sarah.connor@example.com'),
            models.Doctor(first_name='James', last_name='Wilson', specialization='Emergency', contact_number='1234567894', email='james.wilson@example.com'),
            models.Doctor(first_name='Emma', last_name='Davis', specialization='Dermatology', contact_number='1234567895', email='emma.davis@example.com'),
            models.Doctor(first_name='Oliver', last_name='Thompson', specialization='Oncology', contact_number='1234567896', email='oliver.thompson@example.com'),
            models.Doctor(first_name='Sophia', last_name='Taylor', specialization='Radiology', contact_number='1234567897', email='sophia.taylor@example.com'),
            models.Doctor(first_name='Liam', last_name='Anderson', specialization='Gastroenterology', contact_number='1234567898', email='liam.anderson@example.com'),
            models.Doctor(first_name='Mia', last_name='Thomas', specialization='Psychiatry', contact_number='1234567899', email='mia.thomas@example.com'),
        ]
        db.add_all(doctors)
        db.commit()

        patients = [
            models.Patient(first_name='Alice', last_name='Doe', date_of_birth=datetime(1990, 1, 1).date(), gender='Female', contact_number='1234567895', email='alice.doe@example.com', address='123 Elm St'),
            models.Patient(first_name='Bob', last_name='Smith', date_of_birth=datetime(1985, 2, 2).date(), gender='Male', contact_number='1234567896', email='bob.smith@example.com', address='456 Maple Ave'),
            models.Patient(first_name='Charlie', last_name='Brown', date_of_birth=datetime(1992, 3, 3).date(), gender='Male', contact_number='1234567897', email='charlie.brown@example.com', address='789 Oak St'),
            models.Patient(first_name='Diana', last_name='Prince', date_of_birth=datetime(1988, 4, 4).date(), gender='Female', contact_number='1234567898', email='diana.prince@example.com', address='321 Pine St'),
            models.Patient(first_name='Ethan', last_name='Hunt', date_of_birth=datetime(1980, 5, 5).date(), gender='Male', contact_number='1234567899', email='ethan.hunt@example.com', address='654 Cedar St'),
            models.Patient(first_name='Fiona', last_name='Shrek', date_of_birth=datetime(1983, 6, 6).date(), gender='Female', contact_number='1234567800', email='fiona.shrek@example.com', address='987 Birch St'),
            models.Patient(first_name='George', last_name='Jetson', date_of_birth=datetime(1995, 7, 7).date(), gender='Male', contact_number='1234567801', email='george.jetson@example.com', address='159 Walnut St'),
            models.Patient(first_name='Hannah', last_name='Montana', date_of_birth=datetime(1991, 8, 8).date(), gender='Female', contact_number='1234567802', email='hannah.montana@example.com', address='753 Cherry St'),
            models.Patient(first_name='Ian', last_name='Malcolm', date_of_birth=datetime(1975, 9, 9).date(), gender='Male', contact_number='1234567803', email='ian.malcolm@example.com', address='852 Fir St'),
            models.Patient(first_name='Julia', last_name='Roberts', date_of_birth=datetime(1986, 10, 10).date(), gender='Female', contact_number='1234567804', email='julia.roberts@example.com', address='159 Spruce St'),
        ]
        db.add_all(patients)
        db.commit()

        appointments = [
            models.Appointment(patient_id=1, doctor_id=1, appointment_date=datetime.now() + timedelta(days=1), status='Scheduled'),
            models.Appointment(patient_id=2, doctor_id=2, appointment_date=datetime.now() + timedelta(days=2), status='Scheduled'),
            models.Appointment(patient_id=3, doctor_id=3, appointment_date=datetime.now() + timedelta(days=3), status='Scheduled'),
            models.Appointment(patient_id=4, doctor_id=4, appointment_date=datetime.now() + timedelta(days=4), status='Scheduled'),
            models.Appointment(patient_id=5, doctor_id=5, appointment_date=datetime.now() + timedelta(days=5), status='Scheduled'),
            models.Appointment(patient_id=6, doctor_id=6, appointment_date=datetime.now() + timedelta(days=6), status='Scheduled'),
            models.Appointment(patient_id=7, doctor_id=7, appointment_date=datetime.now() + timedelta(days=7), status='Scheduled'),
            models.Appointment(patient_id=8, doctor_id=8, appointment_date=datetime.now() + timedelta(days=8), status='Scheduled'),
            models.Appointment(patient_id=9, doctor_id=9, appointment_date=datetime.now() + timedelta(days=9), status='Scheduled'),
            models.Appointment(patient_id=10, doctor_id=10, appointment_date=datetime.now() + timedelta(days=10), status='Scheduled'),
        ]
        db.add_all(appointments)
        db.commit()

        treatments = [
            models.Treatment(treatment_name='Heart Checkup', cost=150.00, description='A comprehensive heart examination.'),
            models.Treatment(treatment_name='Brain MRI', cost=400.00, description='MRI scan of the brain to check for issues.'),
            models.Treatment(treatment_name='Knee Surgery', cost=3000.00, description='Surgical procedure for knee issues.'),
            models.Treatment(treatment_name='Child Vaccination', cost=50.00, description='Routine vaccination for children.'),
            models.Treatment(treatment_name='Emergency Trauma Care', cost=500.00, description='Immediate care for trauma patients.'),
            models.Treatment(treatment_name='Skin Biopsy', cost=250.00, description='Biopsy procedure for skin analysis.'),
            models.Treatment(treatment_name='Chemotherapy', cost=2000.00, description='Treatment for cancer patients.'),
            models.Treatment(treatment_name='X-Ray', cost=100.00, description='Radiographic imaging for diagnosis.'),
            models.Treatment(treatment_name='Endoscopy', cost=800.00, description='Procedure to examine digestive tract.'),
            models.Treatment(treatment_name='Psychotherapy', cost=120.00, description='Therapeutic sessions for mental health.'),
        ]
        db.add_all(treatments)
        db.commit()

        patient_treatments = [
            models.PatientTreatment(patient_id=1, treatment_id=1, doctor_id=1, date_administered=datetime.now(), notes='Patient has no previous heart issues.'),
            models.PatientTreatment(patient_id=2, treatment_id=2, doctor_id=2, date_administered=datetime.now(), notes='MRI performed to investigate headaches.'),
            models.PatientTreatment(patient_id=3, treatment_id=3, doctor_id=3, date_administered=datetime.now(), notes='Knee pain for 6 months, surgical recommendation.'),
            models.PatientTreatment(patient_id=4, treatment_id=4, doctor_id=4, date_administered=datetime.now(), notes='Routine checkup for child vaccinations.'),
            models.PatientTreatment(patient_id=5, treatment_id=5, doctor_id=5, date_administered=datetime.now(), notes='Trauma care required after accident.'),
            models.PatientTreatment(patient_id=6, treatment_id=6, doctor_id=6, date_administered=datetime.now(), notes='Skin lesion to be examined.'),
            models.PatientTreatment(patient_id=7, treatment_id=7, doctor_id=7, date_administered=datetime.now(), notes='Chemotherapy sessions starting.'),
            models.PatientTreatment(patient_id=8, treatment_id=8, doctor_id=8, date_administered=datetime.now(), notes='X-Ray recommended for bone injury.'),
            models.PatientTreatment(patient_id=9, treatment_id=9, doctor_id=9, date_administered=datetime.now(), notes='Endoscopy procedure scheduled for digestive issues.'),
            models.PatientTreatment(patient_id=10, treatment_id=10, doctor_id=10, date_administered=datetime.now(), notes='Therapy sessions for anxiety and stress.'),
        ]
        db.add_all(patient_treatments)
        db.commit()

        invoices = [
            models.Invoice(patient_id=1, amount_due=150.00, payment_status='Pending'),
            models.Invoice(patient_id=2, amount_due=400.00, payment_status='Pending'),
            models.Invoice(patient_id=3, amount_due=3000.00, payment_status='Pending'),
            models.Invoice(patient_id=4, amount_due=50.00, payment_status='Paid'),
            models.Invoice(patient_id=5, amount_due=500.00, payment_status='Pending'),
            models.Invoice(patient_id=6, amount_due=250.00, payment_status='Pending'),
            models.Invoice(patient_id=7, amount_due=2000.00, payment_status='Pending'),
            models.Invoice(patient_id=8, amount_due=100.00, payment_status='Paid'),
            models.Invoice(patient_id=9, amount_due=800.00, payment_status='Pending'),
            models.Invoice(patient_id=10, amount_due=120.00, payment_status='Pending'),
        ]
        db.add_all(invoices)
        db.commit()

        staff_members = [
            models.Staff(first_name='Emma', last_name='Watson', role='Nurse', contact_number='1234567897', email='emma.watson@example.com', department_id=1),
            models.Staff(first_name='Oliver', last_name='Twist', role='Technician', contact_number='1234567898', email='oliver.twist@example.com', department_id=2),
            models.Staff(first_name='Sophia', last_name='Turner', role='Receptionist', contact_number='1234567899', email='sophia.turner@example.com', department_id=1),
            models.Staff(first_name='Liam', last_name='Reed', role='Pharmacist', contact_number='1234567800', email='liam.reed@example.com', department_id=3),
            models.Staff(first_name='Mia', last_name='Parker', role='Radiologist', contact_number='1234567801', email='mia.parker@example.com', department_id=4),
            models.Staff(first_name='Noah', last_name='Bennett', role='Surgeon', contact_number='1234567802', email='noah.bennett@example.com', department_id=2),
            models.Staff(first_name='Olivia', last_name='Robinson', role='Therapist', contact_number='1234567803', email='olivia.robinson@example.com', department_id=5),
            models.Staff(first_name='James', last_name='Hall', role='Medical Assistant', contact_number='1234567804', email='james.hall@example.com', department_id=1),
            models.Staff(first_name='Ava', last_name='Young', role='Dietitian', contact_number='1234567805', email='ava.young@example.com', department_id=4),
            models.Staff(first_name='Lucas', last_name='Harris', role='Laboratory Technician', contact_number='1234567806', email='lucas.harris@example.com', department_id=3),
        ]
        db.add_all(staff_members)
        db.commit()

    except Exception as e:
        print(f"An unknown error occurred: {str(e)}")
        db.rollback()
    finally:
        db.close()

if __name__ == "__main__":
    seed_db()
