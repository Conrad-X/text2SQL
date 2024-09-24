from datetime import datetime, timedelta
from app.db import SessionLocal, engine
from app.models import music_festival_models as models 

def seed_db():
    models.MusicFestivalBase.metadata.create_all(bind=engine)

    db = SessionLocal()

    try:
        artists = [
            models.Artist(name='The Rolling Stones', genre='Rock', bio='An English rock band formed in London in 1962.', social_media_links={"Twitter": "https://twitter.com/rollingstones"}),
            models.Artist(name='Beyoncé', genre='Pop', bio='An American singer, songwriter, and actress.', social_media_links={"Instagram": "https://instagram.com/beyonce"}),
            models.Artist(name='Eminem', genre='Hip Hop', bio='An American rapper, songwriter, and record producer.', social_media_links={"Facebook": "https://facebook.com/eminem"}),
            models.Artist(name='Coldplay', genre='Alternative', bio='A British rock band formed in London in 1996.', social_media_links={"Website": "https://coldplay.com"}),
            models.Artist(name='Taylor Swift', genre='Country', bio='An American singer-songwriter.', social_media_links={"Twitter": "https://twitter.com/taylorswift"}),
            models.Artist(name='Billie Eilish', genre='Pop', bio='An American singer-songwriter known for her unique style.', social_media_links={"Instagram": "https://instagram.com/billieeilish"}),
            models.Artist(name='Drake', genre='Hip Hop', bio='A Canadian rapper, singer, songwriter, and actor.', social_media_links={"Twitter": "https://twitter.com/Drake"}),
            models.Artist(name='Adele', genre='Pop', bio='An English singer-songwriter known for her powerful voice.', social_media_links={"Website": "https://adele.com"}),
            models.Artist(name='The Weeknd', genre='R&B', bio='A Canadian singer, songwriter, and record producer.', social_media_links={"Instagram": "https://instagram.com/theweeknd"}),
            models.Artist(name='Kendrick Lamar', genre='Hip Hop', bio='An American rapper, songwriter, and record producer.', social_media_links={"Twitter": "https://twitter.com/kendricklamar"}),
        ]
        db.add_all(artists)
        db.commit()

        stages = [
            models.Stage(stage_name='Main Stage', capacity=50000, location='Field A'),
            models.Stage(stage_name='Sunset Stage', capacity=30000, location='Field B'),
            models.Stage(stage_name='Chill Stage', capacity=20000, location='Field C'),
            models.Stage(stage_name='Acoustic Stage', capacity=15000, location='Field D'),
            models.Stage(stage_name='Dance Stage', capacity=40000, location='Field E'),
            models.Stage(stage_name='Comedy Stage', capacity=10000, location='Field F'),
            models.Stage(stage_name='VIP Stage', capacity=25000, location='Field G'),
            models.Stage(stage_name='Local Talent Stage', capacity=12000, location='Field H'),
            models.Stage(stage_name='Family Stage', capacity=8000, location='Field I'),
            models.Stage(stage_name='Alternative Stage', capacity=18000, location='Field J'),
        ]
        db.add_all(stages)
        db.commit()

        performances = [
            models.Performance(artist_id=1, stage_id=1, performance_time=datetime.now() + timedelta(days=1, hours=18), duration=int(timedelta(hours=2).total_seconds())),
            models.Performance(artist_id=2, stage_id=2, performance_time=datetime.now() + timedelta(days=1, hours=20), duration=int(timedelta(hours=1, minutes=30).total_seconds())),
            models.Performance(artist_id=3, stage_id=1, performance_time=datetime.now() + timedelta(days=2, hours=19), duration=int(timedelta(hours=1, minutes=45).total_seconds())),
            models.Performance(artist_id=4, stage_id=3, performance_time=datetime.now() + timedelta(days=2, hours=21), duration=int(timedelta(hours=1).total_seconds())),
            models.Performance(artist_id=5, stage_id=2, performance_time=datetime.now() + timedelta(days=3, hours=20), duration=int(timedelta(hours=1, minutes=30).total_seconds())),
            models.Performance(artist_id=6, stage_id=1, performance_time=datetime.now() + timedelta(days=3, hours=22), duration=int(timedelta(hours=2).total_seconds())),
            models.Performance(artist_id=7, stage_id=3, performance_time=datetime.now() + timedelta(days=4, hours=20), duration=int(timedelta(hours=1, minutes=15).total_seconds())),
            models.Performance(artist_id=8, stage_id=2, performance_time=datetime.now() + timedelta(days=4, hours=18), duration=int(timedelta(hours=1, minutes=30).total_seconds())),
            models.Performance(artist_id=9, stage_id=4, performance_time=datetime.now() + timedelta(days=5, hours=19), duration=int(timedelta(hours=1).total_seconds())),
            models.Performance(artist_id=10, stage_id=5, performance_time=datetime.now() + timedelta(days=5, hours=21), duration=int(timedelta(hours=1, minutes=45).total_seconds())),
        ]
        db.add_all(performances)
        db.commit()

        attendees = [
            models.Attendee(name='Alice Cooper', email='alice.cooper@example.com', ticket_type='VIP', purchase_date=datetime.now()),
            models.Attendee(name='John Doe', email='john.doe@example.com', ticket_type='General', purchase_date=datetime.now()),
            models.Attendee(name='Jane Smith', email='jane.smith@example.com', ticket_type='Student', purchase_date=datetime.now()),
            models.Attendee(name='Tom Hardy', email='tom.hardy@example.com', ticket_type='General', purchase_date=datetime.now()),
            models.Attendee(name='Lucy Liu', email='lucy.liu@example.com', ticket_type='VIP', purchase_date=datetime.now()),
            models.Attendee(name='Michael Scott', email='michael.scott@example.com', ticket_type='VIP', purchase_date=datetime.now()),
            models.Attendee(name='Pam Beesly', email='pam.beesly@example.com', ticket_type='General', purchase_date=datetime.now()),
            models.Attendee(name='Jim Halpert', email='jim.halpert@example.com', ticket_type='Student', purchase_date=datetime.now()),
            models.Attendee(name='Dwight Schrute', email='dwight.schrute@example.com', ticket_type='General', purchase_date=datetime.now()),
            models.Attendee(name='Stanley Hudson', email='stanley.hudson@example.com', ticket_type='VIP', purchase_date=datetime.now()),
        ]
        db.add_all(attendees)
        db.commit()

        tickets = [
            models.Ticket(attendee_id=1, performance_id=1, ticket_price=150.00, status='Active'),
            models.Ticket(attendee_id=2, performance_id=2, ticket_price=100.00, status='Active'),
            models.Ticket(attendee_id=3, performance_id=3, ticket_price=120.00, status='Used'),
            models.Ticket(attendee_id=4, performance_id=4, ticket_price=90.00, status='Cancelled'),
            models.Ticket(attendee_id=5, performance_id=5, ticket_price=130.00, status='Active'),
            models.Ticket(attendee_id=6, performance_id=6, ticket_price=160.00, status='Active'),
            models.Ticket(attendee_id=7, performance_id=7, ticket_price=110.00, status='Used'),
            models.Ticket(attendee_id=8, performance_id=8, ticket_price=140.00, status='Active'),
            models.Ticket(attendee_id=9, performance_id=9, ticket_price=95.00, status='Cancelled'),
            models.Ticket(attendee_id=10, performance_id=10, ticket_price=145.00, status='Active'),
        ]
        db.add_all(tickets)
        db.commit()

        vendors = [
            models.Vendor(name='Food Truck', type='Food', location='Field A', contact_info='123-456-7890'),
            models.Vendor(name='Merch Booth', type='Merchandise', location='Field B', contact_info='098-765-4321'),
            models.Vendor(name='Coffee Stand', type='Beverages', location='Field C', contact_info='111-222-3333'),
            models.Vendor(name='Ice Cream Truck', type='Desserts', location='Field D', contact_info='444-555-6666'),
            models.Vendor(name='Taco Stand', type='Food', location='Field E', contact_info='777-888-9999'),
            models.Vendor(name='Clothing Booth', type='Merchandise', location='Field F', contact_info='000-111-2222'),
            models.Vendor(name='Smoothie Bar', type='Beverages', location='Field G', contact_info='333-444-5555'),
            models.Vendor(name='Pizza Stand', type='Food', location='Field H', contact_info='666-777-8888'),
            models.Vendor(name='Popcorn Cart', type='Snacks', location='Field I', contact_info='999-000-1111'),
            models.Vendor(name='Bakery Booth', type='Desserts', location='Field J', contact_info='222-333-4444'),
        ]
        db.add_all(vendors)
        db.commit()

        vendor_sales = [
            models.VendorSale(vendor_id=1, attendee_id=1, sale_amount=25.00, sale_date=datetime.now()),
            models.VendorSale(vendor_id=2, attendee_id=2, sale_amount=15.00, sale_date=datetime.now()),
            models.VendorSale(vendor_id=3, attendee_id=3, sale_amount=10.00, sale_date=datetime.now()),
            models.VendorSale(vendor_id=4, attendee_id=4, sale_amount=7.50, sale_date=datetime.now()),
            models.VendorSale(vendor_id=5, attendee_id=5, sale_amount=8.00, sale_date=datetime.now()),
            models.VendorSale(vendor_id=6, attendee_id=6, sale_amount=35.00, sale_date=datetime.now()),
            models.VendorSale(vendor_id=7, attendee_id=7, sale_amount=5.00, sale_date=datetime.now()),
            models.VendorSale(vendor_id=8, attendee_id=8, sale_amount=12.00, sale_date=datetime.now()),
            models.VendorSale(vendor_id=9, attendee_id=9, sale_amount=3.00, sale_date=datetime.now()),
            models.VendorSale(vendor_id=10, attendee_id=10, sale_amount=20.00, sale_date=datetime.now()),
        ]
        db.add_all(vendor_sales)
        db.commit()

        sponsorships = [
            models.Sponsorship(company_name='Coca Cola', sponsorship_amount=50000.00, sponsorship_type='Gold', contract_date=datetime.now()),
            models.Sponsorship(company_name='Pepsi', sponsorship_amount=30000.00, sponsorship_type='Silver', contract_date=datetime.now()),
            models.Sponsorship(company_name='Red Bull', sponsorship_amount=45000.00, sponsorship_type='Gold', contract_date=datetime.now()),
            models.Sponsorship(company_name='Spotify', sponsorship_amount=40000.00, sponsorship_type='Platinum', contract_date=datetime.now()),
            models.Sponsorship(company_name='Apple Music', sponsorship_amount=35000.00, sponsorship_type='Gold', contract_date=datetime.now()),
            models.Sponsorship(company_name='Monster Energy', sponsorship_amount=25000.00, sponsorship_type='Silver', contract_date=datetime.now()),
            models.Sponsorship(company_name='Budweiser', sponsorship_amount=30000.00, sponsorship_type='Bronze', contract_date=datetime.now()),
            models.Sponsorship(company_name='Amazon Music', sponsorship_amount=32000.00, sponsorship_type='Gold', contract_date=datetime.now()),
            models.Sponsorship(company_name='Samsung', sponsorship_amount=48000.00, sponsorship_type='Platinum', contract_date=datetime.now()),
            models.Sponsorship(company_name='Nike', sponsorship_amount=38000.00, sponsorship_type='Gold', contract_date=datetime.now()),
        ]
        db.add_all(sponsorships)
        db.commit()

        feedbacks = [
            models.Feedback(attendee_id=1, comments='Great festival!', rating=5, submission_date=datetime.now()),
            models.Feedback(attendee_id=2, comments='Had an amazing time!', rating=4, submission_date=datetime.now()),
            models.Feedback(attendee_id=3, comments='Could be better, more food options.', rating=3, submission_date=datetime.now()),
            models.Feedback(attendee_id=4, comments='Loved the performances!', rating=5, submission_date=datetime.now()),
            models.Feedback(attendee_id=5, comments='Too crowded, but fun!', rating=4, submission_date=datetime.now()),
            models.Feedback(attendee_id=6, comments='Best experience of my life!', rating=5, submission_date=datetime.now()),
            models.Feedback(attendee_id=7, comments='Good vibes, will come again.', rating=4, submission_date=datetime.now()),
            models.Feedback(attendee_id=8, comments='The music was great!', rating=5, submission_date=datetime.now()),
            models.Feedback(attendee_id=9, comments='Nice atmosphere!', rating=4, submission_date=datetime.now()),
            models.Feedback(attendee_id=10, comments='Disappointed with the schedule.', rating=2, submission_date=datetime.now()),
        ]
        db.add_all(feedbacks)
        db.commit()

        schedule = [
            models.Schedule(performance_id=1, scheduled_time=datetime.now() + timedelta(days=1, hours=18), stage_id=1, description='Opening Act'),
            models.Schedule(performance_id=2, scheduled_time=datetime.now() + timedelta(days=1, hours=20), stage_id=2, description='Evening Performance'),
            models.Schedule(performance_id=3, scheduled_time=datetime.now() + timedelta(days=2, hours=19), stage_id=1, description='Main Performance'),
            models.Schedule(performance_id=4, scheduled_time=datetime.now() + timedelta(days=2, hours=21), stage_id=3, description='Chill Session'),
            models.Schedule(performance_id=5, scheduled_time=datetime.now() + timedelta(days=3, hours=20), stage_id=2, description='Final Act'),
            models.Schedule(performance_id=6, scheduled_time=datetime.now() + timedelta(days=3, hours=22), stage_id=1, description='Late Night Show'),
            models.Schedule(performance_id=7, scheduled_time=datetime.now() + timedelta(days=4, hours=20), stage_id=3, description='Hip Hop Night'),
            models.Schedule(performance_id=8, scheduled_time=datetime.now() + timedelta(days=4, hours=18), stage_id=2, description='Pop Night'),
            models.Schedule(performance_id=9, scheduled_time=datetime.now() + timedelta(days=5, hours=19), stage_id=4, description='Acoustic Evening'),
            models.Schedule(performance_id=10, scheduled_time=datetime.now() + timedelta(days=5, hours=21), stage_id=5, description='Dance Party'),
        ]
        db.add_all(schedule)
        db.commit()

    except Exception as e:
        print(f"An unknown error occurred: {str(e)}")
        db.rollback()
    finally:
        db.close()

if __name__ == "__main__":
    seed_db()
