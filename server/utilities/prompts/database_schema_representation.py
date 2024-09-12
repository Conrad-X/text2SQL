code_representation_database_schema = """
create table hotel(
    hotelno varchar(10) primary key,
    hotelname varchar(20),
    city varchar(20),
)
create table room(
    roomno numeric(5),
    hotelno varchar(10),
    type varchar(10),
    price decimal(5,2),
    primary key (roomno, hotelno),
    foreign key (hotelno) REFERENCES hotel(hotelno)
)
create table guest(
    guestno numeric(5),
    guestname varchar(20),
    guestaddress varchar(50),
    primary key (guestno)
)
create table booking(
    hotelno varchar(10),
    guestno numeric(5),
    datefrom datetime,
    dateto datetime,
    roomno numeric(5),
    primary key (hotelno, guestno, datefrom),
    foreign key (roomno, hotelno) REFERENCES room(roomno, hotelno),
    foreign key (guestno) REFERENCES guest(guestno)
)
"""