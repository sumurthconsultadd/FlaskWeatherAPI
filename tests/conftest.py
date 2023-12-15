import pytest
from flaskr.app import app, engine, Session, WeatherRecord, WeatherStats
from datetime import datetime


@pytest.fixture
def client():
    app.config['TESTING'] = True
    app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///:memory:'
    with app.test_client() as client:
        yield client


@pytest.fixture
def db_session():
    connection = engine.connect()
    transaction = connection.begin()
    session = Session(bind=connection)
    yield session
    session.close()
    transaction.rollback()
    connection.close()


@pytest.fixture
def populated_db(db_session):
    record1 = WeatherRecord(
        station_id='USC00112193',
        date=datetime.strptime('1987-01-01', '%Y-%m-%d').date(),
        max_temperature=25.0,
        min_temperature=15.0,
        precipitation=5.0
    )
    db_session.add(record1)

    record2 = WeatherRecord(
        station_id='USC00112348',
        date=datetime.strptime('1998-01-02', '%Y-%m-%d').date(),
        max_temperature=28.0,
        min_temperature=18.0,
        precipitation=3.0
    )
    db_session.add(record2)

    record3 = WeatherStats(
        station_id='USC00112348',
        year='1998',
        avg_max_temperature=32.7,
        avg_min_temperature=-24.8,
        total_precipitation=7342
    )
    db_session.add(record3)

    record4 = WeatherStats(
        station_id='USC00112394',
        year='2001',
        avg_max_temperature=39.7,
        avg_min_temperature=-92.8,
        total_precipitation=7722
    )
    db_session.add(record4)

    db_session.commit()
    return db_session
