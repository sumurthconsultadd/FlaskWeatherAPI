from sqlalchemy import Column, Integer, String, Date, Float
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class WeatherRecord(Base):
    __tablename__ = 'weather_records'

    id = Column(Integer, primary_key=True)
    station_id = Column(String)
    date = Column(Date)
    max_temperature = Column(Float)
    min_temperature = Column(Float)
    precipitation = Column(Float)


class WeatherStats(Base):
    __tablename__ = 'weather_stats'

    id = Column(Integer, primary_key=True)
    year = Column(Integer)
    station_id = Column(String)
    avg_max_temperature = Column(Float)
    avg_min_temperature = Column(Float)
    total_precipitation = Column(Float)
