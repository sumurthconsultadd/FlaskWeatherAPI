import os
import logging
from datetime import datetime
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError
from flaskr.models import WeatherRecord, WeatherStats, Base


logging.basicConfig(filename='weather_ingest.log', level=logging.INFO)

engine = create_engine('sqlite:///weather.db', echo=True)
Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)
session = Session()


def ingest_data(directory):
    for filename in os.listdir(directory):
        logging.info(f"Processing file: {filename}")
        start_time = datetime.now()

        with open(os.path.join(directory, filename), 'r') as file:
            for line in file:
                data = line.strip().split('\t')
                station_id = filename.split('.')[0]
                date = datetime.strptime(data[0], '%Y%m%d').date()
                max_temperature = float(data[1]) if float(data[1]) != -9999 else None
                min_temperature = float(data[2]) if float(data[2]) != -9999 else None
                precipitation = float(data[3]) if float(data[3]) != -9999 else None

                record = WeatherRecord(
                    station_id=station_id,
                    date=date,
                    max_temperature=max_temperature,
                    min_temperature=min_temperature,
                    precipitation=precipitation,
                )
                try:
                    session.add(record)
                    session.commit()
                except IntegrityError:
                    session.rollback()

        end_time = datetime.now()
        elapsed_time = end_time - start_time
        logging.info(f"File {filename} processed. Records: {session.query(WeatherRecord).count()}. Time elapsed: {elapsed_time}")


def calculate_and_insert_stats():
    distinct_years = session.query(func.extract('year', WeatherRecord.date).label('year')).distinct()
    distinct_station_ids = session.query(WeatherRecord.station_id).distinct()

    for year in distinct_years:
        for station_id in distinct_station_ids:
            avg_max_temp = session.query(func.avg(WeatherRecord.max_temperature))\
                .filter(func.extract('year', WeatherRecord.date) == year.year, WeatherRecord.station_id == station_id.station_id).scalar()
            avg_min_temp = session.query(func.avg(WeatherRecord.min_temperature))\
                .filter(func.extract('year', WeatherRecord.date) == year.year, WeatherRecord.station_id == station_id.station_id).scalar()
            total_precipitation = session.query(func.sum(WeatherRecord.precipitation))\
                .filter(func.extract('year', WeatherRecord.date) == year.year, WeatherRecord.station_id == station_id.station_id).scalar()

            avg_max_temp = avg_max_temp or 0.0
            avg_min_temp = avg_min_temp or 0.0
            total_precipitation = total_precipitation or 0.0

            stats_record = WeatherStats(
                year=year.year,
                station_id=station_id.station_id,
                avg_max_temperature=avg_max_temp,
                avg_min_temperature=avg_min_temp,
                total_precipitation=total_precipitation
            )
            session.add(stats_record)


data_directory = "wx_data"
# ingest_data(data_directory)
#
# session.commit()

# calculate_and_insert_stats()

# session.commit()
session.close()
