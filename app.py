from flask import Flask, request, jsonify
from flask_restful import Api, reqparse
from flasgger import Swagger
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from flaskr.models import WeatherRecord, WeatherStats, Base
from datetime import datetime

app = Flask(__name__)
api = Api(app)
swagger = Swagger(app)

engine = create_engine('sqlite:///weather.db', echo=True)
Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)
session = Session()

parser = reqparse.RequestParser()
parser.add_argument('date', type=str, help='Date filter (YYYY-MM-DD)')
parser.add_argument('station_id', type=str, help='Station ID filter')


@app.route('/api/weather')
def get_weather_data():
    """
    This is the endpoint for getting weather data.
    ---
    parameters:
      - name: date
        in: query
        type: string
        description: Date filter (YYYY-MM-DD)
      - name: station_id
        in: query
        type: string
        description: Station ID filter
    responses:
      200:
        description: Successful response
    """
    args = parser.parse_args()
    date_filter = args['date']
    station_id_filter = args['station_id']

    query = session.query(WeatherRecord)

    page = request.args.get('page', type=int, default=1)
    per_page = request.args.get('per_page', type=int, default=20)

    if date_filter:
        query = query.filter(WeatherRecord.date == datetime.strptime(date_filter, '%Y-%m-%d').date())

    if station_id_filter:
        query = query.filter(WeatherRecord.station_id == station_id_filter)

    offset = (page - 1) * per_page
    paginated_data = query.offset(offset).limit(per_page).all()
    result = {
        'data': [{'id': record.id,
                  'station_id': record.station_id,
                  'date': record.date.strftime('%Y-%m-%d'),
                  'max_temperature': record.max_temperature,
                  'min_temperature': record.min_temperature,
                  'precipitation': record.precipitation
                  } for record in paginated_data],
        'total': len(paginated_data)
    }

    return jsonify(result)


@app.route('/api/weather/stats')
def get_weather_stats():
    """
    This is the endpoint for getting weather stats.
    ---
    parameters:
      - name: date
        in: query
        type: string
        description: Date filter (YYYY-MM-DD)
      - name: station_id
        in: query
        type: string
        description: Station ID filter
    responses:
      200:
        description: Successful response
    """
    args = parser.parse_args()
    date_filter = args['date']
    station_id_filter = args['station_id']

    query = session.query(WeatherStats)

    if date_filter:
        query = query.filter(WeatherStats.year == datetime.strptime(date_filter, '%Y-%m-%d').year)

    if station_id_filter:
        query = query.filter(WeatherStats.station_id == station_id_filter)

    all_data = query.all()

    result = {
        'data': [{'id': record.id,
                  'station_id': record.station_id,
                  'avg_max_temperature': record.avg_max_temperature,
                  'avg_min_temperature': record.avg_min_temperature,
                  'total_precipitation': record.total_precipitation
                  } for record in all_data],
        'total': len(all_data)
    }

    return jsonify(result)


if __name__ == '__main__':
    app.run(debug=True)
