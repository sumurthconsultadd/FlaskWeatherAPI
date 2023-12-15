import json


def test_get_weather_data(client, populated_db):
    response = client.get('/api/weather', data=json.dumps({}), headers={'Content-Type': 'application/json'})
    assert response.status_code == 200
    data = response.get_json()
    assert len(data['data']) == 2


def test_get_weather_stats(client, populated_db):
    response = client.get('/api/weather/stats', data=json.dumps({}),
                          headers={'Content-Type': 'application/json'})
    assert response.status_code == 200
    data = response.get_json()
    assert len(data['data']) == 2
