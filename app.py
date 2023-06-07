from flask import Flask, request
from sqlalchemy import create_engine
import pandas as pd
import json

app = Flask(__name__)

# HOME
@app.route("/",methods=['POST'])
def home():
    engine = create_engine('postgresql://postgres:(CIMB2023)@proyectosanti.postgres.database.azure.com:5432/postgres')
    data = request.get_json()
    #time = f"""SELECT AVG("Throttle") FROM measurements WHERE measurement_time >= '{data['inicio']}' and measurement_time <= '{data['final']}'"""
    time = f"""WITH previous_interval AS (
    SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY "Throttle") AS previous_average
    FROM measurements
    WHERE measurement_time >= '{data['inicio']}'::timestamp - INTERVAL '120 second'
    AND measurement_time < '{data['inicio']}'::timestamp - INTERVAL '60 second'
    ),
    current_interval AS (
    SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY "Throttle") AS current_average
    FROM measurements
    WHERE measurement_time >= '{data['inicio']}'::timestamp - INTERVAL '60 second'
    )
    SELECT previous_average, current_average, (current_average - previous_average) / previous_average * 100 AS percentile_difference
    FROM previous_interval, current_interval;"""
    df = pd.read_sql(sql=time,con=engine)
    previous = df.iloc[0,0]
    current = df.iloc[0,1]
    average = df.iloc[0,2]
    payload = {
    'previous': previous,
    'current': current,
    'average': average
    }
    return json.dumps(payload)

if __name__ == '_main_' or not hasattr(app, 'serve'):
    app.run(debug=True)
