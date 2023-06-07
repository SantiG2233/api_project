from flask import Flask, request
from sqlalchemy import create_engine
import pandas as pd
import json

app = Flask(__name__)

# HOME Pls
@app.route("/",methods=['POST'])
def home():
    engine = create_engine('postgresql://postgres:(CIMB2023)@proyectosanti.postgres.database.azure.com:5432/postgres')
    data = request.get_json()
    time = f"""SELECT AVG("Throttle") FROM measurements WHERE measurement_time >= '{data['inicio']}' and measurement_time <= '{data['final']}'"""
    df = pd.read_sql(sql=time,con=engine)
    data_return = df.iloc[0,0]
    payload = {
    'average': data_return,
    }
    return json.dumps(payload)

if __name__ == '_main_' or not hasattr(app, 'serve'):
    app.run(debug=-False)
