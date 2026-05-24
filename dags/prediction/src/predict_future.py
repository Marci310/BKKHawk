from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from airflow.hooks.postgres_hook import PostgresHook
from psycopg2.extras import execute_values
from prophet import Prophet
import logging
import numpy as np
from tensorflow.keras.models import load_model
import joblib

def get_latest_data(hook):
    query = """
            SELECT DATE_TRUNC('minute', f.timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Budapest') AS ds, 
                   AVG(f.delay_seconds)            AS y,
                   AVG(w.temperature_celsius)      AS temperature_celsius,
                   AVG(w.precipitation_mm)         AS precipitation_mm,
                   AVG(w.wind_speed_kph)           AS wind_speed_kph,
                   AVG(w.humidity_percent)         AS humidity_percent,
                   AVG(w.pressure_hpa)             AS pressure_hpa
            FROM fact_stop_time_delay f
                     JOIN dim_weather w ON f.weather_id = w.weather_id
            WHERE f.timestamp >= NOW() - INTERVAL '7 days'
              AND f.delay_seconds IS NOT NULL
            GROUP BY 1
            ORDER BY 1 \
            """
    df = hook.get_pandas_df(query)
    logging.info(f"Successfully fetched {len(df)} rows of latest data")
    return df

def generate_global_prophet_predictions(latest_data):
    if len(latest_data) < 60:
        logging.error("Too little data for predictions")
        return None

    try:
        logging.info("Generating Prophet predictions")
        m = Prophet(daily_seasonality=True, weekly_seasonality=False)
        m.add_country_holidays(country_name='HU')

        # Regresszorok
        regressors = ['temperature_celsius', 'precipitation_mm', 'wind_speed_kph', 'humidity_percent', 'pressure_hpa']
        for reg in regressors:
            m.add_regressor(reg)

        logging.info("Fitting Prophet model...")
        m.fit(latest_data)
        logging.info("Prophet fit complete.")

        logging.info("Creating future dataframe (60 rows only)...")
        future = m.make_future_dataframe(periods=60, freq='min', include_history=False)

        logging.info("Applying last known weather to future dataframe...")
        last_weather = latest_data.iloc[-1]
        for reg in regressors:
            future[reg] = last_weather[reg]

        logging.info("Running Prophet predict...")
        forecast = m.predict(future)
        logging.info("Prophet predict complete.")

        return forecast

    except Exception as e:
        logging.error(f"Prediction failed: {e}")
        return None

def insert_fact_predictions(hook, forecast, type):
    future_only = forecast.tail(60)
    records = []

    for _, row in future_only.iterrows():
        records.append((
            row['ds'],  # predicted_timestamp
            row['yhat'],  # predicted_delay_seconds
            type,  # model_name
        ))

    sql = """
          INSERT INTO fact_predictions (predicted_timestamp, predicted_delay_seconds, model_name)
          VALUES %s
          ON CONFLICT (predicted_timestamp, model_name)
          DO UPDATE SET
            predicted_delay_seconds = EXCLUDED.predicted_delay_seconds; \
          """

    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        # A execute_values a leggyorsabb módszer bulk insert/upsert-re Postgresben
        execute_values(cursor, sql, records)
        conn.commit()
        logging.info(f"Successfully upserted {len(records)} prediction rows.")
    except Exception as e:
        conn.rollback()
        logging.error(f"Failed to insert predictions: {e}")
        raise
    finally:
        cursor.close()


def generate_global_lstm_predictions(latest_data):
    # Az LSTM-nek szüksége van egy minimum múltbeli "ablakra" (pl. 60 perc)
    TIME_STEPS = 60

    if len(latest_data) < TIME_STEPS:
        logging.error(f"Too little data for LSTM. Need at least {TIME_STEPS} rows.")
        return None

    try:
        logging.info("Generating LSTM predictions")
        model = load_model('/opt/airflow/models/lstm_global_model.keras')
        scaler_X = joblib.load('/opt/airflow/models/scaler_X.pkl')
        scaler_y = joblib.load('/opt/airflow/models/scaler_y.pkl')

        df_recent = latest_data.tail(TIME_STEPS).copy()

        features = ['y', 'temperature_celsius', 'precipitation_mm', 'wind_speed_kph', 'humidity_percent',
                    'pressure_hpa']
        X_recent = df_recent[features].values

        X_scaled = scaler_X.transform(X_recent)
        current_window = X_scaled.reshape(1, TIME_STEPS, len(features))

        future_predictions_scaled = []

        for _ in range(60):
            pred_scaled = model.predict(current_window, verbose=0)[0, 0]
            future_predictions_scaled.append(pred_scaled)
            last_weather_scaled = current_window[0, -1, 1:]
            new_row = np.insert(last_weather_scaled, 0, pred_scaled)
            current_window = np.append(current_window[0, 1:, :], [new_row], axis=0).reshape(1, TIME_STEPS,
                                                                                            len(features))

        future_predictions_scaled = np.array(future_predictions_scaled).reshape(-1, 1)
        future_predictions = scaler_y.inverse_transform(future_predictions_scaled)

        last_time = latest_data['ds'].iloc[-1]
        future_dates = [last_time + pd.Timedelta(minutes=i) for i in range(1, 61)]

        forecast = pd.DataFrame({
            'ds': future_dates,
            'yhat': future_predictions.flatten()
        })

        return forecast

    except Exception as e:
        logging.error(f"LSTM Prediction failed: {e}")
        return None

def predict_future(**kwargs):
    hook = PostgresHook(postgres_conn_id='bkk_dwh')
    latest_data = get_latest_data(hook)
    if latest_data is not None and not latest_data.empty:
        prophet_forecast = generate_global_prophet_predictions(latest_data)
        lstm_forecast = generate_global_lstm_predictions(latest_data)
        if prophet_forecast is not None:
            logging.info("Prophet Prediction successful.")
            insert_fact_predictions(hook, prophet_forecast, 'Prophet_Global_Minutely')
        if lstm_forecast is not None:
            logging.info("LSTM Prediction successful.")
            insert_fact_predictions(hook, lstm_forecast, 'LSTM_Global_Minutely')
