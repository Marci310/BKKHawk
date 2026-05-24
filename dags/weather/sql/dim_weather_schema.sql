CREATE TABLE IF NOT EXISTS dim_weather (
    weather_id SERIAL PRIMARY KEY,
    weather_timestamp TIMESTAMP NOT NULL UNIQUE,
    temperature_celsius FLOAT NOT NULL,
    wind_speed_kph FLOAT NOT NULL,
    pressure_hpa FLOAT NOT NULL,
    humidity_percent INT NOT NULL,
    precipitation_mm FLOAT NOT NULL,
    weather_description VARCHAR(100),
    condition_code VARCHAR(10),
    is_rain_event BOOLEAN NOT NULL,
    is_snow_event BOOLEAN NOT NULL,
    is_ice_risk BOOLEAN NOT NULL,
    is_foggy BOOLEAN NOT NULL,
    icon_url VARCHAR(255) NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_dim_weather_ts ON dim_weather(weather_timestamp);