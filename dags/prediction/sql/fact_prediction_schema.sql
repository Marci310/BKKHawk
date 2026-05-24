CREATE TABLE IF NOT EXISTS fact_predictions (
    prediction_id SERIAL PRIMARY KEY,
    predicted_timestamp TIMESTAMP NOT NULL,
    predicted_delay_seconds FLOAT NOT NULL,
    model_name VARCHAR(50) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexek a gyors Power BI lekérdezésekhez
CREATE INDEX IF NOT EXISTS idx_fact_predictions_timestamp ON fact_predictions(predicted_timestamp);
CREATE UNIQUE INDEX IF NOT EXISTS uq_fact_predictions ON fact_predictions(predicted_timestamp, model_name);
