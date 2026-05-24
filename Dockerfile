# Használd az Airflow hivatalos, stabil alap képét
FROM apache/airflow:2.9.2

# Másold be és telepítsd az extra Python csomagokat
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt