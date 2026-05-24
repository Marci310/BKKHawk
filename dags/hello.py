import logging
from datetime import datetime
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook

# Teszteljük, hogy az egyedi Docker image importálja-e a csomagokat
try:
    import pandas as pd
    from google.transit import gtfs_realtime_pb2
    logging.info("Sikeresen importálta: pandas, gtfs_realtime_pb2")
except ImportError as e:
    pd = None
    gtfs_realtime_pb2 = None
    logging.error(f"Hiba az importálás során! {e}")
    logging.error("Győződj meg róla, hogy a csomagok a requirements.txt-ben vannak és a Dockerfile telepíti őket.")


@dag(
    dag_id="bkk_hello_world",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["bkk", "test"],
)
def bkk_hello_world_dag():
    """
    Egy egyszerű teszt DAG, ami ellenőrzi, hogy a
    szükséges csomagok települtek-e, és látja-e a DWH kapcsolatot.
    """

    @task
    def check_custom_packages():
        """Ellenőrzi a pandas és a gtfs-rt modulokat."""
        if pd is None or gtfs_realtime_pb2 is None:
            logging.error("A szükséges csomagok nem érhetők el!")
            raise ImportError("pandas vagy gtfs_realtime_pb2 nem importálható")
        logging.info(f"Pandas verzió: {pd.__version__}")
        logging.info(f"GTFS modul elérhető: {gtfs_realtime_pb2.__file__}")

    @task
    def check_dwh_connection():
        """Ellenőrzi a docker-compose-ban definiált DWH kapcsolatot."""
        logging.info("DWH kapcsolat ellenőrzése...")
        try:
            # Ez a Connection ID (bkk_dwh_conn) az AIRFLOW_CONN_BKK_DWH
            # env változóból jön létre a docker-compose.yaml-ban.
            conn = BaseHook.get_connection("bkk_dwh_conn")
            logging.info(f"Sikeres! Kapcsolat neve: {conn.conn_id}")
            logging.info(f"Host: {conn.host}")
            logging.info(f"Adatbázis: {conn.schema}")
            logging.info(f"Port: {conn.port}")
            # A jelszót sose logoljuk!
            return True
        except Exception as e:
            logging.error(f"Nem sikerült lekérni a 'bkk_dwh_conn' kapcsolatot: {e}")
            logging.error("Ellenőrizd az AIRFLOW_CONN_BKK_DWH környezeti változót a docker-compose.yaml-ban!")
            raise e

    # Futtatjuk a taskokat
    check_custom_packages()
    check_dwh_connection()

# Meghívjuk a DAG függvényt
bkk_hello_world_dag()