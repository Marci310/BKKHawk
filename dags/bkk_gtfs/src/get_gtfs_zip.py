import datetime
import hashlib
import logging
import requests
import pathlib



DIR_PATH = pathlib.Path("./bkk_zips")


def get_latest_file_name():
    files = list(DIR_PATH.glob("budapest_gtfs_*.zip"))
    if not files:
        return None
    latest_file = max(files, key=lambda f: f.stat().st_mtime)
    return latest_file.name


def get_gtfs_zip(**kwargs):
    gtfs_url = f"https://go.bkk.hu/api/static/v1/public-gtfs/budapest_gtfs.zip"
    pathlib.Path(DIR_PATH).mkdir(parents=True, exist_ok=True)

    response = requests.get(gtfs_url)
    response.raise_for_status()
    content = response.content

    checksum = hashlib.sha256(content).hexdigest()

    latest_file = get_latest_file_name()

    if latest_file:
        existing = hashlib.sha256((DIR_PATH / latest_file).read_bytes()).hexdigest()
        if existing == checksum:
            logging.info("GTFS zip unchanged; reusing existing file.")
            kwargs['ti'].xcom_push(key="gtfs_zip_path", value=str(DIR_PATH / latest_file))
            return

    local_zip_path = DIR_PATH / f"budapest_gtfs_{datetime.datetime.today().strftime('%Y%m%d')}.zip"
    local_zip_path.write_bytes(content)
    logging.info(f"GTFS data downloaded and saved to {local_zip_path}")
    kwargs['ti'].xcom_push(key="gtfs_zip_path", value=str(local_zip_path))


if __name__ == "__main__":
    get_gtfs_zip()
