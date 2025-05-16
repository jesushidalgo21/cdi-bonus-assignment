import yaml
import argparse
from datetime import datetime

def load_config():
    try:
        with open("config/config.yaml", "r") as file:
            return yaml.safe_load(file)
    except Exception as e:
        raise RuntimeError(f"Error inesperado al cargar la configuración: {e}")

def get_postgres_connection(pg):
    url = f"jdbc:postgresql://{pg['host']}:{pg['port']}/{pg['database']}"
    connection_props = {
        "user": pg["user"],
        "password": pg["password"],
        "driver": pg["driver"]
    }
    return url, connection_props

def parse_args(description):
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--start", type=str, required=True, help="Fecha de inicio (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, help="Fecha de fin (YYYY-MM-DD)")
    return parser.parse_args()

def validate_dates(start, end):
    try:
        start_date = datetime.strptime(start, "%Y-%m-%d").date()
        end_date = datetime.strptime(end or start, "%Y-%m-%d").date()
        if start_date > end_date:
            raise ValueError("La fecha de inicio no puede ser posterior a la de fin")
        return str(start_date), str(end_date)
    except ValueError as e:
        raise ValueError(f"❌ Fechas inválidas: {e}")