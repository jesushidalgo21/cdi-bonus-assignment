import yaml


def load_config():
    with open("config/config.yaml", "r") as file:
        return yaml.safe_load(file)

def get_postgres_connection(pg):
    url = f"jdbc:postgresql://{pg['host']}:{pg['port']}/{pg['database']}"
    connection_props = {
        "user": pg["user"],
        "password": pg["password"],
        "driver": pg["driver"]
    }
    return url, connection_props