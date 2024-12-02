import pandas as pd
import json
from sqlalchemy import create_engine
import logging
import Take_Home_ETL

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def load_config(config_file="config.json"):
    try:
        with open(config_file, 'r') as file:
            config = json.load(file)
        logging.info(f"database config loaded from {config_file}")
        return config.get("database")
    except FileNotFoundError:
        logging.error(f"configuration file not found {config_file}")
        return None
    except json.JSONDecodeError as e:
        logging.error(f"error checking configuration file: {e}")
        return None
    
def get_db_engine(config_file="config.json"):
    db_config = load_config(config_file)
    if db_config is None:
        logging.error("failed to load database configuration. Exiting.")
        return None

    engine_url = f"postgresql+pg8000://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
    return create_engine(engine_url)


def load_data_to_postgres(engine, table_name, dataframe, key_column=None):
    if dataframe.empty:
        logging.warning(f"No data to load for {table_name}")
        return
    try:
        with engine.begin() as conn:
            if key_column is not None:
                existing_data = pd.read_sql(f"SELECT {key_column} FROM {table_name}", conn)
                delta_data = dataframe[~dataframe[key_column].isin(existing_data[key_column])]
                print(delta_data)
            else:
                dataframe.to_sql(table_name, conn, index=False, if_exists='replace')
                logging.info(f"Replaced all records in {table_name}.")
    except Exception as e:
        logging.error(f"failed to load data into {table_name}: {e}")

def main():
    customer_data, sales_data, sales_summary = Take_Home_ETL.main()
    print(customer_data, "\n \n \n", sales_data, "\n \n \n", sales_summary)
    engine = get_db_engine()
    load_data_to_postgres(engine, 'customer_data', customer_data,'customer_id')
if __name__ == '__main__':
    main()
