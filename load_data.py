import pandas as pd
import json
from sqlalchemy import create_engine
import logging
import Take_Home_ETL
from sqlalchemy.orm import sessionmaker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def load_config(config_file="config.json"):
    """
    Loads the file paths for database info from config.json
    returns a config_object containing the data.
    """
    try:
        with open(config_file, 'r') as file:
            config = json.load(file)
        logging.info(f"database config loaded from {config_file}")
        return config.get("database")
    except FileNotFoundError:
        logging.error(f"configuration file not found: {config_file}")
        return None
    except json.JSONDecodeError as e:
        logging.error(f"error parsing configuration file: {e}")
        return None
    
def get_db_engine(config_file="config.json"):
    """
   Load database and establish connection to PostgreSQL database
    """
    db_config = load_config(config_file)
    if db_config is None:
        logging.error("failed to load database configuration")
        return None

    engine_url = f"postgresql+pg8000://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
    return create_engine(engine_url)


def load_data_to_postgres(engine, table_name, dataframe, key_column=None, batch_size=1000):
    Session = sessionmaker(bind=engine)
    session = Session()

    if dataframe.empty:
        logging.warning(f"No data to load for {table_name}")
        return

    try:
        with engine.begin() as conn:
            if key_column:
                existing_data = pd.read_sql(f"SELECT {key_column} FROM {table_name}", conn)
                delta_data = dataframe[~dataframe[key_column].isin(existing_data[key_column])]
                
                if not delta_data.empty:
                    try:
                        for start in range(0, len(delta_data), batch_size):
                            batch = delta_data.iloc[start:start + batch_size]
                            batch.to_sql(table_name, conn, index=False, if_exists="append")
                            logging.info(f"Inserted batch of {len(batch)} records into {table_name}.")
                        session.commit()
                        logging.info(f"successfully inserted {len(delta_data)} new records into {table_name}.")
                    except Exception as e:
                        session.rollback()
                        logging.error(f"rolled back due to error: {e}")
                else:
                    logging.info(f"no new records to insert into {table_name}.")
            else:
                for start in range(0, len(dataframe), batch_size):
                    batch = dataframe.iloc[start:start + batch_size]
                    batch.to_sql(table_name, conn, index=False, if_exists="replace")
                    logging.info(f"inserted batch of {len(batch)} records into {table_name}.")
                logging.info(f"Replaced all records in {table_name}.")
    except Exception as e:
        logging.error(f"failed to load data into {table_name}: {e}")


def main():
    customer_data, sales_data, sales_summary = Take_Home_ETL.main()
    engine = get_db_engine()

    load_data_to_postgres(engine, 'customer_data', customer_data,'customer_id')
    load_data_to_postgres(engine, 'sales_data', sales_data,"order_id")
    load_data_to_postgres(engine, 'sales_summary', sales_summary)

if __name__ == '__main__':
    main()

