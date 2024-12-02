import pandas as pd
import json
import numpy as np
import logging
from datetime import datetime


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)

def load_config(config_file="config.json"):
        with open(config_file, 'r') as file:
            config = json.load(file)
        logging.info(f"Configuration loaded from {config_file}")
        return config

def file_import(config):
    customer_config = config["customer_data"]
    sales_config = config["sales_data"]
    
    customer_data = pd.read_csv(customer_config["file_path"])
    sales_data = pd.read_csv(sales_config["file_path"])
    
    if customer_data is None or sales_data is None:
        logging.error("Critical files are missing. Aborting ETL process.")
        return None, None
    return customer_data, sales_data



def main(config_file="config.json"):
    config = load_config(config_file)
    customer_data, sales_data = file_import(config)
    print(customer_data, "\n" ,sales_data)

if __name__ == '__main__':
    main()