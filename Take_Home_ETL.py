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
    try:
        with open(config_file, 'r') as file:
            config = json.load(file)
        logging.info(f"Configuration loaded from {config_file}")
        return config
    except FileNotFoundError:
        logging.error(f"Configuration file not found: {config_file}")
        return None
    except json.JSONDecodeError as e:
        logging.error(f"Error parsing configuration file: {e}")
        return None
    

def file_import(config):
    customer_config = config["customer_data"]
    sales_config = config["sales_data"]
    
    customer_data = read_csv_file(customer_config["file_path"], set(customer_config["required_columns"]))
    sales_data = read_csv_file(sales_config["file_path"], set(sales_config["required_columns"]))
    
    if customer_data is None or sales_data is None:
        logging.error("Critical files are missing. Aborting ETL process.")
        return None, None

    customer_data = standardize_data(customer_data, ['signup_date'], ['customer_id'])
    sales_data = standardize_data(sales_data, ['order_date'], ['customer_id', 'order_id'])
    return customer_data, sales_data


def file_filters(file_data):
    customer_data = file_data[0]
    sales_data = file_data[1]

    filtered_customer_data = customer_data[(customer_data['customer_id'].notna()) & (customer_data['signup_date'].notna())].copy()
    filtered_sales_data = sales_data[(sales_data['customer_id'].notna()) & (sales_data['customer_id'].isin(filtered_customer_data['customer_id'])) & (sales_data['quantity'] >= 1) & (sales_data['order_date'].notna()) & (sales_data['order_id'].notna()) ].copy()

    filtered_customer_data['customer_tenure'] = ((datetime.now() - filtered_customer_data['signup_date']).dt.days).astype(int)
    filtered_sales_data['total_value'] = pd.to_numeric(filtered_sales_data['quantity'], errors='coerce') * pd.to_numeric((filtered_sales_data['price']), errors='coerce')
    filtered_sales_data['order_type'] = np.where(filtered_sales_data['total_value'] > 1000, "High-Value Order", "Regular Order")
    
    filtered_customer_data['customer_id'] = pd.to_numeric(filtered_customer_data["customer_id"], errors='coerce').astype(int)
    filtered_sales_data['order_id'] = pd.to_numeric(filtered_sales_data["order_id"], errors='coerce').astype(int)


    filtered_sales_data = pd.merge(filtered_sales_data,filtered_customer_data[['customer_id', 'customer_name', 'email','customer_tenure']], on='customer_id', how='left')
    
    return filtered_customer_data, filtered_sales_data


def read_csv_file(file_name, required_columns):
    try:
        data = pd.read_csv(file_name)
        logging.info(f"Successfully loaded {file_name}")

        if not validate_columns(data, required_columns, file_name):
            return None
        return data
    except FileNotFoundError:
        logging.error(f"File not found: {file_name}")
    except pd.errors.EmptyDataError:
        logging.error(f"File is empty: {file_name}")
    except Exception as e:
        logging.error(f"Unexpected error reading {file_name}: {e}")
    return None


def validate_columns(data, required_columns, file_name):
    missing_columns = required_columns - set(data.columns)
    if missing_columns:
        logging.error(f"{file_name} is missing columns: {missing_columns}")
        return False
    logging.info(f"All required columns are present in {file_name}")
    return True



def standardize_data(data, date_column,id_columns):
    for date_val in date_column:
        data[date_val] = pd.to_datetime(data[date_val], errors='coerce')
        
    for col_name in id_columns:
        data[col_name] = pd.to_numeric(data[col_name], errors='coerce')

    return data

def main(config_file="config.json"):
    config = load_config(config_file)
    if config is None:
        logging.error("Failed to load configuration. Exiting.")
        return None, None, None

    customer_data, sales_data = file_filters(file_import(config))
    
    if customer_data is not None and sales_data is not None:
        logging.info("Data import and validation successful.")
    else:
        logging.error("Data import failed. Check logs for details.")
        return None, None, None

    
    summary_table = sales_data.groupby('product').agg(total_sales=('total_value', 'sum'),order_count=('order_id', 'count')).reset_index()

    
    return customer_data, sales_data, summary_table

if __name__ == '__main__':
    customer_data, sales_data, summary_table = main()
