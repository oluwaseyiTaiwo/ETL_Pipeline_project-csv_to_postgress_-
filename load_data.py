import json
import Take_Home_ETL
from sqlalchemy import create_engine
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)



def main():
    customer_data, sales_data, sales_summary = Take_Home_ETL.main()
    print(customer_data, "\n \n \n", sales_data, "\n \n \n", sales_summary)


if __name__ == '__main__':
    main()
