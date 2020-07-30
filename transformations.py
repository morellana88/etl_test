import pandas as pd
import numpy as np
import headers
import mappers
import getpass
import json
from datetime import datetime
import logging
logger = logging.getLogger(__name__)

def order_details(filepath, gcs_bucket, gcs_object, csv_encoding=None):
    logger.info(f"Reading CSV from: {filepath}")
    # Overriding the default encoding giving issues with problematic characters
    df = pd.read_csv(
            filepath, 
            encoding=csv_encoding,
            header=0, 
            names=headers.order_detail,
            converters= {'order_dow': mappers.dow_converter})

    # Split order details string delimited by "~" into a list
    df.order_detail = df.order_detail.apply(lambda x: x.split('~'))
    df=df.explode('order_detail')

    # Add order details columns delimited by "|" 
    df[headers.product_detail] = df.order_detail.str.split("|", expand=True)

    df.drop('order_detail', axis=1, inplace=True)

    logging.debug(f"Showing first 10 records:\n {df.head(10)}")
    
    # Store transformed results into a distributed storage
    df.to_parquet(f"gs://{gcs_bucket}/{gcs_object}", index=False)


def products(filepath, gcs_bucket, gcs_object):
    logger.info(f"Reading JSON from: {filepath}")
    
    with open(filepath) as f:
        data = json.load(f)

    df = pd.DataFrame(data['results'][0]['items'], columns=headers.products)
    
    # Emulate network user using operative system user
    username = getpass.getuser()
    logging.info(f"Catalog updated by: {username}")
    df['added_by'] = username
    df['added_timestamp'] = datetime.now()

    logging.debug(f"Showing first 10 records:\n {df.head(10)}")

    # Store transformed results into a distributed storage
    df.to_parquet(f"gs://{gcs_bucket}/{gcs_object}", index=False)
