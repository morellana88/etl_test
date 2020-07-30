import pandas as pd
import numpy as np


# TODO figure out how to do it using python datetime or calendar libraries
dow_mapper={
    'Sunday': 0, 
    'Monday': 1, 
    'Tuesday': 2, 
    'Wednesday': 3, 
    'Thursday': 4, 
    'Friday': 5, 
    'Saturday': 6
}

def dow_converter(day):
    return int(dow_mapper.get(day,day))

raw_schema = ['order_id', 'user_id', 'order_number', 'order_dow', 'order_hour_of_day', 'order_detail']
order_detail_schema = ['product_name', 'aisle', 'product_number']
converters = {'order_dow': dow_converter}

def order_details(filepath, csv_encoding, gcs_bucket, gcs_object):
    # Overriding the default encoding giving issues with problematic characters
    df = pd.read_csv(
            filepath, 
            encoding=csv_encoding,
            header=0, 
            names=raw_schema,
            converters=converters)

    # Split order details string delimited by "~" into a list
    df.order_detail = df.order_detail.apply(lambda x: x.split('~'))
    df=df.explode('order_detail')

    # Add order details columns delimited by "|" 
    df[order_detail_schema] = df.order_detail.str.split("|", expand=True)

    df.drop('order_detail', axis=1, inplace=True)

    # logging.info(df.head(10))
    # logging.info(df.info())

    # Store transformed results into a distributed storage
    df.to_parquet(f"gs://{gcs_bucket}/{gcs_object}", index=False)
