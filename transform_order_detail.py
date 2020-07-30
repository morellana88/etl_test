import pandas as pd
import numpy as np
import headers
import mappers
from decouple import config
from cmd_args import cmdline_args
import logging
logger = logging.getLogger(__name__)

def main(filepath, csv_encoding, gcs_bucket, gcs_object):
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


if __name__ == '__main__':
    logging.basicConfig(level=config('LOG_LEVEL'), format=config('LOG_FORMAT'))
    args=cmdline_args()
    main(
        args.filename, 
        config('CSV_ENCODING'), 
        config('GCS_BUCKET'), 
        config('GCS_ORDER_DETAIL_OBJECT'))
