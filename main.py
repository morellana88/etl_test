import logging
import sys
import argparse
from decouple import config
from transformation import order_details


def cmdline_args():
    p = argparse.ArgumentParser(description='Specify the data file to process')
    p.add_argument(
        '-f', '--filename', required=True, type=str, help='Add a valid path'         
    )
    return(p.parse_args())


if __name__ == '__main__':
    try:
        args = cmdline_args()
        order_details(
            args.filename, config('CSV_ENCODING'), config('GCS_BUCKET'), 'orders/order_details.parquet')
    except:
        print('Try $python <script_name> "Hello" 123 --enable')