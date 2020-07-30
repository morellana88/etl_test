import sys
import argparse
from decouple import config
import transformations
import logging
logger = logging.getLogger(__name__)

def make_parser():
    parser = argparse.ArgumentParser(description='Apply transformations to a local file based on the selection to parquet file')
    parser.add_argument(
        '-t', '--table', required=True, choices=['order_details', 'products'], help='Choose on option to apply the proper transformations'
    )
    parser.add_argument(
        '-f', '--filename', required=True, help='Specify the path of a local file to process'         
    )
    parser.add_argument(
        '-o', '--gcs_object', required=True, help='A valid GCS location to store the parquet file'
    )
    parser.add_argument(
        '-e', '--csv_encoding', default='latin1', help='The character set to parse a CSV file'
    )
    parser.add_argument(
        '-b', '--bucket', default=config('GCS_BUCKET'), help='Add a valid GCS bucket, optional if does not specified the enviroment variable will be used'
    )
    return(parser.parse_args())


if __name__ == '__main__':
    logging.basicConfig(level=config('LOG_LEVEL'), format=config('LOG_FORMAT')) 
    args= make_parser()
    
    if args.table == 'order_details':
        transformations.order_details(
            args.filename, args.bucket, args.gcs_object, csv_encoding=args.csv_encoding)
    elif args.table == 'products':
        transformations.products(
            args.filename, args.bucket, args.gcs_object)
