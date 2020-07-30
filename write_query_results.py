from gcp import bigquery_helper
from decouple import config
import logging
import sql
import argparse
import os
logger = logging.getLogger(__name__)


def is_valid_file(parser, arg):
      if not os.path.exists(arg):
           parser.error(f'The file {arg} does not exist! Use the --help flag for input options.' )
      else:
           return arg


def make_parser():
    parser = argparse.ArgumentParser(description='Write query results to a destination table')
    parser.add_argument(
        '-t', '--table', required=True, type=str , help='Destination table name to create or replace'
    )
    parser.add_argument(
        '-s', '--sql', required=True, type=argparse.FileType('r'), 
        help='The sql filename to execute'
    )
    parser.add_argument(
        '-d', '--dataset', 
        default=config("BQ_DATASET"), 
        help='An existing dataset id, optional if does not specified the enviroment variable will be used'
    )
    parser.add_argument(
        '-p',
        '--project_id',
        default=config("GCP_PROJECT_ID"),
        help='Add a valid GCP project_id, optional if does not specified the enviroment variable will be used'
    )    
    return(parser.parse_args())

if __name__ == '__main__':
    logging.basicConfig(level=config('LOG_LEVEL'), format=config('LOG_FORMAT')) 
    args= make_parser()
    bigquery = bigquery_helper.BigQueryClient(args.project_id)
    bigquery.write_query_results(
            args.sql.read(),
            args.dataset,
            args.table)