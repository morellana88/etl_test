from gcp import bigquery_helper
from decouple import config
import argparse
import logging
logger = logging.getLogger(__name__)


def make_parser():
    parser = argparse.ArgumentParser(description='Load a parquet file from GCS to BigQuery')
    parser.add_argument(
        '-o',
        '--gcs_object',
        required=True, 
        help='A valid GCS location, bucket does not need to be part of the path'
    )
    parser.add_argument( 
        '-t',
        '--table', 
        required=True, 
        help='The table name to be created/replaced in BigQuery')
    parser.add_argument(
        '-d',
        '--dataset',
        default=config("BQ_DATASET"),
        help='An existing dataset id, optional if does not specified the enviroment variable will be used'
    )
    parser.add_argument(
        '-p',
        '--project_id',
        default=config("GCP_PROJECT_ID"),
        help='Add a valid GCP project_id, optional if does not specified the enviroment variable will be used'
    )
    parser.add_argument(
        '-b',
        '--bucket',
        default=config("GCS_BUCKET"),
        help='Add a valid GCS bucket, optional if does not specified the enviroment variable will be used'
    )

    return(parser.parse_args())

if __name__ == '__main__':
    logging.basicConfig(level=config('LOG_LEVEL'), format=config('LOG_FORMAT')) 
    args= make_parser()
    bigquery = bigquery_helper.BigQueryClient(args.project_id)
    bigquery.load_parquet(
            f'gs://{args.bucket}/{args.gcs_object}',
            args.dataset,
            args.table,
        )
