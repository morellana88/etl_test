from gcp import bigquery_helper
from decouple import config
import logging
logger = logging.getLogger(__name__)


bigquery = bigquery_helper.BigQueryClient(config('GCP_PROJECT_ID'))
tables = ['order_details', 'products']

if __name__ == '__main__':
    logging.basicConfig(level=config('LOG_LEVEL'), format=config('LOG_FORMAT'))
    
    for table in tables:
        gcs_uri = f'gs://{config("GCS_BUCKET")}/orders/{table}.parquet'
        logging.info("Loading file")
        bigquery.load_parquet(
            gcs_uri, 
            config('BQ_DATASET'), 
            table)
