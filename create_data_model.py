from gcp import bigquery_helper
from decouple import config
import logging
import sql
logger = logging.getLogger(__name__)


bigquery = bigquery_helper.BigQueryClient(config('GCP_PROJECT_ID'))
dataset = config('BQ_DATASET')

tables = [
    {
        'name': "hour_dim",
        'sql': sql.hour_dim
    },
    {
        'name': "day_dim",
        'sql': sql.day_dim
    },

]

if __name__ == '__main__':
    logging.basicConfig(level=config('LOG_LEVEL'), format=config('LOG_FORMAT'))
    
    for table in tables:
        logging.info(f"Creating table: {table}")
        bigquery.write_query_results(
            table['sql'], 
            config('BQ_DATASET'), 
            table['name'])
