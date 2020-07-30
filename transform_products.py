import pandas as pd
import numpy as np
import json
import headers
from decouple import config
from cmd_args import cmdline_args
import getpass
from datetime import datetime
import logging
logger = logging.getLogger(__name__)

def main(filepath, gcs_bucket, gcs_object):
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


if __name__ == '__main__':
    logging.basicConfig(level=config('LOG_LEVEL'), format=config('LOG_FORMAT'))
    args=cmdline_args()
    main(
        args.filename, 
        config('GCS_BUCKET'), 
        config('GCS_PRODUCTS_OBJECT'))
