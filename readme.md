# ETL technical test

**Requirements for local deployment:**
- Python 3.7
- GCC compiler, this is used by pyarrow and is required only on local mode
- A Goolge Cloud Service account with the following roles:
        - BigQuery admin
        - Storage admin

**Pre-execution steps:**
- Create a new virtual enviroment (optional)
- Restore pip dependencies using the command: `pip3 install requirements.txt`
- Create a local .venv file and make sure the following variables are set:
    -  LOG_LEVEL (optional)
    -  LOG_FORMAT (optional)
    -  GCP_PROJECT_ID, a existing Google Cloud Platform project
    -  GCS_BUCKET, a existing Google Cloud Storage Bucket
    -  BQ_DATASET, a existing BigQuery dataset id
    -  GCP_SERVICE_ACCOUNT, a service account json file placed in the root of the project

**Running the extractions and transformations:**
Using the script *extract_and_transform.py* with the below mandatory parameters:
- **--table**: weather order_details or products to parse and transform the orders csv or the catalog of json products
- **--filename**: The local path of the filename required by the table paramter
- **--gcs_object**: Parquet filename and path on GCS
- 
E.G.
`python extract_and_transform.py --table products --filename /data/products.json --gcs_object orders/products.parquet`

**Running the loads from GCS to BigQuery:**
Using the script *load_parquet_to_bq.py* with the below mandatory parameters:
- **--gcs_object**: The GCS object name used in the previous step
- **--table**: A BigQuery table name to load the content from the parquet file

E.G.
`python load_parquet_to_bq.py --gcs_object orders/products.parquet --table products`


**Creating the dimensional model:**
Using the script *write_query_results.py* with the below mandatory parameters: 
- **--table**: A BigQuery table where the dimension or fact tables are going to be created
- **--sql**: The SQL script to create the dimensions or fact tables


**Building the docker image**
Addionally the ETL has been desined to run on docker for this you only need to make sure that all the local environment variables are set and the service account json file is placed in the root of the project then just run the command: `docker build ` with all of the required arguments.

**Orchestration**
For demo purposes a **docker-compose.yml** has been crated with all of the necessary arguments to orchestrate the pipeline from end to end.
Just run:
`docker-compose -f "docker-compose.yml" up -d --build`
