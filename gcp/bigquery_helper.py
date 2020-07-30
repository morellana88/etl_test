from google.cloud import bigquery
import logging
logger = logging.getLogger(__name__)

class BigQueryClient():
    _conn = None

    def __init__(self, project_id):
        self.project_id= project_id

    def get_conn(self):
        if not self._conn:
            self._conn = bigquery.Client(project=self.project_id)

        return self._conn

    def load_parquet(self, gcs_uri, dataset, table):
        conn = self.get_conn()
        dataset_ref = conn.dataset(dataset)
        job_config = bigquery.LoadJobConfig()
        job_config.source_format = bigquery.SourceFormat.PARQUET
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        job_config.create_disposition = bigquery.CreateDisposition.CREATE_IF_NEEDED

        load_job = conn.load_table_from_uri(
            gcs_uri, dataset_ref.table(table), job_config=job_config
        )
        logger.info("Starting job {}".format(load_job.job_id))
        # Waits for the job result
        load_job.result()
        table = conn.get_table(dataset_ref.table(table))
        logger.info("Loaded {} rows.".format(table.num_rows))

    def write_query_results(self, sql, dataset, table):
        conn = self.get_conn()
        job_config = bigquery.QueryJobConfig(
            destination=f"{self.project_id}.{dataset}.{table}", use_legacy_sql=False)
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        query_job = conn.query(sql, job_config=job_config)
        query_job.result()
        logger.info("Query results loaded to the table {}".format(table))
