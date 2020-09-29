from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class S3ToRedshiftOperator(BaseOperator):

    @apply_defaults
    def __init__(self, redshift_conn_id, table, s3_bucket, s3_folder_path, s3_access_key_id,
                 s3_secret_access_key, delimiter, region, *args, **kwargs):
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_folder_path = s3_folder_path
        self.s3_access_key_id = s3_access_key_id
        self.s3_secret_access_key = s3_secret_access_key
        self.delimiter = delimiter
        self.region = region

        super(S3ToRedshiftOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        try:
            self.hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
            conn = self.hook.get_conn()
            cursor = conn.cursor()
            self.log.info("Connected with " + self.redshift_conn_id)

            # get the folder for the current execution date
            exec_date = context['ds_nodash']

            load_statement = """
            copy
            {0}
            from 's3://{1}/{2}/{3}'
            access_key_id '{4}' secret_access_key '{5}'
            delimiter '{6}' region '{7}' """.format(
                self.table, self.s3_bucket, self.s3_folder_path, exec_date,
                self.s3_access_key_id, self.s3_secret_access_key,
                self.delimiter, self.region)

            cursor.execute(load_statement)
            cursor.close()
            conn.commit()
            self.log.info("Load command completed")

            return True
        except Exception as e:
            self.log.error('Error:{}'.format(e))
