from airflow.operators import BaseOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.utils.decorators import apply_defaults


class MoveFileToS3(BaseOperator):

    @apply_defaults
    def __init__(self,
                 file_name="",
                 key="",
                 bucket_name="",
                 *args, **kwargs):
        super(MoveFileToS3, self).__init__(*args, **kwargs)
        self.file_name = file_name
        self.key = key
        self.bucket_name = bucket_name

    def execute(self, context):
        try:
            self.log.info('Loading file {} to s3 bucket {}'.format(self.file_name, self.bucket_name))
            s3 = S3Hook()
            s3.load_file(filename=self.file_name, replace=True, bucket_name=self.bucket_name, key=self.key)
            self.log.info('Successfully loaded files into s3')
        except Exception as e:
            self.log.error('Error: {}'.format(e))
