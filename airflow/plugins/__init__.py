from airflow.plugins_manager import AirflowPlugin
import operators
import helpers


class UberLogAnalyticsPlugin(AirflowPlugin):
    name = "uber_log_analytics"
    operators = [
        operators.MoveFileToS3,
        operators.DynamicEMRStepsOperator,
        operators.S3ToRedshiftOperator
    ]

    helpers = [
        helpers.SQLQueries
    ]
