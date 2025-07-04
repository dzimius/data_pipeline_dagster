from dagster import schedule
from dagster_etl_jobs import mock_comment_job

@schedule(cron_schedule="*/1 * * * *", job=mock_comment_job, execution_timezone="UTC")
def mock_comment_schedule(_context):
    return {}