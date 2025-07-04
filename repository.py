from dagster import Definitions
from dagster_etl_jobs import etl_initial_job, mock_comment_job
from schedules import mock_comment_schedule
from resources import config_resource

defs = Definitions(
    jobs=[etl_initial_job, mock_comment_job],
    schedules=[mock_comment_schedule],
    resources={"config": config_resource}
    
)
