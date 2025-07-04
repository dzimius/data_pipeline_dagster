from dagster import op
import config
from data_loader_dagster import (
    DataLoadManager, extract_comment_values, extract_post_values,
    insert_comments_query, insert_posts_query,
    comments_table_schema, posts_table_schema
)

@op(required_resource_keys={"config"})
def create_tables(context):
    cfg = context.resources.config
    loader = DataLoadManager(cfg["DRIVER"], cfg["SERVER"], cfg["DATABASE"])
    loader.drop_table_if_exists("stg_comments")
    loader.drop_table_if_exists("stg_posts")
    loader.create_table("stg_comments", comments_table_schema)
    loader.create_table("stg_posts", posts_table_schema)

    loader.close_connection()

@op(required_resource_keys={"config"})
def fetch_comments(context):
    cfg = context.resources.config
    loader = DataLoadManager(cfg["DRIVER"], cfg["SERVER"], cfg["DATABASE"])
    data = loader.fetch_data_from_api(config.COMMENTS_API_URL)
    fetched_time = loader.fetched_time
    loader.close_connection()
    return {"data": data, "fetched_time": fetched_time}

@op(required_resource_keys={"config"})
def fetch_posts(context):
    cfg = context.resources.config
    loader = DataLoadManager(cfg["DRIVER"], cfg["SERVER"], cfg["DATABASE"])
    data = loader.fetch_data_from_api(config.POSTS_API_URL)
    fetched_time = loader.fetched_time
    loader.close_connection()
    return {"data": data, "fetched_time": fetched_time}

@op(required_resource_keys={"config"})
def apply_valid(context, payload):
    cfg = context.resources.config
    loader = DataLoadManager(cfg["DRIVER"], cfg["SERVER"], cfg["DATABASE"])
    result = loader.apply_valid_cols(payload["data"], payload["fetched_time"])
    loader.close_connection()
    return result

@op(required_resource_keys={"config"})
def load_comments(context, comments):
    cfg = context.resources.config
    loader = DataLoadManager(cfg["DRIVER"], cfg["SERVER"], cfg["DATABASE"])
    loader.load_data("stg_comments", comments, insert_comments_query, extract_comment_values)
    loader.close_connection()

@op(required_resource_keys={"config"})
def load_posts(context, posts):
    cfg = context.resources.config
    loader = DataLoadManager(cfg["DRIVER"], cfg["SERVER"], cfg["DATABASE"])
    loader.load_data("stg_posts", posts, insert_posts_query, extract_post_values)
    loader.close_connection()

@op(required_resource_keys={"config"})
def generate_mock_comments(context):
    cfg = context.resources.config
    loader = DataLoadManager(cfg["DRIVER"], cfg["SERVER"], cfg["DATABASE"])
    max_comment_id = loader.get_max_id("stg_comments")
    max_post_id = loader.get_max_item_id("stg_comments")
    comments_data = []
    for _ in range(3):
        comments_data.append(loader.fetch_mock_comments_new(max_comment_id, max_post_id))
        max_comment_id += 1
        max_post_id += 1
        comments_data.append(loader.fetch_mock_comments_edited(max_comment_id))
        
    fetched_time = loader.fetched_time
    loader.close_connection()
    return {"data": comments_data, "fetched_time": fetched_time}

@op(required_resource_keys={"config"})
def apply_scd2(context):
    cfg = context.resources.config
    loader = DataLoadManager(cfg["DRIVER"], cfg["SERVER"], cfg["DATABASE"])
    loader.apply_scd_2("stg_comments")
    loader.apply_scd_2("stg_posts")
    loader.close_connection()