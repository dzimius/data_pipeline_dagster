from dagster import job
from dagster_ops import *

@job
def etl_initial_job():
    #loader = init_loader()
    
    create_tables()

    comments = fetch_comments()
    valid_comments = apply_valid(comments)
    load_comments(valid_comments)

    posts = fetch_posts()
    valid_posts = apply_valid(posts)
    load_posts(valid_posts)

@job
def mock_comment_job():
    #loader = init_loader()
    mock_comments = generate_mock_comments()
    valid_mock_comments = apply_valid(mock_comments)
    load_comments(valid_mock_comments)

    apply_scd2()