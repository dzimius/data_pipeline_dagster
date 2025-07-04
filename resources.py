from dagster import resource
import config

@resource
def config_resource(_):
    return {
        "DRIVER": config.DRIVER,
        "SERVER": config.SERVER,
        "DATABASE": config.DATABASE,
        "COMMENTS_API_URL": config.COMMENTS_API_URL,
        "POSTS_API_URL": config.POSTS_API_URL,
    }