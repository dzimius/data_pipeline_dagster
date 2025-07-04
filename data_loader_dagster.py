import requests
import urllib
import pyodbc
import datetime
import random
import math

def extract_comment_values(comment):
    return (
        comment['concat_id'],
        comment['postId'],
        comment['id'],
        comment['name'],
        comment['email'],
        comment['body'],
        comment['valid_start'],
        comment['valid_end'],
        comment['is_valid']
    )

def extract_post_values(post):
    return (
        post['concat_id'],
        post['userId'],
        post['id'],
        post['title'],
        post['body'],
        post['valid_start'],
        post['valid_end'],
        post['is_valid']
    )

def scd_2_query(table_name):
    query = f"""
    WITH VersionPairs AS (
    SELECT
        concat_id,
        valid_start,
        LEAD(valid_start) OVER (PARTITION BY concat_id ORDER BY valid_start) AS next_valid_start,
        COUNT(*) OVER (PARTITION BY concat_id) AS version_count
    FROM dbo.{table_name}
    WHERE is_valid = 1
    )
    UPDATE t
    SET
        valid_end = vp.next_valid_start,
        is_valid = 0
    FROM dbo.{table_name} t
    JOIN VersionPairs vp
        ON t.concat_id = vp.concat_id
        AND t.valid_start = vp.valid_start
    WHERE vp.next_valid_start IS NOT NULL
    AND vp.version_count > 1;
    """
    return query

comments_table_schema = """
CREATE TABLE dbo.stg_comments (
    surrogate_id INT IDENTITY(1,1) PRIMARY KEY,
    concat_id INT,
    postId INT,
    id INT,
    name NVARCHAR(255),
    email NVARCHAR(255),
    body NVARCHAR(MAX),
    valid_start DATETIME2,
    valid_end DATETIME2,
    is_valid BIT

)
"""

posts_table_schema = """
CREATE TABLE dbo.stg_posts (
    surrogate_id INT IDENTITY(1,1) PRIMARY KEY,
    concat_id INT,
    userId INT,
    id INT,
    title NVARCHAR(255),
    body NVARCHAR(MAX),
    valid_start DATETIME2,
    valid_end DATETIME2,
    is_valid BIT
)
"""

id_dict = {
    'stg_comments': 'postId',
    'stg_posts': 'userId'
}

insert_comments_query = """
INSERT INTO dbo.stg_comments (concat_id, postId, id, name, email, body, valid_start, valid_end, is_valid)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

insert_posts_query = """
INSERT INTO dbo.stg_posts (concat_id, userId, id, title, body, valid_start, valid_end, is_valid)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
"""

class DataLoadManager:
    def __init__(self, driver, server, database, batch_size=500):
        self.conn = pyodbc.connect(
            f'DRIVER={driver};'
            f'SERVER={server};'
            f'DATABASE={database};'
        )
        self.cursor = self.conn.cursor()
        self.batch_size = batch_size
        self.counter = 0

    # def create_table_if_not_exists(self, table_name, table_schema_sql):
    #     create_sql = f"""
    #     IF OBJECT_ID('dbo.{table_name}', 'U') IS NULL
    #     BEGIN
    #         {table_schema_sql}
    #     END
    #     """
    #     self.cursor.execute(create_sql)
    #     self.conn.commit()
    #     print(f"[INFO] Table '{table_name}' checked/created.")

    # def truncate_table(self, table_name):
    #     self.cursor.execute(f"TRUNCATE TABLE dbo.{table_name}")
    #     self.conn.commit()
    #     print(f"[INFO] Table '{table_name}' truncated.")
    def drop_table_if_exists(self, table_name):
        drop_sql = f"""
        IF OBJECT_ID('dbo.{table_name}', 'U') IS NOT NULL
        BEGIN
            DROP TABLE dbo.{table_name}
        END
        """
        self.cursor.execute(drop_sql)
        self.conn.commit()
        print(f"[INFO] Table '{table_name}' dropped if it existed.")
        
    def create_table(self, table_name, table_schema_sql):
        self.cursor.execute(table_schema_sql)
        self.conn.commit()
        print(f"[INFO] Table '{table_name}' created.")

    def insert_record(self, table_name, record, insert_query, values_extractor):
        try:
            self.cursor.execute(insert_query, *values_extractor(record))
            self.counter += 1
            if self.counter % self.batch_size == 0:
                self.conn.commit()
        except Exception as e:
            print(f"[ERROR] Failed to insert record into {table_name}: {e}")

    def fetch_data_from_api(self, url):
        response = requests.get(url)
        response.raise_for_status()
        print(f"[INFO] Data fetched from {url}")
        self.fetched_time=datetime.datetime.now()
        data = response.json()
        for row in data:
            row['concat_id'] = str(row[list(row.keys())[0]]) + str(row[list(row.keys())[1]])
        return data

    def load_data(self, table_name, data, insert_query, values_extractor):
        self.counter = 0  # reset counter per table
        for record in data:
            self.insert_record(table_name, record, insert_query, values_extractor)
        self.conn.commit()
        print(f"[INFO] {self.counter} records loaded into '{table_name}'.")

    def close_connection(self):
        self.cursor.close()
        self.conn.close()
        print("[INFO] Database connection closed.")

    def apply_valid_cols(self, data, fetched_time):
        for row in data:
            row['valid_start'] = fetched_time
            row['valid_end'] = '9999-12-31'
            row['is_valid'] = 1
        return data
    
    def apply_scd_2(self, table_name):
        self.cursor.execute(scd_2_query(table_name))
        self.conn.commit()

    def get_max_id(self, table_name):
        self.cursor.execute(f'SELECT max(id) from {table_name}')
        max_id = int(self.cursor.fetchone()[0])
        return max_id
        
    
    def get_max_item_id(self, table_name):
        column_name = id_dict[table_name]
        self.cursor.execute(f'SELECT max({column_name}) from dbo.{table_name}')
        max_id = int(self.cursor.fetchone()[0])
        return max_id
    
    def fetch_mock_comments_new(self, max_id, max_post_id):
        inc_id = max_id + 1
        post_id = max_post_id + 1
        new_comment = {
                    'concat_id': str(post_id) + str(inc_id),
                    'postId': post_id,
                    'id': inc_id,
                    'name': f'new name post_id: {post_id}',
                    'email': 'new_email@mail.com',
                    'body': 'new body'
        }
        self.fetched_time=datetime.datetime.now()
        return new_comment
            
    def fetch_mock_comments_edited(self, max_id):
        inc_id = random.choice(range(1, max_id + 1))
        post_id = math.ceil(inc_id/5)
        edited_comment= {
            'concat_id': str(post_id) + str(inc_id),
            'postId': post_id,
            'id': inc_id,
            'name': f'edited name post_id: {post_id}',
            'email': 'edited_email@mail.com',
            'body': 'edited body'
        }
        return edited_comment
