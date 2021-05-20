import configparser

config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

# Assign s3 paths to variables
log_data = config.get('S3','log_data')
log_data_json = config.get('S3','log_data_json')
song_data = config.get('S3','song_data')
arn = config.get('IAM','arn')

# Drop tables
staging_events_table_drop = "DROP TABLE IF EXISTS events_stg;"
staging_songs_table_drop = "DROP TABLE IF EXISTS songs_stg;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays_fact;"
user_table_drop = "DROP TABLE IF EXISTS users_dim;"
song_table_drop = "DROP TABLE IF EXISTS songs_dim;"
artist_table_drop = "DROP TABLE IF EXISTS artists_dim;"
time_table_drop = "DROP TABLE IF EXISTS time_dim;"

# Create table statements
staging_events_table_create = ("""
CREATE TABLE IF NOT EXISTS events_stg (
    artist VARCHAR,
    auth VARCHAR,
    firstName VARCHAR,
    gender VARCHAR,
    itemInSession SMALLINT,
    lastName VARCHAR,
    length FLOAT,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration VARCHAR,
    sessionId SMALLINT,
    song VARCHAR,
    status SMALLINT,
    ts BIGINT,
    userAgent VARCHAR,
    userId SMALLINT 
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS songs_stg (
    num_songs SMALLINT,
    artist_id VARCHAR,
    artist_latitude FLOAT,
    artist_longitude FLOAT,
    artist_location VARCHAR,
    artist_name VARCHAR,
    song_id VARCHAR,
    title VARCHAR,
    duration FLOAT,
    year SMALLINT 
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays_fact (
    songplay_id INT IDENTITY(1,1),
    start_time TIMESTAMP NOT NULL,
    user_id SMALLINT,
    level VARCHAR,
    song_id VARCHAR,
    artist_id VARCHAR,
    session_id SMALLINT,
    location VARCHAR,
    user_agent VARCHAR,
    PRIMARY KEY(songplay_id),
    FOREIGN KEY(user_id) REFERENCES users_dim(user_id),
    FOREIGN KEY(song_id) REFERENCES songs_dim(song_id),
    FOREIGN KEY(artist_id) REFERENCES artists_dim(artist_id),
    FOREIGN KEY(start_time) REFERENCES time_dim(start_time)) DISTKEY(user_id) SORTKEY(start_time); 
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users_dim (
    user_id SMALLINT NOT NULL,
    first_name VARCHAR,
    last_name VARCHAR,
    gender VARCHAR,
    level VARCHAR,
    PRIMARY KEY(user_id)) DISTKEY(user_id) SORTKEY(user_id);
 
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs_dim (
    song_id VARCHAR NOT NULL,
    title VARCHAR NOT NULL,
    artist_id VARCHAR NOT NULL,
    year SMALLINT,
    duration FLOAT,
    PRIMARY KEY(song_id)) DISTKEY(song_id) SORTKEY(song_id);
""")

artist_table_create = ("""
 CREATE TABLE IF NOT EXISTS artists_dim (
    artist_id VARCHAR NOT NULL,
    artist_name VARCHAR NOT NULL,
    location VARCHAR,
    lattitude FLOAT,
    longitude FLOAT,
    PRIMARY KEY(artist_id)) DISTKEY(artist_id) SORTKEY(artist_id);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time_dim (
    start_time TIMESTAMP NOT NULL,
    hour SMALLINT NOT NULL,
    day SMALLINT NOT NULL,
    week SMALLINT NOT NULL,
    month SMALLINT NOT NULL,
    year SMALLINT NOT NULL,
    weekday SMALLINT NOT NULL,
    PRIMARY KEY(start_time)) DISTKEY(start_time) SORTKEY(start_time);
""")

# Copy data from S3 buckets into staging tables
staging_events_copy = ("""
COPY events_stg FROM {}
CREDENTIALS 'aws_iam_role={}'
JSON {}
REGION 'us-west-2'
""").format(log_data, arn, log_data_json)

staging_songs_copy = ("""
COPY songs_stg FROM {}
CREDENTIALS 'aws_iam_role={}'
JSON 'auto'
REGION 'us-west-2'
""").format(song_data, arn)

# Insert transformed data into final tables from staging tables
songplay_table_insert = ("""
INSERT INTO songplays_fact (
    start_time,
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    location,
    user_agent
)SELECT
    TIMESTAMP 'epoch' + (es.ts/1000
    ) * interval '1 second' AS start_time, es.userid, es.level, ss.song_id, ss.artist_id, es.sessionid, es.location, es.useragent
FROM
    events_stg es
    LEFT JOIN songs_stg ss ON (es.song = ss.title       
       AND es.artist = ss.artist_name
       AND es.length = ss.duration)
WHERE (es.page = 'NextSong'
       AND ss.song_id IS NOT NULL
       AND ss.artist_id IS NOT NULL);
""")

user_table_insert = ("""
INSERT INTO users_dim (
    user_id, 
    first_name,
    last_name,
    gender,
    level
)
SELECT DISTINCT userid,
    firstName,
    lastName,
    gender,
    level
FROM events_stg es
WHERE ts = (SELECT MAX(ts) FROM events_stg es2 WHERE es2.userid = es.userid)
AND page = 'NextSong'
""")

song_table_insert = ("""
INSERT INTO songs_dim (
    song_id, 
    title,
    artist_id,
    year,
    duration
)
SELECT DISTINCT
    song_id, 
    title,
    artist_id,
    year,
    duration
FROM songs_stg
WHERE song_id NOT IN (SELECT DISTINCT(song_id) FROM songs_dim)
AND song_id IS NOT NULL
AND artist_id IS NOT NULL
AND title IS NOT NULL
""")

artist_table_insert = ("""
INSERT INTO artists_dim (
        artist_id,
        artist_name,
        location,
        lattitude,
        longitude
)
SELECT DISTINCT artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
FROM songs_stg 
WHERE artist_name NOT IN (SELECT DISTINCT(artist_id) FROM artists_dim)
AND artist_id IS NOT NULL
AND artist_name IS NOT NULL
""")

time_table_insert = ("""
INSERT INTO time_dim(
    start_time,
    hour,
    day,
    week,
    month,
    year,
    weekday
)SELECT
    DISTINCT start_time, EXTRACT(h  
    FROM
        start_time 
    ), EXTRACT(d  
    FROM
        start_time 
    ), EXTRACT(w  
    FROM
        start_time 
    ), EXTRACT(mon  
    FROM
        start_time 
    ), EXTRACT(y  
    FROM
        start_time 
    ), EXTRACT(dow  
    FROM
        start_time 
    ) 
FROM
    (SELECT
        TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time  
    FROM
        events_stg 
    ) 
WHERE
    start_time NOT IN (SELECT
        DISTINCT start_time  
    FROM
        time_dim 
    );
""")

# Query lists to execute in create_tables.py

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]
