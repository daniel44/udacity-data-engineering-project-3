import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE staging_events(
        artist VARCHAR(255),
        auth VARCHAR(100),
        firstName VARCHAR(100),
        gender VARCHAR(100),
        itemInSession INTEGER,
        lastName VARCHAR(100),
        length FLOAT,
        level VARCHAR(100),
        location VARCHAR(100),
        method VARCHAR(100),
        page VARCHAR(100),
        registration FLOAT,
        sessionId INTEGER,
        song VARCHAR(255),
        status INTEGER,
        ts VARCHAR(100),
        userAgent VARCHAR(255),
        userId INTEGER 
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs(
        num_songs INTEGER,
        artist_id VARCHAR(100) NOT NULL,
        artist_latitude FLOAT,
        artist_longitude FLOAT,
        artist_location VARCHAR(255),
        artist_name VARCHAR(255),
        song_id VARCHAR(100) NOT NULL,
        title VARCHAR(255),
        duration FLOAT,
        year INTEGER
    )
""")

songplay_table_create = ("""
    CREATE TABLE songplays(
        songplay_id INTEGER IDENTITY(0,1),
        start_time TIMESTAMP,
        user_id INTEGER,
        level VARCHAR(100),
        song_id VARCHAR(100),
        artist_id VARCHAR(255),
        session_id INTEGER,
        location VARCHAR(100),
        user_agent VARCHAR(255)
    )
""")

user_table_create = ("""
    CREATE TABLE users(
        user_id INTEGER NOT NULL,
        first_name VARCHAR(100) NOT NULL,
        last_name VARCHAR(100) NOT NULL,
        gender VARCHAR(1) NOT NULL,
        level VARCHAR(100) NOT NULL
    )
""")

song_table_create = ("""
    CREATE TABLE songs(
        song_id VARCHAR(100) NOT NULL,
        title VARCHAR(255) NOT NULL,
        artist_id VARCHAR(100) NOT NULL,
        year INTEGER NOT NULL,
        duration FLOAT
    )
""")

artist_table_create = ("""
    CREATE TABLE artists(
        artist_id VARCHAR(100) NOT NULL,
        name VARCHAR(255) NOT NULL,
        location VARCHAR(255),
        latitude FLOAT,
        longitude FLOAT
    )
""")

time_table_create = ("""
    CREATE TABLE time(
        start_time TIMESTAMP NOT NULL,
        hour INTEGER NOT NULL,
        day INTEGER NOT NULL,
        week INTEGER NOT NULL,
        month INTEGER NOT NULL,
        year INTEGER NOT NULL,
        weekday INTEGER NOT NULL
    )
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from {log_data}
    credentials 'aws_iam_role={arn}'
    region 'us-west-2'
    format as JSON {log_jsonpath};
""").format(log_data=config['S3']['LOG_DATA'], arn=config['IAM_ROLE']['ARN'], log_jsonpath=config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
    copy staging_songs from {song_data}
    credentials 'aws_iam_role={arn}'
    region 'us-west-2'
    format as JSON 'auto';
""").format(song_data=config['S3']['SONG_DATA'], arn=config['IAM_ROLE']['ARN'])

# FINAL TABLES

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT userId,
            firstName,
            lastName,
            gender,
            level
    FROM staging_events
    WHERE userId IS NOT NULL
    AND page = 'NextSong';
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT  DISTINCT song_id,
            title,
            artist_id,
            year,
            duration
    FROM staging_songs
    WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT  DISTINCT artist_id,
            artist_name,
            artist_location,
            artist_latitude,
            artist_longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT  DISTINCT start_time,
            EXTRACT(hour FROM start_time),
            EXTRACT(day FROM start_time),
            EXTRACT(week FROM start_time),
            EXTRACT(month FROM start_time),
            EXTRACT(year FROM start_time),
            EXTRACT(weekday FROM start_time)
    FROM (
            SELECT DISTINCT TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as start_time 
            FROM staging_events
        );
""")

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT  DISTINCT TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second', 
            e.userId, 
            e.level, 
            s.song_id, 
            s.artist_id, 
            e.sessionId, 
            e.location, 
            e.userAgent
    FROM staging_events e
    JOIN staging_songs s ON e.song = s.title AND e.artist = s.artist_name
    AND e.page = 'NextSong'
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
