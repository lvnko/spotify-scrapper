import spotipy, os, csv, math
import pandas as pd
from spotipy.oauth2 import SpotifyClientCredentials
from dotenv import load_dotenv
from pathlib import Path
from utils.RandomMachine import RandomMachine
from utils.SQLWriter import SQLWriter
from utils.CSVWriter import CSVWriter
from utils.SpotifyPublicScrapper import SpotifyPublicScrapper
from utils.CSVDataRowsSanitizer import CSVDataRowsSanitizer

# Environment variables setup
environment = os.environ.get("ENVIRONMENT")
envSuffix = f".{environment}" if environment is not None else ''
dotenv_path = f".env{envSuffix}.local"
print(f"Loading environment variables from: {dotenv_path}")
load_dotenv(dotenv_path=dotenv_path, override=True)

repo_path = Path(__file__).parent.resolve()
ph_email_domains = [
  "prosolutions.com",
  "eliteinsights.org",
  "strategicedge.net",
  "globalimpact.co",
  "optimindgroup.biz",
  "venturepinnacle.io",
  "apexinnovate.tech",
  "primeadvisors.info",
  "coreachievers.co",
  "summitascent.org",
  "futureforward.net",
  "quantumleap.com",
  "truenorthconsult.co",
  "elevatesuccess.org",
  "masterfulresults.net",
  "premieralliance.com",
  "synergypath.io",
  "brightfuturesolutions.tech",
  "catalystgrowth.info",
  "infinitepossibilities.biz"
]

# Create a random manchine to provide more data variation
randomer = RandomMachine(ph_email_domains=ph_email_domains)

# Load environment variables
# sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(
#     client_id=os.getenv("SPOTIFY_CLIENT_ID"),
#     client_secret=os.getenv("SPOTIFY_CLIENT_SECRET")
# ))
sp = SpotifyPublicScrapper(
    client_id=os.getenv("SPOTIFY_CLIENT_ID"),
    client_secret=os.getenv("SPOTIFY_CLIENT_SECRET"),
    random_machine=randomer
)

# playlists = sp.user_playlists('spotify')
# print(playlists)

def write_to_csv(file_path=None, data_set=None):
    file_path = file_path if file_path is not None else "data/sample.csv"
    data_set = data_set if data_set is not None else []
    
    if len(data_set) <= 0:
        return False
    
    data_header = data_set[0].keys()
    # Save to CSV
    with open(file_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=data_header)
        writer.writeheader()
        writer.writerows(data_set)
    
    return True

def csv_data_rows_sanitizer(
    input_csv=None,
    columns_to_check=None, action='modify', modify_column=None,
    trim_column=None, max_length=None,
    random_column=None, min_value=None, max_value=None, random_is_integer=True,
    remove_column_name=None, transform_function=None, columns_to_count_on_transform=None,
    empty_column_name=None, empty_extra_rows=0, equal_columns=None
):
    if input_csv is not None:
        handler = CSVDataRowsSanitizer(input_csv)
        if columns_to_check is not None and action in ['modify','remove']:
            modify_column = None if action == 'remove' else columns_to_check[0] if modify_column is None else modify_column
            handler.process(
                columns_to_check=columns_to_check,
                action=action,
                modify_column=modify_column
            )
        if equal_columns:
            handler.process(
                equal_columns=equal_columns
            )
        if max_length is not None and trim_column is not None:
            handler.process(
                trim_column=trim_column,
                max_length=max_length
            )
        if random_column is not None and min_value is not None and max_value is not None:
            handler.process(
                random_column=random_column,
                min_value=min_value,
                max_value=max_value,
                random_is_integer=random_is_integer
            )
        if remove_column_name is not None:
            handler.process(
                remove_column_name=remove_column_name
            )

        if transform_function:
            handler.process(
                transform_function=transform_function,
                columns_to_count_on_transform=columns_to_count_on_transform
            )
        
        if empty_column_name:
            handler.process(
                empty_column_name=empty_column_name,
                empty_extra_rows=empty_extra_rows
            )

def scrap_spotify_top_artist(limit=20):

    results = sp.search(q="top artists", limit=limit, type="artist")
    items = results['artists']['items']
    artists = [{
        "name": item['name'],
        "bio": "",
        "email": f"{item['name'].lower().replace(' ','_')}@{ph_email_domains[index]}",
        "profile_pic": item['images'][0]['url'],
        "spty_url": item['external_urls']['spotify'],
        "spty_uri": item['uri'],
    } for index, item in enumerate(items)]
    # name, bio, email, profile_pic
    result = write_to_csv(
        file_path="data/spotify_artists.csv",
        data_set=artists
    )
    if result is True:
        print(f"Successfully scrapped {limit} Spotify Top Artists!")
    else:
        print(f"Spotify {limit} Top Artists scrapping failed...")

def extract_data_set_users(pool_size=10000, limit=50):
    random_indices = randomer.get_random_nums(offset=0, pool_size=pool_size, len=limit, sorted=True)
    # print('random_indices', random_indices)
    try:
        extracted_df = randomer.extract_csv_rows_pandas(f'{repo_path}/data/SocialMediaUsersDataset.csv', random_indices)
        extracted_df["Profie Pic"] = None

        count_female = extracted_df[extracted_df["Gender"] == "Female"]["Gender"].count()
        random_nums_set_female = randomer.get_random_nums(pool_size=150, len=count_female, sorted=False)
        
        count_male = extracted_df[extracted_df["Gender"] == "Male"]["Gender"].count()
        random_nums_set_male = randomer.get_random_nums(pool_size=150, len=count_male, sorted=False)

        counter = 0
        for _, row in extracted_df[extracted_df["Gender"] == "Female"].iterrows():
            extracted_df.loc[_, "Profie Pic"] = f"https://randomuser.me/api/portraits/women/{random_nums_set_female[counter]}.jpg"
            extracted_df.loc[_, "Email"] = randomer.create_random_email_addr(name=row['Name'])
            counter +=1

        counter = 0
        for _, row in extracted_df[extracted_df["Gender"] == "Male"].iterrows():
            extracted_df.loc[_, "Profie Pic"] = f"https://randomuser.me/api/portraits/men/{random_nums_set_male[counter]}.jpg"
            extracted_df.loc[_, "Email"] = randomer.create_random_email_addr(name=row['Name'])
            counter +=1

        extracted_dicts = extracted_df.to_dict('records')
        
        users = [{
            "index": index,
            "name": user['Name'],
            "email": user['Email'],
            "profile_pic": user['Profie Pic']
        } for index, user in enumerate(extracted_dicts)]

        result = write_to_csv(
            file_path="data/dataset_users.csv",
            data_set=users
        )
        if result is True:
            print(f"Successfully scrapped {limit} users from Kaggle dataset!")
        else:
            print(f"Kaggle {limit} users dataset extraction failed...")

        # print("Array dict:\n", users)
        # Save to a new CSV if needed
        # extracted_df.to_csv('extracted_rows.csv', index=False)
    except Exception as e:
        print(e)

def scrap_spotify_top_albums(query='', limit=100, write_mode=None):
    try:
        csv_writer = CSVWriter(file_path="data/spotify_albums.csv")
        def_max_each = 10
        max_each = limit if limit < def_max_each else def_max_each
        offset = 0
        cycle_len = math.ceil(limit / max_each)
        exclude_str = 'NOT "Greatest Hits" NOT "best of" NOT "Compilation" NOT "Hits" NOT "80s" NOT "Collection" NOT "Anos" NOT "Classics"'
        query_str = exclude_str if query == '' else f'{query} {exclude_str}'

        for i in range(cycle_len):
            offset = max_each * i
            req_volume = max_each if max_each + offset < limit else limit - offset
            results = sp.search(q=query_str, limit=req_volume, offset=offset, type="album")
            items = results['albums']['items']
            artists_id_set = randomer.get_random_nums(pool_size=20, len=req_volume, sorted=False)
            print(f"===== V [{i}] V =====")
            # print(items)
            albums = [{
                "artist_id": artists_id_set[index],
                "title": item['name'],
                "cover_pic": item['images'][0]['url'],
                "created_at": f"{item['release_date']} {randomer.get_random_time()}",
                "spty_url": item['external_urls']['spotify'],
                "spty_uri": item['uri'],
                "type": item['album_type']
            } for index, item in enumerate(items)]
            # artist_id , title, cover_pic
            csv_writer.write(data_set=albums, mode= write_mode if write_mode is not None else 'w' if i == 0 else 'a')
            # write_method : data_set, mode
            # if result is True:
            #     print(f"Successfully scrapped {limit} Spotify Top Albums!")
            # else:
            #     print(f"Spotify {limit} Top Albums scrapping failed...")
        
        print(f"Successfully scrapped {limit} Spotify Top Albums!")
    
    except Exception as e:
        print(f"Spotify {limit} Top Albums scrapping failed... {e}")

def scrap_spotify_songs(query='', limit=100, write_mode=None):

    def data_transfomer(self, items, count_report=None):
        albums_id_set = self.random_machine.get_random_nums(pool_size=102, len=len(items), offset=1, sorted=False, no_repeat=True)
        albums_id_cnt_rpt = count_report['album_id']
        results = [{
            "album_id": albums_id_set[index],
            "name": item['name'],
            "track_number": albums_id_cnt_rpt[albums_id_set[index]] + 1 if albums_id_set[index] in count_report['album_id'] else 0
        } for index, item in enumerate(items)]
        return results

    sp.switch_collect_mode(write_to="data/spotify_songs.csv")
    # sp.switch_collect_mode(write_to=None)
    collection = sp.scrap(
        query=query,
        query_type='track',
        query_market='HK,TW,US',
        limit=limit,
        data_transformer=data_transfomer,
        enforce_write_mode_to=write_mode,
        to_count_on_transform=['album_id']
    )
    # print('collection =>\n', collection)

def scrap_spotify_playlists(query='', limit=100, offset=0, write_mode=None):
    
    def data_transfomer(self, items, count_report=None):
        users_id_set = self.random_machine.get_random_nums(pool_size=50, len=len(items), offset=1, sorted=False)
        results = [{
            "user_id": users_id_set[index],
            "name": item['name'],
            "info": item['description'],
            "cover_pic": item["images"][0]["url"]
        } for index, item in enumerate(items)]
        return results

    # sp.switch_collect_mode(write_to=None)
    sp.switch_collect_mode(write_to="data/spotify_playlists.csv")
    collection = sp.scrap(
        query=query,
        query_type='playlist',
        query_offset=offset,
        # query_market='HK,TW,US',
        limit=limit,
        data_transformer=data_transfomer,
        enforce_write_mode_to=write_mode
    )
    # print('collection =>\n', collection)

def create_playlist_entries():
    csv_data_rows_sanitizer(
        input_csv='data/dataset_playlist_entries.csv',
        random_column='play_list_id', min_value=1, max_value=148, random_is_integer=True
    )
    csv_data_rows_sanitizer(
        input_csv='data/dataset_playlist_entries.csv',
        random_column='song_id', min_value=1, max_value=526, random_is_integer=True
    )
    csv_data_rows_sanitizer(
        input_csv='data/dataset_playlist_entries.csv',
        remove_column_name='dummy'
    )
    def custom_transform(row, process_column_counter):
        report = process_column_counter(row)
        playlist_id = f"{row['playlist_id']}"
        report_of_playlist_id_count = report['playlist_id'][playlist_id]
        row['order_number'] = report_of_playlist_id_count
        return row
    
    csv_data_rows_sanitizer(
        input_csv='data/dataset_playlist_entries.csv',
        transform_function=custom_transform,
        columns_to_count_on_transform=['playlist_id']
    )
    csv_data_rows_sanitizer(
        input_csv='data/dataset_playlist_entries.csv',
        columns_to_check=["playlist_id","song_id","order"],
        action="remove"
    )
    
def create_user_followers():
    csv_data_rows_sanitizer(
        input_csv='data/dataset_user_followers.csv',
        empty_column_name='empty', empty_extra_rows=1820
    )
    csv_data_rows_sanitizer(
        input_csv='data/dataset_user_followers.csv',
        random_column='user_id', min_value=1, max_value=50, random_is_integer=True
    )
    csv_data_rows_sanitizer(
        input_csv='data/dataset_user_followers.csv',
        random_column='follower_id', min_value=1, max_value=50, random_is_integer=True
    )
    csv_data_rows_sanitizer(
        input_csv='data/dataset_user_followers.csv',
        remove_column_name='dummy'
    )
    csv_data_rows_sanitizer(
        input_csv='data/dataset_user_followers.csv',
        remove_column_name='empty'
    )
    csv_data_rows_sanitizer(
        input_csv='data/dataset_user_followers.csv',
        equal_columns=('user_id', 'follower_id')
    )
    csv_data_rows_sanitizer(
        input_csv='data/dataset_user_followers.csv',
        columns_to_check=["user_id", "follower_id"],
        action="remove"
    )

def create_artist_followers():
    csv_data_rows_sanitizer(
        input_csv='data/dataset_artist_followers.csv',
        empty_column_name='empty', empty_extra_rows=856
    )
    csv_data_rows_sanitizer(
        input_csv='data/dataset_artist_followers.csv',
        random_column='artist_id', min_value=1, max_value=20, random_is_integer=True
    )
    csv_data_rows_sanitizer(
        input_csv='data/dataset_artist_followers.csv',
        random_column='follower_id', min_value=1, max_value=50, random_is_integer=True
    )
    csv_data_rows_sanitizer(
        input_csv='data/dataset_artist_followers.csv',
        remove_column_name='dummy'
    )
    csv_data_rows_sanitizer(
        input_csv='data/dataset_artist_followers.csv',
        remove_column_name='empty'
    )
    csv_data_rows_sanitizer(
        input_csv='data/dataset_artist_followers.csv',
        columns_to_check=["artist_id", "follower_id"],
        action="remove"
    )

def create_user_added_playlists():
    csv_data_rows_sanitizer(
        input_csv='data/dataset_user_added_playlists.csv',
        empty_column_name='empty', empty_extra_rows=3920
    )
    csv_data_rows_sanitizer(
        input_csv='data/dataset_user_added_playlists.csv',
        random_column='playlist_id', min_value=1, max_value=148, random_is_integer=True
    )
    csv_data_rows_sanitizer(
        input_csv='data/dataset_user_added_playlists.csv',
        random_column='user_id', min_value=1, max_value=50, random_is_integer=True
    )
    csv_data_rows_sanitizer(
        input_csv='data/dataset_user_added_playlists.csv',
        remove_column_name='dummy'
    )
    csv_data_rows_sanitizer(
        input_csv='data/dataset_user_added_playlists.csv',
        remove_column_name='empty'
    )
    csv_data_rows_sanitizer(
        input_csv='data/dataset_user_added_playlists.csv',
        columns_to_check=["playlist_id", "user_id"],
        action="remove"
    )
    

def create_user_added_albums():
    csv_data_rows_sanitizer(
        input_csv='data/dataset_user_added_albums.csv',
        empty_column_name='empty', empty_extra_rows=3130
    )
    csv_data_rows_sanitizer(
        input_csv='data/dataset_user_added_albums.csv',
        random_column='album_id', min_value=1, max_value=102, random_is_integer=True
    )
    csv_data_rows_sanitizer(
        input_csv='data/dataset_user_added_albums.csv',
        random_column='user_id', min_value=1, max_value=50, random_is_integer=True
    )
    csv_data_rows_sanitizer(
        input_csv='data/dataset_user_added_albums.csv',
        remove_column_name='dummy'
    )
    csv_data_rows_sanitizer(
        input_csv='data/dataset_user_added_albums.csv',
        remove_column_name='empty'
    )
    csv_data_rows_sanitizer(
        input_csv='data/dataset_user_added_albums.csv',
        columns_to_check=["album_id", "user_id"],
        action="remove"
    )

def create_user_liked_songs():
    csv_data_rows_sanitizer(
        input_csv='data/dataset_user_liked_songs.csv',
        empty_column_name='empty', empty_extra_rows=18900
    )
    csv_data_rows_sanitizer(
        input_csv='data/dataset_user_liked_songs.csv',
        random_column='song_id', min_value=1, max_value=526, random_is_integer=True
    )
    csv_data_rows_sanitizer(
        input_csv='data/dataset_user_liked_songs.csv',
        random_column='user_id', min_value=1, max_value=50, random_is_integer=True
    )
    csv_data_rows_sanitizer(
        input_csv='data/dataset_user_liked_songs.csv',
        remove_column_name='dummy'
    )
    csv_data_rows_sanitizer(
        input_csv='data/dataset_user_liked_songs.csv',
        remove_column_name='empty'
    )
    csv_data_rows_sanitizer(
        input_csv='data/dataset_user_liked_songs.csv',
        columns_to_check=["song_id", "user_id"],
        action="remove"
    )

def write_artists_sql():
    table_name = 'artist'
    create_sql = f'''
CREATE TABLE IF NOT EXISTS {table_name} (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(32) NOT NULL,
    bio VARCHAR(500),
    email VARCHAR(255) NOT NULL,
    profile_pic VARCHAR(255),
    created_at DATETIME DEFAULT NOW(),
    updated_at DATETIME DEFAULT NOW() ON UPDATE CURRENT_TIMESTAMP
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
    '''
    def data_transformer(df):
        df["bio"] = df["bio"].apply(lambda x: x[:500] if isinstance(x, str) and len(x) > 500 else x)
        return df
    
    sql_writer = SQLWriter(
        create_table_statement=create_sql,
        table_name=table_name,
        input_csv_file='data/spotify_artists_complete.csv',
        output_sql_file='sql/artists.sql',
        drop_columns=['spty_url', 'spty_uri'],
        data_transformer=data_transformer
    )
    sql_writer.write_sql()

def write_users_sql():
    table_name = 'user'
    create_sql = f'''
CREATE TABLE IF NOT EXISTS {table_name} (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(32) NOT NULL,
    email VARCHAR(255) NOT NULL,
    profile_pic VARCHAR(255),
    created_at DATETIME DEFAULT NOW(),
    updated_at DATETIME DEFAULT NOW() ON UPDATE CURRENT_TIMESTAMP
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
    '''
    sql_writer = SQLWriter(
        create_table_statement=create_sql,
        table_name=table_name,
        input_csv_file='data/dataset_users.csv',
        output_sql_file='sql/users.sql',
        drop_columns=['index']
    )
    sql_writer.write_sql()

def write_albums_sql():
    table_name = 'album'
    create_sql = f'''
CREATE TABLE IF NOT EXISTS {table_name} (
    id INT PRIMARY KEY AUTO_INCREMENT,
    artist_id INT NOT NULL,
    title VARCHAR(100) NOT NULL,
    cover_pic VARCHAR(255),
    created_at DATETIME DEFAULT NOW(),
    updated_at DATETIME DEFAULT NOW() ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY(artist_id)
        REFERENCES artist(id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
    '''
    sql_writer = SQLWriter(
        create_table_statement=create_sql,
        table_name=table_name,
        input_csv_file='data/spotify_albums_rename.csv',
        output_sql_file='sql/albums.sql',
        drop_columns=['spty_url', 'spty_uri', 'type']
    )
    sql_writer.write_sql()

def write_songs_sql():

    # csv_data_rows_sanitizer(
    #     input_csv='data/spotify_songs.csv',
    #     random_column='monthly_plays',
    #     min_value=300,
    #     max_value=1000000000,
    #     random_is_integer=True
    # )

    table_name = 'song'
    create_sql = f'''
CREATE TABLE IF NOT EXISTS {table_name} (
    id INT PRIMARY KEY AUTO_INCREMENT,
    album_id INT NOT NULL,
    name VARCHAR(100) NOT NULL,
    track_number INT NOT NULL,
    monthly_plays INT DEFAULT 0,
    created_at DATETIME DEFAULT NOW(),
    updated_at DATETIME DEFAULT NOW() ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY(album_id)
        REFERENCES album(id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    CONSTRAINT unq_song_name_in_album UNIQUE(album_id, name),
    CONSTRAINT unq_song_order_in_album UNIQUE(album_id, track_number)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
    '''
    sql_writer = SQLWriter(
        create_table_statement=create_sql,
        table_name=table_name,
        input_csv_file='data/spotify_songs.csv',
        output_sql_file='sql/songs.sql'
        # drop_columns=['spty_url', 'spty_uri', 'type']
    )
    sql_writer.write_sql()

def write_playlists_sql():

    # csv_data_rows_sanitizer(input_csv='data/spotify_playlists.csv', trim_column='name', max_length=100)
    # csv_data_rows_sanitizer(input_csv='data/spotify_playlists.csv', trim_column='info', max_length=500)

    table_name = 'playlist'
    create_sql = f'''
CREATE TABLE IF NOT EXISTS {table_name} (
    id INT PRIMARY KEY AUTO_INCREMENT,
    user_id INT NOT NULL,
    name VARCHAR(100) NOT NULL,
    info VARCHAR(500),
    cover_pic VARCHAR(255),
    created_at DATETIME DEFAULT NOW(),
    updated_at DATETIME DEFAULT NOW() ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY(user_id)
        REFERENCES user(id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
    '''
    sql_writer = SQLWriter(
        create_table_statement=create_sql,
        table_name=table_name,
        input_csv_file='data/spotify_playlists_reprocess.csv',
        output_sql_file='sql/playlists.sql'
        # drop_columns=['spty_url', 'spty_uri', 'type']
    )
    sql_writer.write_sql()

def write_playlist_entries_sql():

    table_name = 'playlist_entry'
    create_sql = f'''
CREATE TABLE IF NOT EXISTS {table_name} (
    id INT PRIMARY KEY AUTO_INCREMENT,
    playlist_id INT NOT NULL,
    song_id INT NOT NULL,
    order_number INT NOT NULL,
    FOREIGN KEY(playlist_id)
        REFERENCES playlist(id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    FOREIGN KEY(song_id)
        REFERENCES song(id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    CONSTRAINT unq_song_order_in_playlist UNIQUE(playlist_id, song_id, order_number)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
    '''
    sql_writer = SQLWriter(
        create_table_statement=create_sql,
        table_name=table_name,
        input_csv_file='data/dataset_playlist_entries.csv',
        output_sql_file='sql/playlist_entries.sql'
        # drop_columns=['spty_url', 'spty_uri', 'type']
    )
    sql_writer.write_sql()

def write_user_followers_sql():
    
    table_name = 'user_follower'
    create_sql = f'''
CREATE TABLE IF NOT EXISTS {table_name} (
    id INT PRIMARY KEY AUTO_INCREMENT,
    user_id INT NOT NULL,
    follower_id INT NOT NULL,
    FOREIGN KEY(user_id)
        REFERENCES user(id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    FOREIGN KEY(follower_id)
        REFERENCES user(id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    CONSTRAINT unq_user_follower_pair UNIQUE(user_id, follower_id)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
    '''
    sql_writer = SQLWriter(
        create_table_statement=create_sql,
        table_name=table_name,
        input_csv_file='data/dataset_user_followers.csv',
        output_sql_file='sql/user_followers.sql'
        # drop_columns=['spty_url', 'spty_uri', 'type']
    )
    sql_writer.write_sql()

def write_artist_followers_sql():
    
    table_name = 'artist_follower'
    create_sql = f'''
CREATE TABLE IF NOT EXISTS {table_name} (
    id INT PRIMARY KEY AUTO_INCREMENT,
    artist_id INT NOT NULL,
    follower_id INT NOT NULL,
    FOREIGN KEY(artist_id)
        REFERENCES artist(id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    FOREIGN KEY(follower_id)
        REFERENCES user(id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    CONSTRAINT unq_artist_follower_pair UNIQUE(artist_id, follower_id)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
    '''
    sql_writer = SQLWriter(
        create_table_statement=create_sql,
        table_name=table_name,
        input_csv_file='data/dataset_artist_followers.csv',
        output_sql_file='sql/artist_followers.sql'
        # drop_columns=['spty_url', 'spty_uri', 'type']
    )
    sql_writer.write_sql()

def write_user_added_playlists_sql():
    
    table_name = 'user_added_playlist'
    create_sql = f'''
CREATE TABLE IF NOT EXISTS {table_name} (
    id INT PRIMARY KEY AUTO_INCREMENT,
    playlist_id INT NOT NULL,
    user_id INT NOT NULL,
    FOREIGN KEY(playlist_id)
        REFERENCES playlist(id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    FOREIGN KEY(user_id)
        REFERENCES user(id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    CONSTRAINT unq_playlist_user_pair UNIQUE(playlist_id, user_id)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
    '''
    sql_writer = SQLWriter(
        create_table_statement=create_sql,
        table_name=table_name,
        input_csv_file='data/dataset_user_added_playlists.csv',
        output_sql_file='sql/user_added_playlists.sql'
        # drop_columns=['spty_url', 'spty_uri', 'type']
    )
    sql_writer.write_sql()

def write_user_added_albums_sql():
    
    table_name = 'user_added_album'
    create_sql = f'''
CREATE TABLE IF NOT EXISTS {table_name} (
    id INT PRIMARY KEY AUTO_INCREMENT,
    album_id INT NOT NULL,
    user_id INT NOT NULL,
    FOREIGN KEY(album_id)
        REFERENCES album(id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    FOREIGN KEY(user_id)
        REFERENCES user(id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    CONSTRAINT unq_album_user_pair UNIQUE(album_id, user_id)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
    '''
    sql_writer = SQLWriter(
        create_table_statement=create_sql,
        table_name=table_name,
        input_csv_file='data/dataset_user_added_albums.csv',
        output_sql_file='sql/user_added_albums.sql'
        # drop_columns=['spty_url', 'spty_uri', 'type']
    )
    sql_writer.write_sql()

def write_user_liked_songs_sql():
    
    table_name = 'user_liked_song'
    create_sql = f'''
CREATE TABLE IF NOT EXISTS {table_name} (
    id INT PRIMARY KEY AUTO_INCREMENT,
    song_id INT NOT NULL,
    user_id INT NOT NULL,
    FOREIGN KEY(song_id)
        REFERENCES song(id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    FOREIGN KEY(user_id)
        REFERENCES user(id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    CONSTRAINT unq_song_user_pair UNIQUE(song_id, user_id)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
    '''
    sql_writer = SQLWriter(
        create_table_statement=create_sql,
        table_name=table_name,
        input_csv_file='data/dataset_user_liked_songs.csv',
        output_sql_file='sql/user_liked_songs.sql'
        # drop_columns=['spty_url', 'spty_uri', 'type']
    )
    sql_writer.write_sql()


if __name__ == "__main__":
    # scrap_spotify_top_artist(limit=20)
    # write_artists_sql()
    # extract_data_set_users(pool_size=10000, limit=50)
    # write_users_sql()
    # scrap_spotify_top_albums(query='Glow', limit=4, write_mode='a')
    # scrap_spotify_top_albums(query='Sorry', limit=4, write_mode='a')
    # scrap_spotify_top_albums(query='Files', limit=4, write_mode='a')
    # scrap_spotify_top_albums(query='Separated', limit=4, write_mode='a')
    # scrap_spotify_top_albums(query='Drink', limit=4, write_mode='a')
    # scrap_spotify_top_albums(query='Memories', limit=4, write_mode='a')
    # scrap_spotify_top_albums(query='Kind', limit=4, write_mode='a')
    # write_albums_sql()
    # scrap_spotify_songs(query='genere:"pop,rock,jazz,r&b"', limit=422)
    # scrap_spotify_songs(query='genere:"pop,rock,jazz,r&b"', limit=526)
    # write_songs_sql()
    # scrap_spotify_playlists(query='B', limit=6)
    # scrap_spotify_playlists(query='P', limit=4, offset=12, write_mode='a')
    # scrap_spotify_playlists(query='Q', limit=6, offset=12, write_mode='a')
    # scrap_spotify_playlists(query='V', limit=7, offset=12, write_mode='a')
    # scrap_spotify_playlists(query='D', limit=3, offset=12, write_mode='a')
    # scrap_spotify_playlists(query='C', limit=4, offset=12, write_mode='a')
    # scrap_spotify_playlists(query='F', limit=6, offset=12, write_mode='a')
    # scrap_spotify_playlists(query='K', limit=7, offset=12, write_mode='a')
    # scrap_spotify_playlists(query='S', limit=3, offset=12, write_mode='a')
    # write_playlists_sql()
    # create_playlist_entries()
    # write_playlist_entries_sql()
    # create_user_followers()
    # write_user_followers_sql()
    # create_artist_followers()
    # write_artist_followers_sql()
    # create_user_added_playlists()
    # create_user_added_albums()
    # create_user_liked_songs()
    # write_user_added_playlists_sql()
    # write_user_added_albums_sql()
    # write_user_liked_songs_sql()
    pass

