import spotipy, math, datetime
import pandas as pd
from spotipy.oauth2 import SpotifyClientCredentials
from utils.CSVWriter import CSVWriter

class SpotifyPublicScrapper:

    __query_type_res_key_lib = {'artist':'artists', 'album':'albums', 'track':'tracks', 'playlist':'playlists', 'show':'shows', 'episode':'episodes', 'audiobook':'audiobooks'}
    __column_value_counter = {}

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        default_max_each = 10,
        write_to=None,
        random_machine=None
    ):
        try:
            sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(
                client_id=client_id,
                client_secret=client_secret
            ))
            self._sp = sp
        except Exception as e:
            print(f'Spotify API Authentication Failed : {e}')
        
        self.default_max_each = default_max_each
        self.switch_collect_mode(write_to)
        self.random_machine = random_machine


    def __get_res_data_key(self, type=None):
        if type is None or SpotifyPublicScrapper.__query_type_res_key_lib[type] is None:
            return None
        else:
            return SpotifyPublicScrapper.__query_type_res_key_lib[type]
    
    def __reset_column_value_counter(self):
        self.__column_value_counter = {}

    def __setup_column_value_counter(self, field_keys=[]):
        for key in field_keys:
            self.__column_value_counter[key] = {}

    
    def switch_collect_mode(self, write_to=None):
        self.write_to = write_to
        if write_to is not None:
            self._collect_mode = 'w'
        else:
            self._collect_mode = 'r'

    def count_column_value_appearance(self, key, value):
        if key in self.__column_value_counter:
            if value in self.__column_value_counter[key]:
                self.__column_value_counter[key][value] += 1
            else:
                self.__column_value_counter[key][value] = 0

    def retrieve_counter_report(self):
        return self.__column_value_counter
    
    def process_counter(self, data_set, counter_range):
        for row in data_set:
            for key in counter_range:
                if key in row:
                    self.count_column_value_appearance(key=key, value=row[key])
        
    def scrap(
        self,
        query=None,
        query_offset=0,
        query_type=None,
        query_market=None,
        limit=100,
        condition=None,
        enforce_write_mode_to=None,
        data_transformer=None,
        to_count_on_transform=[]
    ):
        counter_mode = False
        try:
            if query_type is None or self.__get_res_data_key(query_type) is None:
                raise Exception("Please state an allowed type of query that you want!\narguement with issue: query_type")
            
            if len(to_count_on_transform) > 0:
                self.__setup_column_value_counter(to_count_on_transform)
                counter_mode = True
            
            data_key = self.__get_res_data_key(query_type)
            csv_writer = CSVWriter(file_path=self.write_to) if self._collect_mode == 'w' else None
            final_df = pd.DataFrame() if self._collect_mode == 'r' else None

            def_max_each = self.default_max_each
            max_each = limit if limit < def_max_each else def_max_each
            offset = 0
            cycle_len = math.ceil(limit / max_each)
            query_str = '' if query is None and condition is None else condition if query is None and condition is not None else query if query is not None and condition is None else f"{query} {condition}"

            for i in range(cycle_len):
                offset = max_each * i
                req_volume = max_each if max_each + offset < limit else limit - offset
                results = self._sp.search(
                    q=query_str,
                    limit=req_volume,
                    offset=offset+query_offset,
                    type=query_type,
                    market=query_market
                )
                
                items = results[data_key]['items']
                items = [item for item in items if item is not None]

                if data_transformer is None:
                    data_set = items    
                else:
                    if counter_mode is False:
                        data_set = data_transformer(self=self, items=items)
                    else:
                        data_set = data_transformer(
                            self=self,
                            items=items,
                            count_report=self.retrieve_counter_report()
                        )
                        self.process_counter(data_set=data_set, counter_range=to_count_on_transform)
                        
                # artist_id , title, cover_pic
                if csv_writer is not None:
                    csv_writer.write(
                        data_set=data_set,
                        mode= enforce_write_mode_to if enforce_write_mode_to is not None else 'w' if i == 0 else 'a',
                        print_remarks=f"- ({i+1}/{cycle_len})"
                    )

                if final_df is not None:
                    action_time = datetime.datetime.now().strftime("%Y-%M-%d %H:%M:%S")
                    batch_df = pd.DataFrame(data_set)
                    final_df = pd.concat([final_df, batch_df], ignore_index=True)
                    print(f"Successfully collected response data for a query of {query_type} at {action_time} ({i+1}/{cycle_len})!")
            
            print(f"Successfully scrapped {limit} Spotify {query_type} data!")

            if final_df is not None:
                return final_df
            
        
        except Exception as e:
            print(f"Spotify {limit} {query_type} scrapping failed... {e}")

    # def get_genre(self):
    #     genres = self._sp.recommendation_genre_seeds()
    #     return genres
        