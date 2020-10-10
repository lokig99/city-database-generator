import os
import re
import json
import pickle
import string
import unicodedata

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_JSON = os.path.join(ROOT_DIR, "output.json")

CACHED_DATABASE_FILEPATH = os.path.join(ROOT_DIR, "cached.pkl")
CACHED_SWL = "swl"
CACHED_MWL = "mwl"
CACHED_COUNTRIES = 'countries'
CACHED_SWL_INDEX = 'swlindex'

# Attributes keys
NAME = "name"
SEARCH_NAME = "s_name"
SEARCH_NAME_NONSPECIAL_CHAR = "s_namen"
LAT = "lat"
LON = "lon"
COUNTRY = "ctr"
COUNTRY_ID = 'ctrid'
SEARCH_COUNTRY = "sctr"
KEYS = "keys"
DATA = "data"
SWL_LOCATIONS = 'swllocs'
MWL_LOCATIONS = 'mwllocs'

NON_UNICODE_CHARS = {'ł': 'l', 'ß': 'ss'}
ASCII_ALPHABET = string.ascii_lowercase


def _replace_special_characters(text: str) -> str:
    text = unicodedata.normalize('NFD', text)
    for char in NON_UNICODE_CHARS:
        text = text.replace(char, NON_UNICODE_CHARS[char])
        text = text.replace(char.upper(), NON_UNICODE_CHARS[char].upper())
    text = text.encode('ascii', 'ignore')
    text = text.decode("utf-8")
    return text


def _text_to_id(text: str) -> str:
    text = _replace_special_characters(text.lower())
    text = re.sub('[ ]+', '_', text)
    text = re.sub('[^0-9a-zA-Z_-]', '', text)
    return text


class LocationDatabase:
    def __init__(self):
        self.__is_opened = False
        self.__single_word_locations = []
        self.__multiple_word_locations = []
        self.__countries = {}
        self.__swl_index = {}

    def open_from_json(self) -> bool:
        success = False
        try:
            with open(DB_JSON, 'r', encoding="utf8") as f:
                db_json = json.load(f)
                success = True
        except IOError as e:
            print('Failed to open database file. Error:', e)
        except json.JSONDecodeError as e:
            print('Database file is corrupted. Error:', e)

        if success:
            self.__is_opened = True
            return self.__regenerate_database(db_json)

        return False

    def open_from_cache(self) -> bool:
        success = False
        try:
            with open(CACHED_DATABASE_FILEPATH, 'rb') as f:
                cached_db = pickle.load(f, encoding="utf8")
                self.__single_word_locations = cached_db[CACHED_SWL]
                self.__multiple_word_locations = cached_db[CACHED_MWL]
                self.__countries = cached_db[CACHED_COUNTRIES]
                self.__swl_index = cached_db[CACHED_SWL_INDEX]
                success = True
        except IOError as e:
            print(e)
        except pickle.UnpicklingError as e:
            print(e)

        if success:
            self.__is_opened = True
        else:
            self.__is_opened = False
        return success

    def is_opened(self) -> bool:
        return self.__is_opened

    def __regenerate_database(self, json_db: dict) -> bool:
        def refresh_db_indexing() -> None:
            # index single word locations
            tmp_swl_index = {}
            index = 0
            for letter in ASCII_ALPHABET:
                i = index
                for loc in self.__single_word_locations[index:]:
                    if loc[SEARCH_NAME_NONSPECIAL_CHAR].startswith(letter):
                        tmp_swl_index[letter] = i
                        index = i
                        break
                    i += 1
            self.__swl_index = tmp_swl_index

            # index locations by country
            for index, loc in enumerate(self.__single_word_locations):
                self.__countries[loc[COUNTRY_ID]][SWL_LOCATIONS].append(index)
            for index, loc in enumerate(self.__multiple_word_locations):
                self.__countries[loc[COUNTRY_ID]][MWL_LOCATIONS].append(index)

        result = False
        try:
            tmp_countries_dict = {}
            tmp_list_swl = []
            tmp_list_mwl = []
            keys = json_db[KEYS]
            for country in json_db[DATA]:
                ctry_id = _text_to_id(country)
                tmp_countries_dict[ctry_id] = {
                    COUNTRY: country, SEARCH_COUNTRY: country.lower(),
                    SWL_LOCATIONS: [], MWL_LOCATIONS: []}
                data = json_db[DATA][country]

                # fill swl and mwl lists
                tmp_dict = {}
                for i, data_bit in enumerate(data):
                    i = i % len(keys)
                    if len(tmp_dict) == len(keys):
                        tmp_dict[COUNTRY_ID] = ctry_id
                        tmp_dict[SEARCH_NAME] = tmp_dict[NAME].lower()
                        tmp_dict[SEARCH_NAME_NONSPECIAL_CHAR] = _text_to_id(
                            tmp_dict[NAME])
                        if len(tmp_dict[NAME].split()) > 1:
                            tmp_list_mwl.append(tmp_dict)
                        else:
                            tmp_list_swl.append(tmp_dict)
                        tmp_dict = {}
                    tmp_dict[keys[i]] = data_bit

            self.__countries = tmp_countries_dict
            self.__single_word_locations = sorted(
                tmp_list_swl, key=lambda x: x[SEARCH_NAME_NONSPECIAL_CHAR])
            self.__multiple_word_locations = tmp_list_mwl
            refresh_db_indexing()
            result = True
        except KeyError as e:
            print(e)

        return result

    def save_to_cache(self) -> bool:
        result = False
        if self.is_opened():
            cached_db = {}
            cached_db[CACHED_COUNTRIES] = self.__countries
            cached_db[CACHED_SWL] = self.__single_word_locations
            cached_db[CACHED_MWL] = self.__multiple_word_locations
            cached_db[CACHED_SWL_INDEX] = self.__swl_index
            try:
                with open(CACHED_DATABASE_FILEPATH, 'wb') as f:
                    pickle.dump(cached_db, f)
                    result = True
            except IOError as e:
                print(e)
            except pickle.PicklingError as e:
                print(e)
        return result

    def search(self, text: str) -> list:
        def text_in_city_attr(text: str, city_attr: dict) -> bool:
            text_split = text.split()
            found = []

            # check if there are no special characters in searched words
            if _replace_special_characters(text) == text:
                city_name = city_attr[SEARCH_NAME_NONSPECIAL_CHAR]
            else:
                city_name = city_attr[SEARCH_NAME]

            country = self.__countries[city_attr[COUNTRY_ID]][SEARCH_COUNTRY]

            for t in text_split:
                if t in city_name or t in country:
                    found.append(True)
                else:
                    found.append(False)
            return all(found)

        matches = []
        text = text.lower()
        fst_letter = text[0]
        search_index_start = self.__swl_index[fst_letter]
        if fst_letter == ASCII_ALPHABET[-1]:
            for city in self.__single_word_locations[search_index_start:]:
                if text_in_city_attr(text, city):
                    matches.append(city)
        else:
            search_index_end = self.__swl_index[ASCII_ALPHABET[ASCII_ALPHABET.find(
                fst_letter) + 1]]
            for city in self.__single_word_locations[search_index_start:search_index_end]:
                if text_in_city_attr(text, city):
                    matches.append(city)

        return matches

    def print_data(self):
        for item in self.__single_word_locations:
            print(item)
        print(self.__swl_index)
        print(self.__countries)


def printable_matches(matches: list) -> list:
    return [f'{item[NAME]}, {item[COUNTRY_ID]}\tLat:{item[LAT]}, Lon:{item[LON]}' for item in matches]


if __name__ == "__main__":
    import time
    db = LocationDatabase()

    start = time.time()
    res = db.open_from_json()
    end = time.time()
    print(res, "took:", round((end - start) * 1000), "miliseconds")
    res = db.save_to_cache()

    # start = time.time()
    # res = db.open_from_cache()
    # end = time.time()
    # print(res, "took:", round((end - start) * 1000), "miliseconds")

    while True:
        searched_text = input('Search for city: ')
        if searched_text == "exit()":
            break
        start = time.time()
        matches = db.search(searched_text)
        end = time.time()
        for match in printable_matches(matches):
            print(match)
        print(f"\nSearch took: {round((end - start) * 1000)} miliseconds\n")

    del db
    time.sleep(10)
