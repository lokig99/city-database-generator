import os
import json
import time
import re
import unicodedata

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
JSON_DICT = os.path.join(ROOT_DIR, "output.json")

# Attributes keys
NAME = "name"
SEARCH_NAME = "s_name"
SEARCH_NAME_NONSPECIAL_CHAR = "s_name_non"
LAT = "lat"
LON = "lon"
COUNTRY = "country"
SEARCH_COUNTRY = "s_country"

SPECIAL_CHARS = {'l': ['ł']}


KEYS = "keys"
DATA = "data"


def strip_accents(text):
    """
    Strip accents from input String.

    :param text: The input string.
    :type text: String.

    :returns: The processed String.
    :rtype: String.
    """
    text = unicodedata.normalize('NFD', text)
    text = text.encode('ascii', 'ignore')
    text = text.decode("utf-8")
    return str(text)


def text_to_id(text):
    """
    Convert input text to id.

    :param text: The input string.
    :type text: String.

    :returns: The processed String.
    :rtype: String.
    """
    text = strip_accents(text.lower())
    text = re.sub('[ ]+', '_', text)
    text = re.sub('[^0-9a-zA-Z_-]', '', text)
    return text


def replace_special_chars(string: str) -> str:
    for normal_char, special_chars in SPECIAL_CHARS.items():
        for spec_char in special_chars:
            string = string.replace(spec_char, normal_char)
    return string


def regenerate_database(compressed_dict: dict) -> list:
    '''
    generate database with given structure:

    [
        {
            NAME: str,
            LAT: list,
            LON: list,
            COUNTRY: str,
            SEARCH_COUNTRY: str,
            SEARCH_NAME: str,
            SEARCH_NAME_NONSPECIAL_CHAR: str
        },
        ...
    ]
    '''
    res = []
    keys = compressed_dict[KEYS]
    for country in compressed_dict[DATA]:
        data = compressed_dict[DATA][country]
        tmp_list = []
        tmp_dict = {}
        for i, data_bit in enumerate(data):
            i = i % len(keys)
            if len(tmp_dict) == len(keys):
                tmp_dict[COUNTRY] = country
                tmp_dict[SEARCH_COUNTRY] = country.lower()
                tmp_dict[SEARCH_NAME] = tmp_dict[NAME].lower()
                tmp_dict[SEARCH_NAME_NONSPECIAL_CHAR] = text_to_id(replace_special_chars(
                    tmp_dict[SEARCH_NAME]))
                tmp_list.append(tmp_dict)
                tmp_dict = {}
            tmp_dict[keys[i]] = data_bit
        res += tmp_list
    return res


def get_single_word_cities_sorted_az(cities_db: list) -> list:
    res = []
    for city in cities_db:
        name_words = city[NAME].split()
        if len(name_words) == 1:
            res.append(city)
    return sorted(res, key=lambda x: x[SEARCH_NAME_NONSPECIAL_CHAR])


def index_sorted_cities(cities_db: list) -> dict:
    import string
    res = {}
    index = 0
    for letter in string.ascii_lowercase:
        i = index
        for city in cities_db[index:]:
            if city[SEARCH_NAME_NONSPECIAL_CHAR].startswith(letter):
                res[letter] = i
                index = i
                break
            i += 1
    return res


with open(JSON_DICT, 'r', encoding="utf8") as f:
    start = time.time()
    DATABASE = regenerate_database(json.load(f))
    end = time.time()
    print(
        f'Database generation took: {round((end - start) * 1000)} miliseconds')

SINGLE_WORD_CITIES_DATABASE = get_single_word_cities_sorted_az(DATABASE)
SW_CITIES_DB_INDEX = index_sorted_cities(SINGLE_WORD_CITIES_DATABASE)


def text_in_city_attr(text: str, city_attr: dict) -> bool:
    text_split = text.split()
    found = []

    # check if there are no special characters in searcheded words
    if replace_special_chars(text) == text:
        city_name = city_attr[SEARCH_NAME_NONSPECIAL_CHAR]
    else:
        city_name = city_attr[SEARCH_NAME]

    country = city_attr[SEARCH_COUNTRY]

    for t in text_split:
        if t in city_name or t in country:
            found.append(True)
        else:
            found.append(False)
    return all(found)


def get_matching_cities(text: str) -> list:
    matches = []
    text = text.lower()
    for city in DATABASE:
        if text_in_city_attr(text, city):
            matches.append(city)
    return matches


def printable_matches(matches: list) -> list:
    return [f'{item[NAME]}, {item[COUNTRY]}\tLat:{item[LAT]}, Lon:{item[LON]}' for item in matches]


if __name__ == "__main__":
    while True:
        searched_text = input('Search for city: ')
        if searched_text == "exit()":
            break
        start = time.time()
        matches = get_matching_cities(searched_text)
        end = time.time()
        for match in printable_matches(matches):
            print(match)
        print(f"\nSearch took: {round((end - start) * 1000)} miliseconds\n")

    # print(len(DATABASE))
    # del DATABASE
    # time.sleep(10)

    # print(len(get_single_word_cities_sorted_az(DATABASE)))
    # for x in get_single_word_cities_sorted_az(DATABASE):
    #     print(x)

    z = get_single_word_cities_sorted_az(DATABASE)
    x = index_sorted_cities(z)
    print(len(DATABASE))
