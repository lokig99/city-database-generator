import os
import json
import time

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
JSON_DICT = os.path.join(ROOT_DIR, "output-compressed.json")

# Attributes keys
NAME = "name"
LAT = "lat"
LON = "lon"
COUNTRY = "country"

SPECIAL_CHARS = {'a': ['ą'], 'c': ['ć'], 'e': ['ę'], 'l': [
    'ł'], 'n': ['ń'], 'o': ['ó'], 's': ['ś'], 'z': ['ź', 'ż']}


KEYS = "keys"
DATA = "data"


def regenerate_dictionary(compressed_dict: dict) -> dict:
    '''
    generate dictionary with given structure:

    [
        {
            NAME: str,
            LAT: list,
            LON: list,
            COUNTRY: str
        },
        ...
    ]
    '''
    res = []
    for country in compressed_dict:
        keys = compressed_dict[country][KEYS]
        data = compressed_dict[country][DATA]
        tmp_list = []
        tmp_dict = {}
        for i, data_bit in enumerate(data):
            i = i % len(keys)
            if len(tmp_dict) == len(keys):
                tmp_dict[COUNTRY] = country
                tmp_list.append(tmp_dict)
                tmp_dict = {}
            tmp_dict[keys[i]] = data_bit
        res += tmp_list
    return res


with open(JSON_DICT, 'r', encoding="utf8") as f:
    start = time.time()
    DATABASE = regenerate_dictionary(json.load(f))
    end = time.time()
    print(f'Database generation took: {round(end - start, 6)} seconds')


def text_in_city_attr(text: str, city_attr: dict) -> bool:
    text_split = text.lower().split()
    found = []

    for t in text_split:
        # check if there are no special characters in searched word
        if replace_special_chars(t) == t:
            city_name = replace_special_chars(city_attr[NAME].lower())
            country = replace_special_chars(city_attr[COUNTRY].lower())
        else:
            city_name = city_attr[NAME].lower()
            country = city_attr[COUNTRY].lower()

        if t in city_name or t in country:
            found.append(True)
        else:
            found.append(False)

    return all(found)


def replace_special_chars(string: str) -> str:
    for normal_char, special_chars in SPECIAL_CHARS.items():
        for spec_char in special_chars:
            string = string.replace(spec_char.upper(), normal_char.upper())
            string = string.replace(spec_char, normal_char)
    return string


def get_matching_cities(text: str) -> list:
    matches = []
    for city in DATABASE:
        if text_in_city_attr(text, city):
            matches.append(city)
    return matches


def printable_matches(matches: list) -> list:
    return [f'{item[NAME]}, {item[COUNTRY]}\tLat:{item[LAT]}, Lon:{item[LON]}' for item in matches]


if __name__ == "__main__":
    while True:
        for match in printable_matches(get_matching_cities(input('Search for city: '))):
            print(match)
        print("\n\n")
