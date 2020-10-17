#!/bin/python3

import os
import re
import json
import pickle
import string
import unicodedata
from typing import List, Tuple, Dict, Any, Set

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_JSON = os.path.join(ROOT_DIR, "output.json")

# Attributes keys
NAME = "name"
LAT = "lat"
LON = "lon"
KEYS = "keys"
DATA = "data"

NON_UNICODE_CHARS = {'ł': 'l', 'ß': 'ss'}
ASCII_ALPHABET = string.ascii_lowercase


def _normalize_text(text: str, keep_diacritics=False) -> str:
    text = " ".join(text.lower().split())
    if not keep_diacritics:
        text = unicodedata.normalize('NFD', text)
        for char in NON_UNICODE_CHARS:
            text = text.replace(char, NON_UNICODE_CHARS[char])
        text = text.encode('ascii', 'ignore').decode("utf-8")
    return text


class Location:
    def __init__(self, name: str):
        self.name = name

    def __str__(self):
        return self.name

    def searchable_name(self) -> str:
        return _normalize_text(self.name, keep_diacritics=True)

    def searchable_name_normalized(self) -> str:
        return _normalize_text(self.name)


class Country(Location):
    def __init__(self, name: str):
        super().__init__(name)
        self.cities: Set[City] = set()


class City(Location):
    def __init__(self, name: str, country: Country, latitude: Tuple[float], longitude: Tuple[float]):
        super().__init__(name)
        self.country = country
        country.cities.add(self)
        self.latitude = latitude
        self.longitude = longitude

    def __str__(self):
        return f"{super().__str__()}, {self.country}"

    def __degrees_to_decimal(self, coordinates: Tuple[float]) -> float:
        decimal = 0.0
        mult = 1
        for dd in coordinates[1:]:
            mult *= 60
            decimal += dd / mult
        if coordinates[0] < 0:
            return coordinates[0] - decimal
        return coordinates[0] + decimal

    def latitude_decimal(self):
        return self.__degrees_to_decimal(self.latitude)

    def longitude_decimal(self):
        return self.__degrees_to_decimal(self.longitude)


class LocationDatabase:
    def __init__(self):
        self.__is_opened = False
        self.__cities_indexed: Dict[str, Dict[str, List[City]]] = dict(
            [(l, {}) for l in ASCII_ALPHABET])
        self.__countries: Set[Country] = set()

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

        if success and self.__regenerate_database(db_json):
            self.__is_opened = True
            return True
        return False

    def is_opened(self) -> bool:
        return self.__is_opened

    def __regenerate_database(self, json_db: Dict[str, Any]) -> bool:
        result = False
        try:
            keys = json_db[KEYS]
            for country_name in json_db[DATA]:
                country = Country(country_name)
                self.__countries.add(country)
                data = json_db[DATA][country_name]
                tmp_dict: Dict[str, Any] = {}
                for i, data_bit in enumerate(data):
                    i = i % len(keys)
                    if len(tmp_dict) == len(keys):
                        city = City(tmp_dict[NAME], country,
                                    tmp_dict[LAT], tmp_dict[LON])
                        header = city.searchable_name_normalized()[:2]
                        if header[1] in self.__cities_indexed[header[0]]:
                            self.__cities_indexed[header[0]
                                                  ][header[1]].append(city)
                        else:
                            self.__cities_indexed[header[0]
                                                  ][header[1]] = [city]
                        tmp_dict.clear()
                    tmp_dict[keys[i]] = data_bit
            result = True
        except KeyError as e:
            print(e)
        return result

    def search(self, text: str) -> Set[City]:
        def text_in_city_attr(text: str, city: City) -> bool:
            # check if there are no special characters in searched words
            if _normalize_text(text) == text:
                city_name = city.searchable_name_normalized()
            else:
                city_name = city.searchable_name()
            return city_name.startswith(text)

        matches: Set[City] = set()
        if len(text) > 0 and text[0].isalpha():
            text = _normalize_text(text, keep_diacritics=True)
            header = _normalize_text(text[:2])
            if len(header) < 2:
                for _, cities in self.__cities_indexed[header[0]].items():
                    matches.update(cities)
            else:
                try:
                    for city in self.__cities_indexed[header[0]][header[1]]:
                        if text_in_city_attr(text, city):
                            matches.add(city)
                except KeyError:
                    pass
        return matches


def main():
    import time

    db = LocationDatabase()
    start = time.time()
    res = db.open_from_json()
    end = time.time()
    print(res, "took:", round((end - start) * 1000), "miliseconds")

    while True:
        searched_text = input('Search for city: ')
        if searched_text == "exit()":
            break
        start = time.time()
        matches = db.search(searched_text)
        end = time.time()
        for city in matches:
            print(city)
        print(f"\nSearch took: {round((end - start) * 1000)} miliseconds\n")


if __name__ == "__main__":
    main()
