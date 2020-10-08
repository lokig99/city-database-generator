import re
import os
import time
import json
from urllib.request import urlopen
from multiprocessing.pool import ThreadPool


WORKERS_COUNT = 512

WIKIPEDIA_ADDRESS = "https://en.wikipedia.org"

LATITUDE_HTML = '<span class="latitude">'
LONGITUDE_HTML = '<span class="longitude">'
SPAN_END_HTML = '</span>'

HREF_BEGIN_HTML = '<a href="'
HREF_END_HTML = '</a>'
HREF_WIKI = "/wiki/"


CORD_CHARS = '°', '′', '″'  # degrees, minutes and seconds

# Dictionary keys
HREF_LINK = "href"
HREF_TEXT = "text"
NAME = "name"
LATITUDE = "lat"
LONGITUDE = "lon"
DATA = "data"
KEYS = "keys"
COUNTRY = "country"
CAPITAL = 'capital'

ARTICLE_NOT_FOUND = 'Wikipedia does not have an article with this exact name'

WIKI_PAGE_ADDRESSES = '/wiki/List_of_cities_in_', 'List_of_cities_and_towns_in_'

MANUAL_HREF_OVERRIDES = [
    ("the_United_States",
     'https://en.wikipedia.org/wiki/List_of_United_States_cities_by_population'),
    ("Netherlands", "https://en.wikipedia.org/wiki/List_of_cities_in_the_Netherlands_by_province"),
    ("Timor-Leste", "https://en.wikipedia.org/wiki/List_of_cities,_towns_and_villages_in_East_Timor")]

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
WIKI_SOURCE_DIR = os.path.join(ROOT_DIR, "countries")
WIKI_SOURCE_EXT = '.wiki'
OUTPUT_FILE = os.path.join(ROOT_DIR, "output.json")
COUNTRIES_FILE = os.path.join(ROOT_DIR, 'cc-final.json')

BANNED_WORDS = {'<img', '<span', 'culture', 'Kingdom', 'communities', 'communes', 'commune', 'community',
                'World War', 'history', 'empire', 'revolution', 'article', 'republic', 'metropoly', 'metropolis',
                'tourism', 'see', 'sea ', 'Volcanoes', 'geography', 'read', '<i', 'county', 'country', 'province', 'region', 'state',
                'damaged', 'United Arab Emirates', 'Middle East', 'national', 'capital', 'island', 'ocean', 'exchange', 'market',
                'central', 'center', 'falklands', 'space program', 'confederation', 'battle', 'Gibraltar', 'desert', 'royal',
                ' sea', '>', '<', 'territor', 'university', 'soviet', 'union', 'europe', ' centre', ' district', 'church ', 'cathedral'}

with open(COUNTRIES_FILE, 'r', encoding="utf8") as f:
    COUNTRIES = json.load(f)


def get_coordinates_from_urlstring(url_string: str) -> dict:
    def split_cord_chars(cord_str: str) -> list:
        for char in CORD_CHARS:
            cord_str = cord_str.replace(char, ' ')

        if 's' in cord_str.lower() or 'w' in cord_str.lower():
            cord_str = f'-{cord_str[:len(cord_str) - 1]}'
        else:
            cord_str = cord_str[:len(cord_str) - 1]

        res = [int(x) for x in cord_str.split()]
        return res

    lat_index_start = url_string.find(LATITUDE_HTML) + len(LATITUDE_HTML)
    lat_index_end = url_string.find(SPAN_END_HTML, lat_index_start)

    lon_index_start = url_string.find(LONGITUDE_HTML) + len(LONGITUDE_HTML)
    lon_index_end = url_string.find(SPAN_END_HTML, lon_index_start)

    lat_str = url_string[lat_index_start:lat_index_end]
    lon_str = url_string[lon_index_start:lon_index_end]

    lat = split_cord_chars(lat_str)
    lon = split_cord_chars(lon_str)

    return {LATITUDE: lat, LONGITUDE: lon}


def split_href_wiki_text(href_wiki: str, country: str) -> tuple:
    res = {}
    href = href_wiki[:href_wiki.find('"')]
    res[HREF_LINK] = f'{WIKIPEDIA_ADDRESS}{href}'
    res[HREF_TEXT] = href_wiki[href_wiki.find('>') + 1:]
    res[COUNTRY] = country

    # Filter out errors

    for word in BANNED_WORDS:
        word = word.lower()
        if word in res[HREF_LINK].lower() or word in res[HREF_TEXT].lower():
            return None

    for c in COUNTRIES:
        _country = c[COUNTRY].lower()
        if _country in res[HREF_LINK].lower() or _country in res[HREF_TEXT].lower():
            return None
        if country.lower() != _country:
            if c[CAPITAL].lower() in res[HREF_TEXT].lower():
                return None

    if any(char.isdigit() for char in res[HREF_TEXT]):
        return None

    if res[HREF_TEXT].upper() == res[HREF_TEXT]:
        return None

    return res


def get_all_wiki_href(country: str) -> dict:
    url_string = ''
    man_override = False
    man_address = ''
    for item in MANUAL_HREF_OVERRIDES:
        if country in item[0]:
            man_override = True
            man_address = item[1]
            break

    if man_override:
        try:
            url = man_address
            r = urlopen(url)
            url_string = str(r.read(), encoding="UTF-8")
        except:
            print(f"Failed to visit site: {url}")
    else:
        for page in WIKI_PAGE_ADDRESSES:
            try:
                url = f'{WIKIPEDIA_ADDRESS}{page}{country}'
                r = urlopen(url)
                url_string = str(r.read(), encoding="UTF-8")
            except:
                print(f"Failed to visit site: {url}")

            if not ARTICLE_NOT_FOUND in url_string:
                break

    all_href = [url_string[m.end(): url_string.find(HREF_END_HTML, m.end())]
                for m in re.finditer(HREF_BEGIN_HTML, url_string)]

    print("Got data for:", country)

    return [split_href_wiki_text(href, country) for href in all_href if href.startswith(HREF_WIKI)]


def get_list_of_cities_async(city_href_list: list, processes_count=-1) -> list:
    def split_list(l: list, n) -> list:
        return [l[i:i + n] for i in range(0, len(l), n)]

    if processes_count == -1:
        processes_count = len(city_href_list)

    try:
        list_splitted = split_list(city_href_list, len(
            city_href_list) // processes_count)
    except ZeroDivisionError:
        print("No tasks available...")
        return []
    except:
        print("ERROR! Too many processes. To use maximum number of threads set 'processes_count' key-arg to: -1")
        return []

    result_list = []

    def log_result(result):
        result_list.append(result)

    pool = ThreadPool(processes=processes_count)
    pool.map_async(get_city_attr_async, list_splitted, callback=log_result)

    pool.close()
    pool.join()

    return flatten(flatten(result_list))


def get_city_attr_from_href(city_href: dict) -> dict:
    try:
        req = urlopen(city_href[HREF_LINK])
    except:
       # print(f"Failed to open url: {city_href[HREF_LINK]}")
       pass
    data = str(req.read(), encoding="UTF-8")

    res = {}
    res[NAME] = ' '.join(city_href[HREF_TEXT].split())
    if len(res[NAME]) > 1 and not res[NAME][0].isalpha():
        res[NAME] = res[NAME][1:]

    cords = get_coordinates_from_urlstring(data)
    res[LATITUDE] = cords[LATITUDE]
    res[LONGITUDE] = cords[LONGITUDE]
    res[COUNTRY] = city_href[COUNTRY]
    return res


def get_city_attr_async(city_href_list: list) -> list:
    res = []
    for href in city_href_list:
        try:
            item = get_city_attr_from_href(href)
            res.append(item)
            #print(item)
        except:
          #  print("failed")
          pass
    return res


def flatten(lst: list) -> list:
    return [item for sublist in lst for item in sublist]


def get_duplicate_cities(cities_attr: list) -> list:
    occurences = {}
    duplicates = []
    for city in cities_attr:
        occurences[city[NAME]] = occurences.get(city[NAME], 0) + 1
        if occurences[city[NAME]] > 1:
            duplicates.append(city)
    return duplicates


def delete_duplicates(cities_attr: list) -> None:
    duplicates = get_duplicate_cities(cities_attr)
    for d in duplicates:
        cities_attr.remove(d)


def optimized_output(cities_attr: list) -> dict:
    keys = [NAME, LATITUDE, LONGITUDE]
    data = flatten([list(city.values()) for city in cities_attr])
    return {KEYS: keys, DATA: data}


def split_cities_by_countries(cities_lst: list) -> dict:
    res = {}
    for city in cities_lst:
        country = city[COUNTRY]
        del city[COUNTRY]
        res[country] = res.get(country, []) + [city]
    return res


def generate_database() -> dict:
    database = {}

    def generate_complete_href_list():
        clst = [c[COUNTRY] for c in COUNTRIES]
        pool = ThreadPool(processes=len(clst))

        result_list = []

        def get_res(res):
            result_list.extend(res)

        pool.map_async(get_all_wiki_href, clst, callback=get_res)
        pool.close()
        pool.join()
        return flatten(result_list)

    tmp = get_list_of_cities_async(
        generate_complete_href_list(), processes_count=WORKERS_COUNT)
    delete_duplicates(tmp)

    cities_by_country = split_cities_by_countries(tmp)
    for country in cities_by_country:
        database[country.replace("_", " ")] = optimized_output(
            cities_by_country[country])
    return database


if __name__ == "__main__":
    time_start = time.time()

    data = generate_database()
    with open(OUTPUT_FILE, 'w', encoding="UTF-8") as f:
        json.dump(data, f, ensure_ascii=False)

    time_end = time.time()
    print(f'\nFinished all tasks in {round(time_end - time_start, 2)} seconds')
