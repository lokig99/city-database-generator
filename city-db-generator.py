import re
from urllib.request import urlopen
from multiprocessing.pool import ThreadPool
import json
import os
import time


WIKIPEDIA_ADDRESS = "https://en.wikipedia.org"

LATITUDE_HTML = '<span class="latitude">'
LONGITUDE_HTML = '<span class="longitude">'
SPAN_END_HTML = '</span>'

HREF_BEGIN_HTML = '<a href="'
HREF_END_HTML = '</a>'
HREF_WIKI = "/wiki/"


HREF_KEY = "href"
HREF_TEXT_KEY = "text"

CORD_CHARS = '°', '′', '″'

LATITUDE_KEY = "latitude"
LONGITUDE_KEY = "longitude"


# Attributes keys
NAME = "name"
REGION = "region"
LAT = "lat"
LON = "lon"

DATA = "data"
KEYS = 'keys'

COUNTRY = "Poland"

PROGRAM_PATH = os.path.dirname(os.path.abspath(__file__))
WIKI_SOURCE_FILE = os.path.join(PROGRAM_PATH, "countries/Poland.wiki")
WIKI_SOURCE_EXT = '.wiki'
OUTPUT_FILE = os.path.join(PROGRAM_PATH, "output.json")

# examples: "province", "state", "voivodeship", "region"
ADM_REGION_HTML = '<a href="/wiki/Voivodeships_of_Poland" title="Voivodeships of Poland">Voivodeship</a>'
REGION_NAME = "Voivodeship"


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

    return {LATITUDE_KEY: lat, LONGITUDE_KEY: lon}


def split_href_wiki_text(href_wiki: str) -> tuple:
    res = {}
    href = href_wiki[:href_wiki.find('"')]
    res[HREF_KEY] = f'{WIKIPEDIA_ADDRESS}{href}'
    res[HREF_TEXT_KEY] = href_wiki[href_wiki.find('>') + 1:]
    return res


def get_adm_region_from_urlstring(url_string: str, append_region_name=False) -> str:
    title_href_end = next(re.finditer(ADM_REGION_HTML, url_string)).end()
    url_string = url_string[title_href_end:]
    region_href_iter = re.finditer(HREF_BEGIN_HTML, url_string)
    region_href_end = next(region_href_iter).end()
    href = url_string[region_href_end: url_string.find(
        HREF_END_HTML, region_href_end)]

    # skip images before text
    if href.find('class="image') != -1:
        region_href_end = next(region_href_iter).end()
        href = url_string[region_href_end: url_string.find(
            HREF_END_HTML, region_href_end)]

    region = split_href_wiki_text(href)[HREF_TEXT_KEY]
    if append_region_name:
        if not REGION_NAME.lower() in region.lower():
            region = f"{region} {REGION_NAME}"

    return region


def get_all_wiki_href(url_string: str) -> dict:
    all_href = [url_string[m.end(): url_string.find(HREF_END_HTML, m.end())]
                for m in re.finditer(HREF_BEGIN_HTML, url_string)]
    return [split_href_wiki_text(href) for href in all_href if href.startswith(HREF_WIKI)]


def get_list_of_cities_async(city_href_list: list, processes_count=-1) -> list:
    def split_list(l: list, n) -> list:
        return [l[i:i + n] for i in range(0, len(l), n)]

    if processes_count == -1:
        processes_count = len(city_href_list)

    try:
        list_splitted = split_list(city_href_list, len(
            city_href_list) // processes_count)
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
    req = urlopen(city_href[HREF_KEY])
    data = str(req.read(), encoding="UTF-8")

    res = {}
    res[NAME] = city_href[HREF_TEXT_KEY]

    cords = get_coordinates_from_urlstring(data)
    res[LAT] = cords[LATITUDE_KEY]
    res[LON] = cords[LONGITUDE_KEY]
    res[REGION] = get_adm_region_from_urlstring(data, append_region_name=True)

    return res


def get_city_attr_async(city_href_list: list) -> list:
    res = []
    for href in city_href_list:
        try:
            item = get_city_attr_from_href(href)
            res.append(item)
            print(item)
        except:
            print("failed")

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


def optimized_output(cities_attr: list) -> dict:
    cities = cities_attr.copy()
    dups = get_duplicate_cities(cities)
    for city in cities:
        if city in dups:
            city[NAME] = f'{city[NAME]}, {city[REGION]}'
        del city[REGION]

    keys = [NAME, LAT, LON]
    data = flatten([list(city.values()) for city in cities])

    return {KEYS: keys, DATA: data}


if __name__ == "__main__":
    with open(WIKI_SOURCE_FILE, 'r') as f:
        data = str(f.read())

    time_start = time.time()

    final_dict = {}

    res = get_list_of_cities_async(get_all_wiki_href(data), processes_count=-1)

    print('duplicates:', get_duplicate_cities(res))

    final_dict[COUNTRY] = optimized_output(res)

    with open(OUTPUT_FILE, 'w', encoding="UTF-8") as f:
        json.dump(final_dict, f, indent=4, ensure_ascii=False)

    time_end = time.time()
    print(f'\nFinished all tasks in {round(time_end - time_start, 2)} seconds')
