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

PROGRAM_PATH = os.path.dirname(os.path.abspath(__file__))
WIKI_SOURCE_FILE = os.path.join(PROGRAM_PATH, "wiki.source")
OUTPUT_FILE = os.path.join(PROGRAM_PATH, "output.json")


# examples: "province", "state", "voivodeship", "region"
ADM_REGION_HTML = '<a href="/wiki/Voivodeships_of_Poland" title="Voivodeships of Poland">Voivodeship</a>'

result_list = []


def get_coordinates_from_urlstring(url_string: str) -> dict:
    def split_cord_chars(cord_str: str) -> list:
        for char in CORD_CHARS:
            cord_str = cord_str.replace(char, ' ')

        res = [int(x) for x in cord_str.split()]
        if len(res) < 3:
            res.append(0)
        return res

    lat_index_start = url_string.find(LATITUDE_HTML) + len(LATITUDE_HTML)
    lat_index_end = url_string.find(SPAN_END_HTML, lat_index_start) - 1

    lon_index_start = url_string.find(LONGITUDE_HTML) + len(LONGITUDE_HTML)
    lon_index_end = url_string.find(SPAN_END_HTML, lon_index_start) - 1

    res = {}
    res[LATITUDE_KEY] = split_cord_chars(
        url_string[lat_index_start:lat_index_end])
    res[LONGITUDE_KEY] = split_cord_chars(
        url_string[lon_index_start:lon_index_end])

    return res


def split_href_wiki_text(href_wiki: str) -> tuple:
    res = {}
    href = href_wiki[:href_wiki.find('"')]
    res[HREF_KEY] = f'{WIKIPEDIA_ADDRESS}{href}'
    res[HREF_TEXT_KEY] = href_wiki[href_wiki.find('>') + 1:]
    return res


def get_adm_region_from_urlstring(url_string: str) -> str:
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

    return split_href_wiki_text(href)[HREF_TEXT_KEY]


def get_all_wiki_href(url_string: str) -> dict:
    all_href = [url_string[m.end(): url_string.find(HREF_END_HTML, m.end())]
                for m in re.finditer(HREF_BEGIN_HTML, url_string)]
    return [split_href_wiki_text(href) for href in all_href if href.startswith(HREF_WIKI)]


def log_result(result):
    result_list.append(result)


def get_list_of_cities_async(city_href_list: list, processes_count=-1) -> list:
    def split_list(l: list, n) -> list:
        return [l[i:i + n] for i in range(0, len(l), n)]

    if processes_count == -1:
        processes_count = len(city_href_list)

    try:
        list_splitted = split_list(city_href_list, len(
            city_href_list) // processes_count)
    except:
        print("ERROR! Too many processes. To use maximum number of thread set 'processes_count' to -1")
        return []

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
    res[REGION] = get_adm_region_from_urlstring(data)

    cords = get_coordinates_from_urlstring(data)
    res[LAT] = cords[LATITUDE_KEY]
    res[LON] = cords[LONGITUDE_KEY]

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


if __name__ == "__main__":
    with open(WIKI_SOURCE_FILE, 'r') as f:
        data = str(f.read())

    time_start = time.time()

    res = get_list_of_cities_async(get_all_wiki_href(data))

    time_end = time.time()

    print(f'\nFinished all tasks in {round(time_end - time_start, 2)} seconds')

    with open(OUTPUT_FILE, 'w', encoding="UTF-8") as f:
        json.dump(res, f, indent=4, ensure_ascii=False)
