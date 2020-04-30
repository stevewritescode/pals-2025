#!/usr/bin/env python3

from dataclasses import dataclass, field
from typing import Dict, List, Final
import json
import os
import requests
import marshmallow
import marshmallow_dataclass


@dataclass
class CensusDataResponse:
    """Class for Census Report Data API."""

    class Meta:
        unknown = marshmallow.EXCLUDE

    @dataclass
    class DataItem:
        class Meta:
            unknown = marshmallow.EXCLUDE

        estimate: Dict[str, float]

    data: Dict[str, Dict[str, DataItem]]

    @dataclass
    class TableDefinition:
        class Meta:
            unknown = marshmallow.EXCLUDE

        @dataclass
        class ColumnDefinition:
            indent: int
            name: str

        columns: Dict[str, ColumnDefinition]
        title: str

    tables: Dict[str, TableDefinition]


CensusDataResponseSchema = marshmallow_dataclass.class_schema(CensusDataResponse)

# fmt: off

# https://mapchart.net/usa-counties.html

# List of USA County Census Codes:
#    https://www2.census.gov/geo/pdfs/reference/ua/County_Rural_Lookup_v4.pdf
# List of Southern CA Counties:
#    https://en.wikipedia.org/wiki/Southern_California#Northern_boundary_of_southern_California
# List of Northern CA Counties:
#    https://en.wikipedia.org/wiki/Northern_California#Counties
# List of New York CSA Counties:
#    https://en.wikipedia.org/wiki/New_York_metropolitan_area#/media/File:New_York_Metropolitan_Area_Counties_2013.png
# Chicago CSA Counties:
#    https://www2.census.gov/geo/maps/econ/ec2012/csa/EC2012_330M200US176M.pdf

hub_list: Dict[str, List[str]] = {
    "Southern CA": [
        "05000US06037",  # Los Angeles County
        "05000US06073",  # San Diego County
        "05000US06059",  # Orange County
        "05000US06065",  # Riverside County
        "05000US06071",  # San Bernardino County
        "05000US06029",  # Kern County
        "05000US06111",  # Ventura County
        "05000US06083",  # Santa Barbara County
        "05000US06079",  # San Luis Obispo County
        "05000US06025",  # Imperial County
    ],
    "Northern CA": [
        "05000US06001",  # Alameda County
        "05000US06003",  # Alpine County
        "05000US06005",  # Amador County
        "05000US06007",  # Butte County
        "05000US06009",  # Calaveras County
        "05000US06011",  # Colusa County
        "05000US06013",  # Contra Costa County
        "05000US06015",  # Del Norte County
        "05000US06017",  # El Dorado County
        "05000US06019",  # Fresno County
        "05000US06021",  # Glenn County
        "05000US06023",  # Humboldt County
        "05000US06027",  # Inyo County
        "05000US06031",  # Kings County
        "05000US06033",  # Lake County
        "05000US06035",  # Lassen County
        "05000US06039",  # Madera County
        "05000US06041",  # Marin County
        "05000US06043",  # Mariposa County
        "05000US06045",  # Mendocino County
        "05000US06047",  # Merced County
        "05000US06049",  # Modoc County
        "05000US06051",  # Mono County
        "05000US06053",  # Monterey County
        "05000US06055",  # Napa County
        "05000US06057",  # Nevada County
        "05000US06061",  # Placer County
        "05000US06063",  # Plumas County
        "05000US06067",  # Sacramento County
        "05000US06069",  # San Benito County
        "05000US06075",  # San Francisco County
        "05000US06077",  # San Joaquin County
        "05000US06081",  # San Mateo County
        "05000US06085",  # Santa Clara County
        "05000US06087",  # Santa Cruz County
        "05000US06089",  # Shasta County
        "05000US06091",  # Sierra County
        "05000US06093",  # Siskiyou County
        "05000US06095",  # Solano County
        "05000US06097",  # Sonoma County
        "05000US06099",  # Stanislaus County
        "05000US06101",  # Sutter County
        "05000US06103",  # Tehama County
        "05000US06105",  # Trinity County
        "05000US06107",  # Tulare County
        "05000US06109",  # Tuolumne County
        "05000US06113",  # Yolo County
        "05000US06115",  # Yuba County
    ],
    "Pacfic Northwest": [
        "04000US53",  # Washington State
        "04000US41",  # Oregon State
    ],
    "Ohio": [
        "04000US39",  # Ohio State
    ],
    "Illinois": [
        "04000US17",     # Illinois State
        "05000US55059",  # Wisconsin - Kenosha County
        "05000US18089",  # Indiana - Lake County
        "05000US18127",  # Indiana - Porter County
        "05000US18111",  # Indiana - Newton County
        "05000US18073",  # Indiana - Jasper County
        "05000US18091",  # Indiana - LaPorte County
    ],
    "Philadelphia": [
        "33000US428",    # Philadelphia-Reading-Camden, PA-NJ-DE-MD CSA
        "05000US10005",  # Delaware - Sussex County
        "05000US42071",  # Pennsylvania - Lancaster County
        "33000US276",    # Harrisburg-York-Lebanon, PA CSA
    ],
    "New York City": [
        "33000US408"  # New York-Newark, NY-NJ-CT-PA CSA
    ],
    "New England": [
        "04000US23",     # Maine
        "04000US50",     # Vermont
        "04000US33",     # New Hampshire
        "04000US25",     # Massachusetts
        "05000US09003",  # Connecticut - Hartford County
        "05000US09007",  # Connecticut - Middlesex County
        "05000US09013",  # Connecticut - Tolland County
        "05000US09015",  # Connecticut - Windham County
        "05000US09011",  # Connecticut - New London County
    ],
    "Mid Atlantic": [
        "33000US548",    # Washington-Baltimore-Arlington, DC-MD-VA-WV-PA CSA
        "05000US24029",  # Maryland - Kent County
        "05000US24011",  # Maryland - Caroline County
        "05000US24045",  # Maryland - Wicomico County
        "05000US24039",  # Maryland - Somerset County
        "05000US24047",  # Maryland - Worchester County
    ],
    "South Atlantic": [
        "04000US37",  # North Carolina State
        "04000US45",  # South Carolina State
    ],
}
# fmt: on

CENSUS_REPORTER_URL: Final = "https://api.censusreporter.org/1.0/data/show/latest"


def get_census_data_response(table_id: str, geo_id: str) -> CensusDataResponse:
    CACHE_DIR: Final = "./cache/"
    if not os.path.exists(CACHE_DIR):
        os.makedirs(CACHE_DIR)

    cache_path: str = os.path.join(CACHE_DIR, f"{table_id}.{geo_id}.json")
    js_data: dict
    if os.path.exists(cache_path):
        with open(cache_path, "r") as cache_file:
            js_data = json.loads(cache_file.read())
    else:
        print(f"getting data for {cache_path}")
        url: str = f"{CENSUS_REPORTER_URL}?table_ids={table_id}&geo_ids={geo_id}"
        r = requests.get(url)
        if r.status_code != 200:
            raise Exception(f"unexpected status code {r.status_code}")
        js_data = r.json()
        with open(cache_path, "w") as cache_file:
            cache_file.write(json.dumps(js_data))

    return CensusDataResponseSchema().load(js_data)


def get_age_data(geo: str) -> Dict[str, int]:
    AGE_ID: Final = "B01001"
    age_data = get_census_data_response(AGE_ID, geo)

    # Population and age breakdown
    age_categories: Dict[str, List[str]] = {
        "0-9": ["Under 5 years", "5 to 9 years"],
        "10-19": ["10 to 14 years", "15 to 17 years", "18 and 19 years"],
        "20-29": ["20 years", "21 years", "22 to 24 years", "25 to 29 years"],
        "30-39": ["30 to 34 years", "35 to 39 years"],
        "40-49": ["40 to 44 years", "45 to 49 years"],
        "50-59": ["50 to 54 years", "55 to 59 years"],
        "60-69": [
            "60 and 61 years",
            "62 to 64 years",
            "65 and 66 years",
            "67 to 69 years",
        ],
        "70+": [
            "70 to 74 years",
            "75 to 79 years",
            "80 to 84 years",
            "85 years and over",
        ],
    }

    age_estimates = age_data.data[geo][AGE_ID].estimate

    age_totals: Dict[str, int] = {}
    for category in age_categories:
        column_nums = []
        for column_name in age_categories[category]:
            for c in age_data.tables[AGE_ID].columns:
                column_def = age_data.tables[AGE_ID].columns[c]
                if column_def.name == column_name:
                    column_nums.append(c)

        total = 0
        for column_name in column_nums:
            total += int(age_estimates[column_name])

        age_totals[category] = total

    return age_totals


def get_race_data(geo: str) -> Dict[str, int]:
    RACE_ID: Final = "B03002"
    race_data = get_census_data_response(RACE_ID, geo)

    race_categories: Dict[str, List[str]] = {
        "White": ["B03002003"],
        "Hispanic": ["B03002012"],
        "Black": ["B03002004"],
        "Asian": ["B03002006"],
        "Other": ["B03002007", "B03002008"],
        "Two+": ["B03002009"],
    }

    race_totals: Dict[str, int] = {}
    for category in race_categories:
        total = 0
        for column_num in race_categories[category]:
            total += int(race_data.data[geo][RACE_ID].estimate[column_num])
        race_totals[category] = total

    return race_totals


def get_language_data(geo: str) -> Dict[str, int]:
    LANGUAGE_ID: Final = "B16007"
    language_data = get_census_data_response(LANGUAGE_ID, geo)

    language_categories: Dict[str, List[str]] = {
        "English Only": ["B16007009"],
        "Spanish": ["B16007010"],
        "Other": ["B16007011", "B16007012", "B16007013"],
    }

    language_totals: Dict[str, int] = {}
    for category in language_categories:
        total = 0
        for column_num in language_categories[category]:
            total += int(language_data.data[geo][LANGUAGE_ID].estimate[column_num])
        language_totals[category] = total

    return language_totals


def get_income_data(geo: str) -> Dict[str, int]:
    INCOME_ID: Final = "B19001"
    income_data = get_census_data_response(INCOME_ID, geo)

    # Population and age breakdown
    income_categories: Dict[str, List[str]] = {
        "$0-24k": [
            "Less than $10,000",
            "$10,000 to $14,999",
            "$15,000 to $19,999",
            "$20,000 to $24,999",
        ],
        "$25k-49k": [
            "$25,000 to $29,999",
            "$30,000 to $34,999",
            "$35,000 to $39,999",
            "$40,000 to $44,999",
            "$45,000 to $49,999",
        ],
        "$50k-74k": ["$50,000 to $59,999", "$60,000 to $74,999"],
        "$75k-100k": ["$75,000 to $99,999"],
        "$100k-$124k": ["$100,000 to $124,999"],
        "$125k-$149k": ["$125,000 to $149,999"],
        "$150k-$200k": ["$150,000 to $199,999"],
        "$200k+": ["$200,000 or more"],
    }

    income_estimates = income_data.data[geo][INCOME_ID].estimate

    income_totals: Dict[str, int] = {}
    for category in income_categories:
        column_nums = []
        for column_name in income_categories[category]:
            for c in income_data.tables[INCOME_ID].columns:
                column_def = income_data.tables[INCOME_ID].columns[c]
                if column_def.name == column_name:
                    column_nums.append(c)

        total = 0
        for column_name in column_nums:
            total += int(income_estimates[column_name])

        income_totals[category] = total

    return income_totals


@dataclass()
class GeoData:
    age_totals: Dict[str, int] = field(default_factory=dict)
    race_totals: Dict[str, int] = field(default_factory=dict)
    language_totals: Dict[str, int] = field(default_factory=dict)
    income_totals: Dict[str, int] = field(default_factory=dict)


def get_geo_data(geo: str) -> GeoData:
    geo_data: GeoData = GeoData()

    try:
        geo_data.age_totals = get_age_data(geo)
        geo_data.race_totals = get_race_data(geo)
        geo_data.language_totals = get_language_data(geo)
        geo_data.income_totals = get_income_data(geo)
    except Exception as e:
        print(f"error in {geo}")
        raise e

    return geo_data


def get_hub_data(hub: str) -> None:
    geo_list: List[str] = hub_list[hub]

    final_totals: GeoData = GeoData()
    age_sum: int = 0
    race_sum: int = 0
    language_sum: int = 0
    income_sum: int = 0

    def sum_totals(current: Dict[str, int], add: Dict[str, int]) -> int:
        s: int = 0
        for num in add:
            current[num] = current.get(num, 0) + add[num]
            s += add[num]
        return s

    for geo in geo_list:
        totals: GeoData = get_geo_data(geo)
        age_sum += sum_totals(final_totals.age_totals, totals.age_totals)
        race_sum += sum_totals(final_totals.race_totals, totals.race_totals)
        language_sum += sum_totals(final_totals.language_totals, totals.language_totals)
        income_sum += sum_totals(final_totals.income_totals, totals.income_totals)

    if age_sum == 0:
        print(hub)
    else:

        def as_percent(num: float, total: int) -> float:
            return round(num / total * 100, 1)

        cols = [
            hub,
            round(age_sum / 1_000_000, 1),
            as_percent(final_totals.age_totals["0-9"], age_sum),
            as_percent(final_totals.age_totals["10-19"], age_sum),
            as_percent(final_totals.age_totals["20-29"], age_sum),
            as_percent(final_totals.age_totals["30-39"], age_sum),
            as_percent(final_totals.age_totals["40-49"], age_sum),
            as_percent(final_totals.age_totals["50-59"], age_sum),
            as_percent(final_totals.age_totals["60-69"], age_sum),
            as_percent(final_totals.age_totals["70+"], age_sum),
            as_percent(final_totals.race_totals["White"], race_sum),
            as_percent(final_totals.race_totals["Hispanic"], race_sum),
            as_percent(final_totals.race_totals["Black"], race_sum),
            as_percent(final_totals.race_totals["Asian"], race_sum),
            as_percent(final_totals.race_totals["Other"], race_sum),
            as_percent(final_totals.race_totals["Two+"], race_sum),
            as_percent(final_totals.language_totals["English Only"], language_sum),
            as_percent(final_totals.language_totals["Spanish"], language_sum),
            as_percent(final_totals.language_totals["Other"], language_sum),
            as_percent(final_totals.income_totals["$0-24k"], income_sum),
            as_percent(final_totals.income_totals["$25k-49k"], income_sum),
            as_percent(final_totals.income_totals["$50k-74k"], income_sum),
            as_percent(final_totals.income_totals["$75k-100k"], income_sum),
            as_percent(final_totals.income_totals["$100k-$124k"], income_sum),
            as_percent(final_totals.income_totals["$125k-$149k"], income_sum),
            as_percent(final_totals.income_totals["$150k-$200k"], income_sum),
            as_percent(final_totals.income_totals["$200k+"], income_sum),
        ]
        for i in range(0, len(cols)):
            if i != 0:
                print(",", end="")
            print(cols[i], end="")
        print()


for hub in hub_list:
    get_hub_data(hub)
