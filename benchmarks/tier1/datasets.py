"""Dataset definitions for Tier 1 benchmark."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Dataset:
    """A benchmark dataset source."""

    name: str
    url: str
    extraction_path: str | None
    local_filename: str


DATASETS: dict[str, Dataset] = {
    "earthquakes": Dataset(
        name="earthquakes",
        url=(
            "https://earthquake.usgs.gov/earthquakes/feed/v1.0"
            "/summary/all_week.geojson"
        ),
        extraction_path="features",
        local_filename="earthquakes.json",
    ),
    "products": Dataset(
        name="products",
        url="https://dummyjson.com/products?limit=0",
        extraction_path="products",
        local_filename="products.json",
    ),
    "users": Dataset(
        name="users",
        url="https://dummyjson.com/users?limit=0",
        extraction_path="users",
        local_filename="users.json",
    ),
    "comments": Dataset(
        name="comments",
        url="https://jsonplaceholder.typicode.com/comments",
        extraction_path=None,
        local_filename="comments.json",
    ),
    "photos": Dataset(
        name="photos",
        url="https://jsonplaceholder.typicode.com/photos",
        extraction_path=None,
        local_filename="photos.json",
    ),
    "countries": Dataset(
        name="countries",
        url=(
            "https://restcountries.com/v3.1/all"
            "?fields=name,capital,region,subregion,"
            "population,area,landlocked"
        ),
        extraction_path=None,
        local_filename="countries.json",
    ),
    "laureates": Dataset(
        name="laureates",
        url=("https://api.nobelprize.org/2.1/laureates?limit=1000&offset=0"),
        extraction_path="laureates",
        local_filename="laureates.json",
    ),
    # Date range is fixed for reproducibility — gold answers are
    # computed at runtime from the fetched data, but pinning the
    # range ensures consistent item counts across runs.
    "weather": Dataset(
        name="weather",
        url=(
            "https://archive-api.open-meteo.com/v1/archive"
            "?latitude=52.52&longitude=13.41"
            "&start_date=2025-01-01&end_date=2025-03-31"
            "&hourly=temperature_2m,wind_speed_10m,precipitation"
        ),
        extraction_path=None,
        local_filename="weather.json",
    ),
}

ALL_DATASET_NAMES = list(DATASETS.keys())
