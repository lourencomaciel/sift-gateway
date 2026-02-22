"""Questions and gold-answer functions for each benchmark dataset."""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass
import hashlib
from typing import Any


@dataclass(frozen=True)
class Question:
    """A benchmark question with a gold-answer function."""

    dataset_name: str
    question_id: str
    question_text: str
    question_type: str
    gold_answer_fn: Callable[[Any], str]
    answer_type: str
    tolerance: float = 0.0


def _safe_float(val: Any) -> float:
    try:
        return float(val)
    except (TypeError, ValueError):
        return 0.0


# -- earthquakes --


def _eq_count_mag_gte4(data: Any) -> str:
    count = sum(
        1
        for f in data
        if isinstance(f, dict)
        and isinstance(f.get("properties"), dict)
        and _safe_float(f["properties"].get("mag")) >= 4.0
    )
    return str(count)


def _eq_avg_mag(data: Any) -> str:
    mags = [
        _safe_float(f["properties"]["mag"])
        for f in data
        if isinstance(f, dict)
        and isinstance(f.get("properties"), dict)
        and f["properties"].get("mag") is not None
    ]
    return f"{sum(mags) / len(mags):.2f}" if mags else "0"


def _eq_place_highest_mag(data: Any) -> str:
    best = max(
        (
            f
            for f in data
            if isinstance(f, dict)
            and isinstance(f.get("properties"), dict)
            and f["properties"].get("mag") is not None
        ),
        key=lambda f: _safe_float(f["properties"]["mag"]),
        default=None,
    )
    if best is None:
        return ""
    return str(best["properties"].get("place", ""))


def _eq_tsunami_count(data: Any) -> str:
    count = sum(
        1
        for f in data
        if isinstance(f, dict)
        and isinstance(f.get("properties"), dict)
        and f["properties"].get("tsunami") == 1
    )
    return str(count)


def _eq_max_depth(data: Any) -> str:
    depths = [
        _safe_float(f["geometry"]["coordinates"][2])
        for f in data
        if isinstance(f, dict)
        and isinstance(f.get("geometry"), dict)
        and isinstance(f["geometry"].get("coordinates"), list)
        and len(f["geometry"]["coordinates"]) >= 3
    ]
    return f"{max(depths):.2f}" if depths else "0"


def _eq_most_reporting_net(data: Any) -> str:
    nets: list[str] = [
        f["properties"]["net"]
        for f in data
        if isinstance(f, dict)
        and isinstance(f.get("properties"), dict)
        and isinstance(f["properties"].get("net"), str)
    ]
    if not nets:
        return ""
    return Counter(nets).most_common(1)[0][0]


# -- products --


def _prod_count_price_gt100(data: Any) -> str:
    return str(
        sum(
            1
            for p in data
            if isinstance(p, dict) and _safe_float(p.get("price")) > 100
        )
    )


def _prod_avg_rating(data: Any) -> str:
    ratings = [
        _safe_float(p["rating"])
        for p in data
        if isinstance(p, dict) and p.get("rating") is not None
    ]
    return f"{sum(ratings) / len(ratings):.2f}" if ratings else "0"


def _prod_cheapest_title(data: Any) -> str:
    cheapest = min(
        (p for p in data if isinstance(p, dict) and p.get("price") is not None),
        key=lambda p: _safe_float(p["price"]),
        default=None,
    )
    return str(cheapest["title"]) if cheapest else ""


def _prod_top_brand(data: Any) -> str:
    brands: list[str] = [
        p["brand"]
        for p in data
        if isinstance(p, dict) and isinstance(p.get("brand"), str)
    ]
    if not brands:
        return ""
    return Counter(brands).most_common(1)[0][0]


def _prod_category_count(data: Any) -> str:
    cats = {
        p["category"]
        for p in data
        if isinstance(p, dict) and isinstance(p.get("category"), str)
    }
    return str(len(cats))


def _prod_total_stock(data: Any) -> str:
    total = sum(
        int(_safe_float(p["stock"]))
        for p in data
        if isinstance(p, dict) and isinstance(p.get("stock"), (int, float))
    )
    return str(total)


# -- users --


def _users_count_age_gt40(data: Any) -> str:
    return str(
        sum(
            1
            for u in data
            if isinstance(u, dict)
            and isinstance(u.get("age"), (int, float))
            and u["age"] > 40
        )
    )


def _users_email_by_username(data: Any) -> str:
    for u in data:
        if (
            isinstance(u, dict)
            and isinstance(u.get("username"), str)
            and u["username"] == "emilys"
        ):
            return str(u.get("email", ""))
    return ""


def _users_most_common_city(data: Any) -> str:
    cities: list[str] = []
    for u in data:
        if not isinstance(u, dict):
            continue
        addr = u.get("address")
        if isinstance(addr, dict) and isinstance(addr.get("city"), str):
            cities.append(addr["city"])
    if not cities:
        return ""
    return Counter(cities).most_common(1)[0][0]


def _users_avg_weight(data: Any) -> str:
    weights = [
        _safe_float(u["weight"])
        for u in data
        if isinstance(u, dict) and u.get("weight") is not None
    ]
    return f"{sum(weights) / len(weights):.2f}" if weights else "0"


def _users_blood_group_count(data: Any) -> str:
    count = sum(
        1 for u in data if isinstance(u, dict) and u.get("bloodGroup") == "A+"
    )
    return str(count)


# -- comments --


def _comments_total(data: Any) -> str:
    return str(len(data))


def _comments_count_post1(data: Any) -> str:
    return str(
        sum(1 for c in data if isinstance(c, dict) and c.get("postId") == 1)
    )


def _comments_email_by_id(data: Any) -> str:
    for c in data:
        if isinstance(c, dict) and c.get("id") == 1:
            return str(c.get("email", ""))
    return ""


def _comments_most_commented_post(data: Any) -> str:
    posts: list[int] = [
        c["postId"]
        for c in data
        if isinstance(c, dict) and isinstance(c.get("postId"), int)
    ]
    if not posts:
        return ""
    return str(Counter(posts).most_common(1)[0][0])


def _comments_avg_body_len(data: Any) -> str:
    lengths = [
        len(c["body"])
        for c in data
        if isinstance(c, dict) and isinstance(c.get("body"), str)
    ]
    return f"{sum(lengths) / len(lengths):.2f}" if lengths else "0"


# -- photos --


def _photos_total(data: Any) -> str:
    return str(len(data))


def _photos_count_album1(data: Any) -> str:
    return str(
        sum(1 for p in data if isinstance(p, dict) and p.get("albumId") == 1)
    )


def _photos_most_photos_album(data: Any) -> str:
    albums: list[int] = [
        p["albumId"]
        for p in data
        if isinstance(p, dict) and isinstance(p.get("albumId"), int)
    ]
    if not albums:
        return ""
    return str(Counter(albums).most_common(1)[0][0])


def _photos_title_by_id(data: Any) -> str:
    for p in data:
        if isinstance(p, dict) and p.get("id") == 1:
            return str(p.get("title", ""))
    return ""


def _photos_distinct_albums(data: Any) -> str:
    albums = {
        p["albumId"]
        for p in data
        if isinstance(p, dict) and isinstance(p.get("albumId"), int)
    }
    return str(len(albums))


# -- countries --


def _countries_pop_gt100m(data: Any) -> str:
    return str(
        sum(
            1
            for c in data
            if isinstance(c, dict)
            and isinstance(c.get("population"), (int, float))
            and c["population"] > 100_000_000
        )
    )


def _countries_capital_lookup(data: Any) -> str:
    for c in data:
        if not isinstance(c, dict):
            continue
        name = c.get("name")
        if isinstance(name, dict) and name.get("common") == "France":
            caps = c.get("capital")
            if isinstance(caps, list) and caps:
                return str(caps[0])
    return ""


def _countries_europe_population(data: Any) -> str:
    total = sum(
        int(c["population"])
        for c in data
        if isinstance(c, dict)
        and c.get("region") == "Europe"
        and isinstance(c.get("population"), (int, float))
    )
    return str(total)


def _countries_most_countries_subregion(data: Any) -> str:
    subregions: list[str] = [
        c["subregion"]
        for c in data
        if isinstance(c, dict)
        and isinstance(c.get("subregion"), str)
        and c["subregion"]
    ]
    if not subregions:
        return ""
    return Counter(subregions).most_common(1)[0][0]


def _countries_landlocked_count(data: Any) -> str:
    return str(
        sum(
            1
            for c in data
            if isinstance(c, dict) and c.get("landlocked") is True
        )
    )


def _countries_largest_area(data: Any) -> str:
    largest = max(
        (
            c
            for c in data
            if isinstance(c, dict) and isinstance(c.get("area"), (int, float))
        ),
        key=lambda c: _safe_float(c["area"]),
        default=None,
    )
    if largest is None:
        return ""
    name = largest.get("name")
    if isinstance(name, dict):
        return str(name.get("common", ""))
    return ""


# -- laureates --


def _laureates_total(data: Any) -> str:
    return str(len(data))


def _laureates_female_count(data: Any) -> str:
    return str(
        sum(
            1
            for la in data
            if isinstance(la, dict) and la.get("gender") == "female"
        )
    )


def _laureates_most_common_category(data: Any) -> str:
    cats: list[str] = []
    for la in data:
        if not isinstance(la, dict):
            continue
        prizes = la.get("nobelPrizes")
        if not isinstance(prizes, list):
            continue
        for prize in prizes:
            if isinstance(prize, dict):
                cat = prize.get("category")
                if isinstance(cat, dict):
                    en = cat.get("en")
                    if isinstance(en, str):
                        cats.append(en)
    if not cats:
        return ""
    return Counter(cats).most_common(1)[0][0]


def _laureates_born_after_1950(data: Any) -> str:
    count = 0
    for la in data:
        if not isinstance(la, dict):
            continue
        born = la.get("birth", {})
        if not isinstance(born, dict):
            continue
        date = born.get("date")
        if isinstance(date, str) and len(date) >= 4:
            try:
                if int(date[:4]) > 1950:
                    count += 1
            except ValueError:
                pass
    return str(count)


def _laureates_distinct_birth_countries(data: Any) -> str:
    countries: set[str] = set()
    for la in data:
        if not isinstance(la, dict):
            continue
        born = la.get("birth", {})
        if not isinstance(born, dict):
            continue
        place = born.get("place")
        if not isinstance(place, dict):
            continue
        country = place.get("country")
        if isinstance(country, dict):
            en = country.get("en")
            if isinstance(en, str):
                countries.add(en)
    return str(len(countries))


# -- weather --
# Gold functions receive the raw downloaded dict (with top-level
# "hourly" key), NOT the Sift-extracted root_path slice.  The
# harness calls gold_answer_fn(loaded[name]) before any Sift
# capture or root selection.


def _weather_hourly_count(data: Any) -> str:
    hourly = data.get("hourly") if isinstance(data, dict) else None
    if not isinstance(hourly, dict):
        return "0"
    time_arr = hourly.get("time")
    if isinstance(time_arr, list):
        return str(len(time_arr))
    return "0"


def _weather_avg_temp(data: Any) -> str:
    hourly = data.get("hourly") if isinstance(data, dict) else None
    if not isinstance(hourly, dict):
        return "0"
    temps = hourly.get("temperature_2m")
    if not isinstance(temps, list):
        return "0"
    valid = [t for t in temps if isinstance(t, (int, float))]
    if not valid:
        return "0"
    return f"{sum(valid) / len(valid):.2f}"


def _weather_max_wind(data: Any) -> str:
    hourly = data.get("hourly") if isinstance(data, dict) else None
    if not isinstance(hourly, dict):
        return "0"
    winds = hourly.get("wind_speed_10m")
    if not isinstance(winds, list):
        return "0"
    valid = [w for w in winds if isinstance(w, (int, float))]
    return f"{max(valid):.1f}" if valid else "0"


def _weather_precip_hours(data: Any) -> str:
    hourly = data.get("hourly") if isinstance(data, dict) else None
    if not isinstance(hourly, dict):
        return "0"
    precip = hourly.get("precipitation")
    if not isinstance(precip, list):
        return "0"
    return str(sum(1 for p in precip if isinstance(p, (int, float)) and p > 0))


def _weather_total_precip(data: Any) -> str:
    hourly = data.get("hourly") if isinstance(data, dict) else None
    if not isinstance(hourly, dict):
        return "0"
    precip = hourly.get("precipitation")
    if not isinstance(precip, list):
        return "0"
    total = sum(p for p in precip if isinstance(p, (int, float)))
    return f"{total:.2f}"


# -- question registry --


QUESTIONS: list[Question] = [
    # earthquakes (6)
    Question(
        dataset_name="earthquakes",
        question_id="eq_mag_gte4",
        question_text=(
            "How many earthquakes in this dataset have a magnitude "
            "of 4.0 or greater?"
        ),
        question_type="count",
        gold_answer_fn=_eq_count_mag_gte4,
        answer_type="number",
    ),
    Question(
        dataset_name="earthquakes",
        question_id="eq_avg_mag",
        question_text=(
            "What is the average magnitude of all earthquakes "
            "in this dataset? Give two decimal places."
        ),
        question_type="aggregation",
        gold_answer_fn=_eq_avg_mag,
        answer_type="number",
        tolerance=0.01,
    ),
    Question(
        dataset_name="earthquakes",
        question_id="eq_highest_place",
        question_text=(
            "What is the place name of the earthquake with the "
            "highest magnitude?"
        ),
        question_type="lookup",
        gold_answer_fn=_eq_place_highest_mag,
        answer_type="string",
    ),
    Question(
        dataset_name="earthquakes",
        question_id="eq_tsunami",
        question_text=(
            "How many earthquakes in this dataset have a tsunami flag of 1?"
        ),
        question_type="filter",
        gold_answer_fn=_eq_tsunami_count,
        answer_type="number",
    ),
    Question(
        dataset_name="earthquakes",
        question_id="eq_max_depth",
        question_text=(
            "What is the maximum depth (in km) among all earthquakes? "
            "Give two decimal places."
        ),
        question_type="aggregation",
        gold_answer_fn=_eq_max_depth,
        answer_type="number",
        tolerance=0.01,
    ),
    Question(
        dataset_name="earthquakes",
        question_id="eq_top_net",
        question_text=(
            "Which reporting network (net) has the most earthquakes "
            "in this dataset?"
        ),
        question_type="cross_field",
        gold_answer_fn=_eq_most_reporting_net,
        answer_type="string",
    ),
    # products (6)
    Question(
        dataset_name="products",
        question_id="prod_price_gt100",
        question_text=(
            "How many products have a price strictly greater than 100?"
        ),
        question_type="count",
        gold_answer_fn=_prod_count_price_gt100,
        answer_type="number",
    ),
    Question(
        dataset_name="products",
        question_id="prod_avg_rating",
        question_text=(
            "What is the average rating across all products? "
            "Give two decimal places."
        ),
        question_type="aggregation",
        gold_answer_fn=_prod_avg_rating,
        answer_type="number",
        tolerance=0.01,
    ),
    Question(
        dataset_name="products",
        question_id="prod_cheapest",
        question_text="What is the title of the cheapest product?",
        question_type="lookup",
        gold_answer_fn=_prod_cheapest_title,
        answer_type="string",
    ),
    Question(
        dataset_name="products",
        question_id="prod_top_brand",
        question_text=("Which brand has the most products in this dataset?"),
        question_type="cross_field",
        gold_answer_fn=_prod_top_brand,
        answer_type="string",
    ),
    Question(
        dataset_name="products",
        question_id="prod_categories",
        question_text=(
            "How many distinct product categories are in this dataset?"
        ),
        question_type="count",
        gold_answer_fn=_prod_category_count,
        answer_type="number",
    ),
    Question(
        dataset_name="products",
        question_id="prod_total_stock",
        question_text=("What is the total stock across all products?"),
        question_type="aggregation",
        gold_answer_fn=_prod_total_stock,
        answer_type="number",
    ),
    # users (5)
    Question(
        dataset_name="users",
        question_id="users_age_gt40",
        question_text="How many users are older than 40?",
        question_type="count",
        gold_answer_fn=_users_count_age_gt40,
        answer_type="number",
    ),
    Question(
        dataset_name="users",
        question_id="users_email_emilys",
        question_text=(
            "What is the email address of the user with username 'emilys'?"
        ),
        question_type="lookup",
        gold_answer_fn=_users_email_by_username,
        answer_type="string",
    ),
    Question(
        dataset_name="users",
        question_id="users_top_city",
        question_text=("What is the most common city among user addresses?"),
        question_type="cross_field",
        gold_answer_fn=_users_most_common_city,
        answer_type="string",
    ),
    Question(
        dataset_name="users",
        question_id="users_avg_weight",
        question_text=(
            "What is the average weight of all users? Give two decimal places."
        ),
        question_type="aggregation",
        gold_answer_fn=_users_avg_weight,
        answer_type="number",
        tolerance=0.01,
    ),
    Question(
        dataset_name="users",
        question_id="users_blood_aplus",
        question_text=("How many users have blood group A+?"),
        question_type="filter",
        gold_answer_fn=_users_blood_group_count,
        answer_type="number",
    ),
    # comments (5)
    Question(
        dataset_name="comments",
        question_id="comments_total",
        question_text="How many total comments are in this dataset?",
        question_type="count",
        gold_answer_fn=_comments_total,
        answer_type="number",
    ),
    Question(
        dataset_name="comments",
        question_id="comments_post1",
        question_text=("How many comments belong to postId 1?"),
        question_type="filter",
        gold_answer_fn=_comments_count_post1,
        answer_type="number",
    ),
    Question(
        dataset_name="comments",
        question_id="comments_email_id1",
        question_text=("What is the email of the comment with id 1?"),
        question_type="lookup",
        gold_answer_fn=_comments_email_by_id,
        answer_type="string",
    ),
    # answer_type="number" so match_number handles the str→float
    # conversion from the gold function's str(Counter(...)) output.
    Question(
        dataset_name="comments",
        question_id="comments_top_post",
        question_text=("Which postId has the most comments?"),
        question_type="aggregation",
        gold_answer_fn=_comments_most_commented_post,
        answer_type="number",
    ),
    Question(
        dataset_name="comments",
        question_id="comments_avg_body",
        question_text=(
            "What is the average body text length (in characters) "
            "of all comments? Give two decimal places."
        ),
        question_type="aggregation",
        gold_answer_fn=_comments_avg_body_len,
        answer_type="number",
        tolerance=0.01,
    ),
    # photos (5)
    Question(
        dataset_name="photos",
        question_id="photos_total",
        question_text="How many total photos are in this dataset?",
        question_type="count",
        gold_answer_fn=_photos_total,
        answer_type="number",
    ),
    Question(
        dataset_name="photos",
        question_id="photos_album1",
        question_text="How many photos belong to albumId 1?",
        question_type="filter",
        gold_answer_fn=_photos_count_album1,
        answer_type="number",
    ),
    Question(
        dataset_name="photos",
        question_id="photos_top_album",
        question_text="Which albumId has the most photos?",
        question_type="aggregation",
        gold_answer_fn=_photos_most_photos_album,
        answer_type="number",
    ),
    Question(
        dataset_name="photos",
        question_id="photos_title_id1",
        question_text="What is the title of the photo with id 1?",
        question_type="lookup",
        gold_answer_fn=_photos_title_by_id,
        answer_type="string",
    ),
    Question(
        dataset_name="photos",
        question_id="photos_distinct_albums",
        question_text=("How many distinct albumIds are in this dataset?"),
        question_type="count",
        gold_answer_fn=_photos_distinct_albums,
        answer_type="number",
    ),
    # countries (6)
    Question(
        dataset_name="countries",
        question_id="countries_pop_gt100m",
        question_text=(
            "How many countries have a population greater than 100 million?"
        ),
        question_type="count",
        gold_answer_fn=_countries_pop_gt100m,
        answer_type="number",
    ),
    Question(
        dataset_name="countries",
        question_id="countries_capital_france",
        question_text="What is the capital of France?",
        question_type="lookup",
        gold_answer_fn=_countries_capital_lookup,
        answer_type="string",
    ),
    Question(
        dataset_name="countries",
        question_id="countries_europe_pop",
        question_text=(
            "What is the total population of all European "
            "countries in this dataset?"
        ),
        question_type="aggregation",
        gold_answer_fn=_countries_europe_population,
        answer_type="number",
        # Population sum (~741 M) can differ by small amounts
        # when LLM-generated code uses float arithmetic instead
        # of exact int sums.
        tolerance=1000,
    ),
    Question(
        dataset_name="countries",
        question_id="countries_top_subregion",
        question_text=("Which subregion has the most countries?"),
        question_type="cross_field",
        gold_answer_fn=_countries_most_countries_subregion,
        answer_type="string",
    ),
    Question(
        dataset_name="countries",
        question_id="countries_landlocked",
        question_text=("How many countries are landlocked?"),
        question_type="count",
        gold_answer_fn=_countries_landlocked_count,
        answer_type="number",
    ),
    Question(
        dataset_name="countries",
        question_id="countries_largest_area",
        question_text=("Which country has the largest area?"),
        question_type="lookup",
        gold_answer_fn=_countries_largest_area,
        answer_type="string",
    ),
    # laureates (5)
    Question(
        dataset_name="laureates",
        question_id="laureates_total",
        question_text="How many laureates are in this dataset?",
        question_type="count",
        gold_answer_fn=_laureates_total,
        answer_type="number",
    ),
    Question(
        dataset_name="laureates",
        question_id="laureates_female",
        question_text="How many female laureates are there?",
        question_type="filter",
        gold_answer_fn=_laureates_female_count,
        answer_type="number",
    ),
    Question(
        dataset_name="laureates",
        question_id="laureates_top_category",
        question_text=("What is the most common Nobel Prize category?"),
        question_type="cross_field",
        gold_answer_fn=_laureates_most_common_category,
        answer_type="string",
    ),
    Question(
        dataset_name="laureates",
        question_id="laureates_born_after_1950",
        question_text=("How many laureates were born after 1950?"),
        question_type="filter",
        gold_answer_fn=_laureates_born_after_1950,
        answer_type="number",
    ),
    Question(
        dataset_name="laureates",
        question_id="laureates_birth_countries",
        question_text=(
            "How many distinct birth countries are represented among laureates?"
        ),
        question_type="count",
        gold_answer_fn=_laureates_distinct_birth_countries,
        answer_type="number",
    ),
    # weather (5)
    Question(
        dataset_name="weather",
        question_id="weather_hourly_count",
        question_text=("How many hourly data points are in this dataset?"),
        question_type="count",
        gold_answer_fn=_weather_hourly_count,
        answer_type="number",
    ),
    Question(
        dataset_name="weather",
        question_id="weather_avg_temp",
        question_text=(
            "What is the average temperature (temperature_2m) "
            "across all hours? Give two decimal places."
        ),
        question_type="aggregation",
        gold_answer_fn=_weather_avg_temp,
        answer_type="number",
        tolerance=0.01,
    ),
    Question(
        dataset_name="weather",
        question_id="weather_max_wind",
        question_text=(
            "What is the maximum wind speed (wind_speed_10m) "
            "recorded? Give one decimal place."
        ),
        question_type="aggregation",
        gold_answer_fn=_weather_max_wind,
        answer_type="number",
        tolerance=0.1,
    ),
    Question(
        dataset_name="weather",
        question_id="weather_precip_hours",
        question_text=("How many hours had precipitation greater than 0?"),
        question_type="filter",
        gold_answer_fn=_weather_precip_hours,
        answer_type="number",
    ),
    Question(
        dataset_name="weather",
        question_id="weather_total_precip",
        question_text=(
            "What is the total precipitation across all hours? "
            "Give two decimal places."
        ),
        question_type="aggregation",
        gold_answer_fn=_weather_total_precip,
        answer_type="number",
        tolerance=0.01,
    ),
]


def get_questions_for_dataset(
    dataset_name: str,
) -> list[Question]:
    """Return questions for a specific dataset."""
    return [q for q in QUESTIONS if q.dataset_name == dataset_name]


def question_set_hash() -> str:
    """Return a short SHA-256 hash identifying the current question set.

    The hash covers question IDs, text, types, answer types, tolerance,
    and gold function names so that changes to the question set produce
    a different hash for cross-run comparison validation.
    """
    parts = [
        f"{q.dataset_name}:{q.question_id}:{q.question_text}"
        f":{q.question_type}:{q.answer_type}:{q.tolerance}"
        f":{q.gold_answer_fn.__name__}"
        for q in QUESTIONS
    ]
    digest = hashlib.sha256("\n".join(parts).encode()).hexdigest()
    return digest[:12]
