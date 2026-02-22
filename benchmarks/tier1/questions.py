"""Questions and gold-answer functions for each benchmark dataset."""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable
import contextlib
from dataclasses import dataclass
import hashlib
import json
import statistics
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
    difficulty: int = 1


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


def _eq_avg_mag_us_net(data: Any) -> str:
    mags = [
        _safe_float(f["properties"]["mag"])
        for f in data
        if isinstance(f, dict)
        and isinstance(f.get("properties"), dict)
        and f["properties"].get("mag") is not None
        and f["properties"].get("net") == "us"
    ]
    return f"{sum(mags) / len(mags):.2f}" if mags else "0"


# Boolean gold functions return "Yes"/"No" strings; match_boolean
# accepts these as _TRUE_VARIANTS / _FALSE_VARIANTS respectively.
def _eq_any_mag_gt7(data: Any) -> str:
    for f in data:
        if (
            isinstance(f, dict)
            and isinstance(f.get("properties"), dict)
            and f["properties"].get("mag") is not None
            and _safe_float(f["properties"]["mag"]) > 7
        ):
            return "Yes"
    return "No"


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


def _prod_expensive_high_rated(data: Any) -> str:
    count = sum(
        1
        for p in data
        if isinstance(p, dict)
        and p.get("price") is not None
        and _safe_float(p.get("price")) > 50
        and p.get("rating") is not None
        and _safe_float(p.get("rating")) > 4.5
    )
    return str(count)


def _prod_avg_rating_expensive(data: Any) -> str:
    ratings = [
        _safe_float(p["rating"])
        for p in data
        if isinstance(p, dict)
        and p.get("rating") is not None
        and _safe_float(p.get("price")) > 50
    ]
    if not ratings:
        return "0"
    return f"{sum(ratings) / len(ratings):.2f}"


def _prod_top3_expensive(data: Any) -> str:
    valid = [
        p for p in data if isinstance(p, dict) and p.get("price") is not None
    ]
    valid.sort(key=lambda p: _safe_float(p["price"]), reverse=True)
    titles = [str(p.get("title", "")) for p in valid[:3]]
    return json.dumps(titles)


def _prod_median_price(data: Any) -> str:
    prices = [
        _safe_float(p["price"])
        for p in data
        if isinstance(p, dict) and p.get("price") is not None
    ]
    if not prices:
        return "0"
    return f"{statistics.median(prices):.2f}"


def _prod_mens_category_count(data: Any) -> str:
    count = sum(
        1
        for p in data
        if isinstance(p, dict)
        and isinstance(p.get("category"), str)
        and p["category"].startswith("mens-")
    )
    return str(count)


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


def _users_pct_over40(data: Any) -> str:
    # Denominator is all user records, including those with missing
    # age, so the percentage is "of all users" not "of users with
    # known age".  This matches the question text intent.
    total = sum(1 for u in data if isinstance(u, dict))
    if total == 0:
        return "0"
    over40 = sum(
        1
        for u in data
        if isinstance(u, dict)
        and isinstance(u.get("age"), (int, float))
        and u["age"] > 40
    )
    return f"{over40 / total * 100:.1f}"


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


def _countries_not_landlocked(data: Any) -> str:
    return str(
        sum(
            1
            for c in data
            if isinstance(c, dict) and c.get("landlocked") is False
        )
    )


def _countries_europe_vs_africa_avg_pop(data: Any) -> str:
    europe_pops = [
        c["population"]
        for c in data
        if isinstance(c, dict)
        and c.get("region") == "Europe"
        and isinstance(c.get("population"), (int, float))
        and c["population"] > 0
    ]
    africa_pops = [
        c["population"]
        for c in data
        if isinstance(c, dict)
        and c.get("region") == "Africa"
        and isinstance(c.get("population"), (int, float))
        and c["population"] > 0
    ]
    if not europe_pops or not africa_pops:
        return "No"
    avg_eu = sum(europe_pops) / len(europe_pops)
    avg_af = sum(africa_pops) / len(africa_pops)
    return "Yes" if avg_eu > avg_af else "No"


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


def _laureates_avg_birth_year_physics(data: Any) -> str:
    years: list[int] = []
    for la in data:
        if not isinstance(la, dict):
            continue
        prizes = la.get("nobelPrizes")
        if not isinstance(prizes, list):
            continue
        has_physics = any(
            isinstance(p, dict)
            and isinstance(p.get("category"), dict)
            and p["category"].get("en") == "Physics"
            for p in prizes
        )
        if not has_physics:
            continue
        born = la.get("birth", {})
        if not isinstance(born, dict):
            continue
        date = born.get("date")
        if isinstance(date, str) and len(date) >= 4:
            with contextlib.suppress(ValueError):
                years.append(int(date[:4]))
    if not years:
        return "0"
    return str(round(sum(years) / len(years)))


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


def _weather_coldest_day(data: Any) -> str:
    hourly = data.get("hourly") if isinstance(data, dict) else None
    if not isinstance(hourly, dict):
        return ""
    times = hourly.get("time")
    temps = hourly.get("temperature_2m")
    if not isinstance(times, list) or not isinstance(temps, list):
        return ""
    daily_sums: dict[str, float] = {}
    daily_counts: dict[str, int] = {}
    # strict=False: gold functions must not crash on malformed data;
    # the API contract guarantees equal lengths but we stay defensive.
    for t, temp in zip(times, temps, strict=False):
        if (
            not isinstance(t, str)
            or not isinstance(temp, (int, float))
            or len(t) < 10
        ):
            continue
        day = t[:10]
        daily_sums[day] = daily_sums.get(day, 0.0) + temp
        daily_counts[day] = daily_counts.get(day, 0) + 1
    if not daily_sums:
        return ""
    return min(
        daily_sums,
        key=lambda d: daily_sums[d] / daily_counts[d],
    )


def _weather_cold_precip_hours(data: Any) -> str:
    hourly = data.get("hourly") if isinstance(data, dict) else None
    if not isinstance(hourly, dict):
        return "0"
    temps = hourly.get("temperature_2m")
    precip = hourly.get("precipitation")
    if not isinstance(temps, list) or not isinstance(precip, list):
        return "0"
    count = sum(
        1
        # strict=False: defensive; see _weather_coldest_day.
        for t, p in zip(temps, precip, strict=False)
        if isinstance(t, (int, float))
        and isinstance(p, (int, float))
        and t < 0
        and p > 0
    )
    return str(count)


# -- gap-coverage: round 2 --

# earthquakes (round 2)


def _eq_mag_gte3_depth_gt100(data: Any) -> str:
    count = sum(
        1
        for f in data
        if isinstance(f, dict)
        and isinstance(f.get("properties"), dict)
        and f["properties"].get("mag") is not None
        and _safe_float(f["properties"]["mag"]) >= 3.0
        and isinstance(f.get("geometry"), dict)
        and isinstance(f["geometry"].get("coordinates"), list)
        and len(f["geometry"]["coordinates"]) >= 3
        and _safe_float(f["geometry"]["coordinates"][2]) > 100
    )
    return str(count)


def _eq_avg_depth_mag_gte4(data: Any) -> str:
    depths = [
        _safe_float(f["geometry"]["coordinates"][2])
        for f in data
        if isinstance(f, dict)
        and isinstance(f.get("properties"), dict)
        and f["properties"].get("mag") is not None
        and _safe_float(f["properties"]["mag"]) >= 4.0
        and isinstance(f.get("geometry"), dict)
        and isinstance(f["geometry"].get("coordinates"), list)
        and len(f["geometry"]["coordinates"]) >= 3
    ]
    if not depths:
        return "0"
    return f"{sum(depths) / len(depths):.2f}"


def _eq_top3_deepest_places(data: Any) -> str:
    valid = [
        f
        for f in data
        if isinstance(f, dict)
        and isinstance(f.get("geometry"), dict)
        and isinstance(f["geometry"].get("coordinates"), list)
        and len(f["geometry"]["coordinates"]) >= 3
        and isinstance(f.get("properties"), dict)
    ]
    valid.sort(
        key=lambda f: _safe_float(f["geometry"]["coordinates"][2]),
        reverse=True,
    )
    places = [str(f["properties"].get("place", "")) for f in valid[:3]]
    return json.dumps(places)


def _eq_net_highest_avg_mag(data: Any) -> str:
    net_sums: dict[str, float] = {}
    net_counts: dict[str, int] = {}
    for f in data:
        if not isinstance(f, dict):
            continue
        props = f.get("properties")
        if not isinstance(props, dict):
            continue
        net = props.get("net")
        mag = props.get("mag")
        if not isinstance(net, str) or mag is None:
            continue
        net_sums[net] = net_sums.get(net, 0.0) + _safe_float(mag)
        net_counts[net] = net_counts.get(net, 0) + 1
    if not net_sums:
        return ""
    # Secondary sort on name for deterministic tie-breaking.
    return max(
        net_sums,
        key=lambda n: (net_sums[n] / net_counts[n], n),
    )


def _eq_california_count(data: Any) -> str:
    count = sum(
        1
        for f in data
        if isinstance(f, dict)
        and isinstance(f.get("properties"), dict)
        and isinstance(f["properties"].get("place"), str)
        and "California" in f["properties"]["place"]
    )
    return str(count)


# products (round 2)


def _prod_price_gt50_rating_gt4(data: Any) -> str:
    count = sum(
        1
        for p in data
        if isinstance(p, dict)
        and p.get("price") is not None
        and _safe_float(p["price"]) > 50
        and p.get("rating") is not None
        and _safe_float(p["rating"]) > 4.0
    )
    return str(count)


def _prod_low_discount(data: Any) -> str:
    count = sum(
        1
        for p in data
        if isinstance(p, dict)
        and p.get("discountPercentage") is not None
        and _safe_float(p["discountPercentage"]) < 1
    )
    return str(count)


def _prod_category_highest_avg_price(data: Any) -> str:
    cat_sums: dict[str, float] = {}
    cat_counts: dict[str, int] = {}
    for p in data:
        if not isinstance(p, dict):
            continue
        cat = p.get("category")
        price = p.get("price")
        if not isinstance(cat, str) or price is None:
            continue
        cat_sums[cat] = cat_sums.get(cat, 0.0) + _safe_float(price)
        cat_counts[cat] = cat_counts.get(cat, 0) + 1
    if not cat_sums:
        return ""
    # Secondary sort on name for deterministic tie-breaking.
    return max(
        cat_sums,
        key=lambda c: (cat_sums[c] / cat_counts[c], c),
    )


def _prod_vowel_titles(data: Any) -> str:
    count = sum(
        1
        for p in data
        if isinstance(p, dict)
        and isinstance(p.get("title"), str)
        and p["title"]
        and p["title"][0].upper() in "AEIOU"
    )
    return str(count)


# users (round 2)


def _users_over30_us(data: Any) -> str:
    count = sum(
        1
        for u in data
        if isinstance(u, dict)
        and isinstance(u.get("age"), (int, float))
        and u["age"] > 30
        and isinstance(u.get("address"), dict)
        and isinstance(u["address"].get("country"), str)
        and u["address"]["country"] == "United States"
    )
    return str(count)


# countries (round 2)


def _countries_avg_area_landlocked(data: Any) -> str:
    areas = [
        _safe_float(c["area"])
        for c in data
        if isinstance(c, dict)
        and c.get("landlocked") is True
        and isinstance(c.get("area"), (int, float))
    ]
    if not areas:
        return "0"
    return f"{sum(areas) / len(areas):.2f}"


def _countries_top5_populous(data: Any) -> str:
    valid = [
        c
        for c in data
        if isinstance(c, dict)
        and isinstance(c.get("population"), (int, float))
        and isinstance(c.get("name"), dict)
    ]
    valid.sort(key=lambda c: c["population"], reverse=True)
    names = [str(c["name"].get("common", "")) for c in valid[:5]]
    return json.dumps(names)


def _countries_pct_landlocked(data: Any) -> str:
    total = sum(1 for c in data if isinstance(c, dict))
    if total == 0:
        return "0"
    landlocked = sum(
        1 for c in data if isinstance(c, dict) and c.get("landlocked") is True
    )
    return f"{landlocked / total * 100:.1f}"


def _countries_region_highest_avg_pop(data: Any) -> str:
    region_sums: dict[str, float] = {}
    region_counts: dict[str, int] = {}
    for c in data:
        if not isinstance(c, dict):
            continue
        region = c.get("region")
        pop = c.get("population")
        if not isinstance(region, str) or not isinstance(pop, (int, float)):
            continue
        region_sums[region] = region_sums.get(region, 0.0) + pop
        region_counts[region] = region_counts.get(region, 0) + 1
    if not region_sums:
        return ""
    # Secondary sort on name for deterministic tie-breaking.
    return max(
        region_sums,
        key=lambda r: (region_sums[r] / region_counts[r], r),
    )


def _countries_any_pop_gt2b(data: Any) -> str:
    for c in data:
        if (
            isinstance(c, dict)
            and isinstance(c.get("population"), (int, float))
            and c["population"] > 2_000_000_000
        ):
            return "Yes"
    return "No"


# laureates (round 2)


def _laureates_pct_female(data: Any) -> str:
    total = sum(1 for la in data if isinstance(la, dict))
    if total == 0:
        return "0"
    female = sum(
        1
        for la in data
        if isinstance(la, dict) and la.get("gender") == "female"
    )
    return f"{female / total * 100:.1f}"


def _laureates_born_1940s(data: Any) -> str:
    count = 0
    for la in data:
        if not isinstance(la, dict):
            continue
        born = la.get("birth", {})
        if not isinstance(born, dict):
            continue
        date = born.get("date")
        if isinstance(date, str) and len(date) >= 4:
            with contextlib.suppress(ValueError):
                year = int(date[:4])
                if 1940 <= year <= 1949:
                    count += 1
    return str(count)


def _laureates_oldest_living_birth_year(data: Any) -> str:
    oldest_year: int | None = None
    for la in data:
        if not isinstance(la, dict):
            continue
        if "death" in la:
            continue
        born = la.get("birth", {})
        if not isinstance(born, dict):
            continue
        date = born.get("date")
        if isinstance(date, str) and len(date) >= 4:
            with contextlib.suppress(ValueError):
                year = int(date[:4])
                if oldest_year is None or year < oldest_year:
                    oldest_year = year
    return str(oldest_year) if oldest_year is not None else ""


def _laureates_multi_prize(data: Any) -> str:
    count = sum(
        1
        for la in data
        if isinstance(la, dict)
        and isinstance(la.get("nobelPrizes"), list)
        and len(la["nobelPrizes"]) > 1
    )
    return str(count)


def _laureates_physics_top_affiliation(data: Any) -> str:
    affiliations: list[str] = []
    for la in data:
        if not isinstance(la, dict):
            continue
        prizes = la.get("nobelPrizes")
        if not isinstance(prizes, list):
            continue
        for prize in prizes:
            if not isinstance(prize, dict):
                continue
            cat = prize.get("category")
            if not isinstance(cat, dict) or cat.get("en") != "Physics":
                continue
            affs = prize.get("affiliations")
            if not isinstance(affs, list):
                continue
            for aff in affs:
                if isinstance(aff, dict):
                    name = aff.get("name")
                    if isinstance(name, dict):
                        en = name.get("en")
                        if isinstance(en, str):
                            affiliations.append(en)
    if not affiliations:
        return ""
    return Counter(affiliations).most_common(1)[0][0]


# weather (round 2)


def _weather_peak_temp_hour(data: Any) -> str:
    hourly = data.get("hourly") if isinstance(data, dict) else None
    if not isinstance(hourly, dict):
        return ""
    times = hourly.get("time")
    temps = hourly.get("temperature_2m")
    if not isinstance(times, list) or not isinstance(temps, list):
        return ""
    hour_sums: dict[int, float] = {}
    hour_counts: dict[int, int] = {}
    # strict=False: defensive; see _weather_coldest_day.
    for t, temp in zip(times, temps, strict=False):
        if (
            not isinstance(t, str)
            or not isinstance(temp, (int, float))
            or len(t) < 13
        ):
            continue
        with contextlib.suppress(ValueError):
            hour = int(t[11:13])
            hour_sums[hour] = hour_sums.get(hour, 0.0) + temp
            hour_counts[hour] = hour_counts.get(hour, 0) + 1
    if not hour_sums:
        return ""
    best = max(
        hour_sums,
        key=lambda h: hour_sums[h] / hour_counts[h],
    )
    return str(best)


def _weather_avg_temp_during_precip(data: Any) -> str:
    hourly = data.get("hourly") if isinstance(data, dict) else None
    if not isinstance(hourly, dict):
        return "0"
    temps = hourly.get("temperature_2m")
    precip = hourly.get("precipitation")
    if not isinstance(temps, list) or not isinstance(precip, list):
        return "0"
    filtered_temps = [
        t
        # strict=False: defensive; see _weather_coldest_day.
        for t, p in zip(temps, precip, strict=False)
        if isinstance(t, (int, float)) and isinstance(p, (int, float)) and p > 0
    ]
    if not filtered_temps:
        return "0"
    avg = sum(filtered_temps) / len(filtered_temps)
    return f"{avg:.2f}"


def _weather_wind_temp_correlation(data: Any) -> str:
    hourly = data.get("hourly") if isinstance(data, dict) else None
    if not isinstance(hourly, dict):
        return ""
    winds = hourly.get("wind_speed_10m")
    temps = hourly.get("temperature_2m")
    if not isinstance(winds, list) or not isinstance(temps, list):
        return ""
    pairs = [
        (w, t)
        # strict=False: defensive; see _weather_coldest_day.
        for w, t in zip(winds, temps, strict=False)
        if isinstance(w, (int, float)) and isinstance(t, (int, float))
    ]
    if len(pairs) < 2:
        return ""
    ws = [p[0] for p in pairs]
    ts = [p[1] for p in pairs]
    n = len(pairs)
    mean_w = sum(ws) / n
    mean_t = sum(ts) / n
    # Covariance sign equals correlation sign; normalizing by
    # std-devs is unnecessary when we only need the direction.
    cov = sum((w - mean_w) * (t - mean_t) for w, t in pairs)
    return "positive" if cov > 0 else "negative"


# -- github_repos --


def _gh_stars_gt100k(data: Any) -> str:
    count = sum(
        1
        for r in data
        if isinstance(r, dict)
        and isinstance(r.get("stargazers_count"), (int, float))
        and r["stargazers_count"] > 100_000
    )
    return str(count)


def _gh_avg_forks(data: Any) -> str:
    forks = [
        _safe_float(r["forks_count"])
        for r in data
        if isinstance(r, dict) and r.get("forks_count") is not None
    ]
    if not forks:
        return "0"
    return f"{sum(forks) / len(forks):.2f}"


def _gh_most_starred_name(data: Any) -> str:
    best = max(
        (
            r
            for r in data
            if isinstance(r, dict) and r.get("stargazers_count") is not None
        ),
        key=lambda r: _safe_float(r["stargazers_count"]),
        default=None,
    )
    if best is None:
        return ""
    return str(best.get("name", ""))


def _gh_top_language(data: Any) -> str:
    langs: list[str] = [
        r["language"]
        for r in data
        if isinstance(r, dict) and isinstance(r.get("language"), str)
    ]
    if not langs:
        return ""
    return Counter(langs).most_common(1)[0][0]


def _gh_owner_most_starred(data: Any) -> str:
    best = max(
        (
            r
            for r in data
            if isinstance(r, dict) and r.get("stargazers_count") is not None
        ),
        key=lambda r: _safe_float(r["stargazers_count"]),
        default=None,
    )
    if best is None:
        return ""
    owner = best.get("owner")
    if isinstance(owner, dict):
        return str(owner.get("login", ""))
    return ""


def _gh_mit_license_count(data: Any) -> str:
    count = sum(
        1
        for r in data
        if isinstance(r, dict)
        and isinstance(r.get("license"), dict)
        and r["license"].get("spdx_id") == "MIT"
    )
    return str(count)


# -- pokemon --


def _poke_total(data: Any) -> str:
    return str(len(data))


def _poke_avg_hp(data: Any) -> str:
    hps = [
        _safe_float(p["base"]["HP"])
        for p in data
        if isinstance(p, dict)
        and isinstance(p.get("base"), dict)
        and p["base"].get("HP") is not None
    ]
    if not hps:
        return "0"
    return f"{sum(hps) / len(hps):.2f}"


def _poke_highest_attack(data: Any) -> str:
    best = max(
        (
            p
            for p in data
            if isinstance(p, dict)
            and isinstance(p.get("base"), dict)
            and p["base"].get("Attack") is not None
        ),
        key=lambda p: _safe_float(p["base"]["Attack"]),
        default=None,
    )
    if best is None:
        return ""
    name = best.get("name")
    if isinstance(name, dict):
        return str(name.get("english", ""))
    return ""


def _poke_fire_count(data: Any) -> str:
    count = sum(
        1
        for p in data
        if isinstance(p, dict)
        and isinstance(p.get("type"), list)
        and "Fire" in p["type"]
    )
    return str(count)


def _poke_top_type(data: Any) -> str:
    types: list[str] = []
    for p in data:
        if not isinstance(p, dict):
            continue
        t = p.get("type")
        if isinstance(t, list):
            types.extend(typ for typ in t if isinstance(typ, str))
    if not types:
        return ""
    return Counter(types).most_common(1)[0][0]


def _poke_avg_speed_water(data: Any) -> str:
    speeds = [
        _safe_float(p["base"]["Speed"])
        for p in data
        if isinstance(p, dict)
        and isinstance(p.get("type"), list)
        and "Water" in p["type"]
        and isinstance(p.get("base"), dict)
        and p["base"].get("Speed") is not None
    ]
    if not speeds:
        return "0"
    return f"{sum(speeds) / len(speeds):.2f}"


# -- openlibrary --


def _ol_total_works(data: Any) -> str:
    return str(len(data))


def _ol_avg_editions(data: Any) -> str:
    editions = [
        _safe_float(w["edition_count"])
        for w in data
        if isinstance(w, dict) and w.get("edition_count") is not None
    ]
    if not editions:
        return "0"
    return f"{sum(editions) / len(editions):.2f}"


def _ol_oldest_title(data: Any) -> str:
    best = min(
        (
            w
            for w in data
            if isinstance(w, dict)
            and isinstance(w.get("first_publish_year"), (int, float))
        ),
        key=lambda w: w["first_publish_year"],
        default=None,
    )
    if best is None:
        return ""
    return str(best.get("title", ""))


def _ol_multi_author_count(data: Any) -> str:
    count = sum(
        1
        for w in data
        if isinstance(w, dict)
        and isinstance(w.get("authors"), list)
        and len(w["authors"]) > 1
    )
    return str(count)


def _ol_top_author(data: Any) -> str:
    names: list[str] = []
    for w in data:
        if not isinstance(w, dict):
            continue
        authors = w.get("authors")
        if not isinstance(authors, list):
            continue
        for a in authors:
            if isinstance(a, dict):
                name = a.get("name")
                if isinstance(name, str):
                    names.append(name)
    if not names:
        return ""
    return Counter(names).most_common(1)[0][0]


def _ol_has_cover_count(data: Any) -> str:
    count = sum(
        1 for w in data if isinstance(w, dict) and w.get("cover_id") is not None
    )
    return str(count)


# -- airports --


def _air_count_us(data: Any) -> str:
    count = sum(
        1 for a in data if isinstance(a, dict) and a.get("country") == "US"
    )
    return str(count)


def _air_avg_elevation(data: Any) -> str:
    elevations = [
        _safe_float(a["elevation"])
        for a in data
        if isinstance(a, dict) and a.get("elevation") is not None
    ]
    if not elevations:
        return "0"
    return f"{sum(elevations) / len(elevations):.2f}"


def _air_lookup_lax(data: Any) -> str:
    for a in data:
        if isinstance(a, dict) and a.get("iata") == "LAX":
            return str(a.get("name", ""))
    return ""


def _air_highest_elevation(data: Any) -> str:
    best = max(
        (
            a
            for a in data
            if isinstance(a, dict) and a.get("elevation") is not None
        ),
        key=lambda a: _safe_float(a["elevation"]),
        default=None,
    )
    if best is None:
        return ""
    return str(best.get("name", ""))


def _air_top_country(data: Any) -> str:
    countries: list[str] = [
        a["country"]
        for a in data
        if isinstance(a, dict) and isinstance(a.get("country"), str)
    ]
    if not countries:
        return ""
    return Counter(countries).most_common(1)[0][0]


def _air_northernmost(data: Any) -> str:
    best = max(
        (a for a in data if isinstance(a, dict) and a.get("lat") is not None),
        key=lambda a: _safe_float(a["lat"]),
        default=None,
    )
    if best is None:
        return ""
    return str(best.get("name", ""))


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
        difficulty=2,
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
        difficulty=2,
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
        difficulty=2,
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
        difficulty=2,
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
        difficulty=2,
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
        difficulty=2,
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
        difficulty=2,
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
        difficulty=2,
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
        difficulty=2,
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
        difficulty=2,
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
        difficulty=2,
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
        difficulty=2,
    ),
    Question(
        dataset_name="countries",
        question_id="countries_top_subregion",
        question_text=("Which subregion has the most countries?"),
        question_type="cross_field",
        gold_answer_fn=_countries_most_countries_subregion,
        answer_type="string",
        difficulty=2,
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
        difficulty=2,
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
        difficulty=2,
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
        difficulty=2,
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
        difficulty=2,
    ),
    # -- new gap-coverage questions (13) --
    # Gap #1: multi-condition filters
    Question(
        dataset_name="products",
        question_id="prod_expensive_high_rated",
        question_text=(
            "How many products have a price greater than 50 "
            "and a rating greater than 4.5?"
        ),
        question_type="multi_condition",
        gold_answer_fn=_prod_expensive_high_rated,
        answer_type="number",
        difficulty=3,
    ),
    # Gap #2: conditional aggregation
    Question(
        dataset_name="products",
        question_id="prod_avg_rating_expensive",
        question_text=(
            "What is the average rating of products with a "
            "price greater than 50? Give two decimal places."
        ),
        question_type="conditional_aggregation",
        gold_answer_fn=_prod_avg_rating_expensive,
        answer_type="number",
        tolerance=0.01,
        difficulty=3,
    ),
    # Gap #3: top-N / ranking (also exercises list answer type)
    Question(
        dataset_name="products",
        question_id="prod_top3_expensive",
        question_text=(
            "What are the titles of the 3 most expensive "
            "products? Return a JSON array in any order."
        ),
        question_type="top_n",
        gold_answer_fn=_prod_top3_expensive,
        answer_type="list",
        difficulty=3,
    ),
    # Gap #4: percentage / ratio
    Question(
        dataset_name="users",
        question_id="users_pct_over40",
        question_text=(
            "What percentage of users are older than 40? "
            "Give one decimal place."
        ),
        question_type="percentage",
        gold_answer_fn=_users_pct_over40,
        answer_type="number",
        tolerance=0.1,
        difficulty=2,
    ),
    # Gap #5: median / percentile
    Question(
        dataset_name="products",
        question_id="prod_median_price",
        question_text=(
            "What is the median price across all products? "
            "Give two decimal places."
        ),
        question_type="median",
        gold_answer_fn=_prod_median_price,
        answer_type="number",
        tolerance=0.01,
        difficulty=3,
    ),
    # Gap #6: negation
    Question(
        dataset_name="countries",
        question_id="countries_not_landlocked",
        question_text="How many countries are not landlocked?",
        question_type="negation",
        gold_answer_fn=_countries_not_landlocked,
        answer_type="number",
        difficulty=2,
    ),
    # Gap #7: group-by with aggregation
    Question(
        dataset_name="earthquakes",
        question_id="eq_avg_mag_us_net",
        question_text=(
            "What is the average magnitude of earthquakes "
            "reported by the 'us' network? Give two decimal "
            "places."
        ),
        question_type="group_aggregation",
        gold_answer_fn=_eq_avg_mag_us_net,
        answer_type="number",
        tolerance=0.01,
        difficulty=3,
    ),
    # Gap #8: date/time parsing + Gap #7: group-by
    Question(
        dataset_name="weather",
        question_id="weather_coldest_day",
        question_text=(
            "On which date was the average hourly temperature "
            "the lowest? Give the date in YYYY-MM-DD format."
        ),
        question_type="datetime",
        gold_answer_fn=_weather_coldest_day,
        answer_type="string",
        difficulty=3,
    ),
    # Gap #9: string operations
    Question(
        dataset_name="products",
        question_id="prod_mens_category_count",
        question_text=(
            "How many products belong to a category that starts with 'mens-'?"
        ),
        question_type="string_op",
        gold_answer_fn=_prod_mens_category_count,
        answer_type="number",
        difficulty=2,
    ),
    # Gap #10: comparison between groups
    Question(
        dataset_name="countries",
        question_id="countries_europe_vs_africa_avg_pop",
        question_text=(
            "Is the average population of European countries "
            "higher than that of African countries in this "
            "dataset? Answer 'Yes' or 'No'."
        ),
        question_type="comparison",
        gold_answer_fn=_countries_europe_vs_africa_avg_pop,
        answer_type="boolean",
        difficulty=2,
    ),
    # Gap #11: existence / boolean
    Question(
        dataset_name="earthquakes",
        question_id="eq_any_mag_gt7",
        question_text=(
            "Is there any earthquake in this dataset with a "
            "magnitude greater than 7? Answer 'Yes' or 'No'."
        ),
        question_type="existence",
        gold_answer_fn=_eq_any_mag_gt7,
        answer_type="boolean",
        difficulty=2,
    ),
    # Gap #2 + #8: conditional aggregation + date/time parsing
    Question(
        dataset_name="laureates",
        question_id="laureates_avg_birth_year_physics",
        question_text=(
            "What is the average birth year of laureates who "
            "won a Physics prize? Round to the nearest whole "
            "number."
        ),
        question_type="conditional_aggregation",
        gold_answer_fn=_laureates_avg_birth_year_physics,
        answer_type="number",
        tolerance=1.0,
        difficulty=3,
    ),
    # Gap #1: multi-condition (columnar variant)
    Question(
        dataset_name="weather",
        question_id="weather_cold_precip_hours",
        question_text=(
            "How many hours had a temperature below 0 and "
            "precipitation greater than 0 at the same time?"
        ),
        question_type="multi_condition",
        gold_answer_fn=_weather_cold_precip_hours,
        answer_type="number",
        difficulty=3,
    ),
    # -- gap-coverage: round 2 (22) --
    # multi-condition filters
    Question(
        dataset_name="earthquakes",
        question_id="eq_mag_gte3_depth_gt100",
        question_text=(
            "How many earthquakes have magnitude >= 3.0 "
            "AND depth greater than 100 km?"
        ),
        question_type="multi_condition",
        gold_answer_fn=_eq_mag_gte3_depth_gt100,
        answer_type="number",
        difficulty=3,
    ),
    # Differs from prod_expensive_high_rated: rating threshold
    # is 4.0 here vs 4.5 there — tests boundary sensitivity.
    Question(
        dataset_name="products",
        question_id="prod_price_gt50_rating_gt4",
        question_text=(
            "How many products have a price greater than "
            "50 and a rating greater than 4.0?"
        ),
        question_type="multi_condition",
        gold_answer_fn=_prod_price_gt50_rating_gt4,
        answer_type="number",
        difficulty=3,
    ),
    Question(
        dataset_name="users",
        question_id="users_over30_us",
        question_text=(
            "How many users are over 30 years old and "
            "live in the United States?"
        ),
        question_type="multi_condition",
        gold_answer_fn=_users_over30_us,
        answer_type="number",
        difficulty=3,
    ),
    # conditional aggregation
    Question(
        dataset_name="earthquakes",
        question_id="eq_avg_depth_mag_gte4",
        question_text=(
            "What is the average depth (in km) of "
            "earthquakes with magnitude >= 4.0? "
            "Give two decimal places."
        ),
        question_type="conditional_aggregation",
        gold_answer_fn=_eq_avg_depth_mag_gte4,
        answer_type="number",
        tolerance=0.01,
        difficulty=3,
    ),
    Question(
        dataset_name="countries",
        question_id="countries_avg_area_landlocked",
        question_text=(
            "What is the average area of landlocked "
            "countries? Give two decimal places."
        ),
        question_type="conditional_aggregation",
        gold_answer_fn=_countries_avg_area_landlocked,
        answer_type="number",
        tolerance=0.01,
        difficulty=3,
    ),
    # top-N / list
    Question(
        dataset_name="countries",
        question_id="countries_top5_populous",
        question_text=(
            "What are the names of the 5 most populous "
            "countries? Return a JSON array."
        ),
        question_type="top_n",
        gold_answer_fn=_countries_top5_populous,
        answer_type="list",
        difficulty=3,
    ),
    Question(
        dataset_name="earthquakes",
        question_id="eq_top3_deepest_places",
        question_text=(
            "What are the place names of the 3 deepest "
            "earthquakes? Return a JSON array."
        ),
        question_type="top_n",
        gold_answer_fn=_eq_top3_deepest_places,
        answer_type="list",
        difficulty=3,
    ),
    # percentage / ratio
    Question(
        dataset_name="countries",
        question_id="countries_pct_landlocked",
        question_text=(
            "What percentage of countries are "
            "landlocked? Give one decimal place."
        ),
        question_type="percentage",
        gold_answer_fn=_countries_pct_landlocked,
        answer_type="number",
        tolerance=0.1,
        difficulty=2,
    ),
    Question(
        dataset_name="laureates",
        question_id="laureates_pct_female",
        question_text=(
            "What percentage of laureates are female? Give one decimal place."
        ),
        question_type="percentage",
        gold_answer_fn=_laureates_pct_female,
        answer_type="number",
        tolerance=0.1,
        difficulty=2,
    ),
    # filter (threshold)
    Question(
        dataset_name="products",
        question_id="prod_low_discount",
        question_text=(
            "How many products have a discount percentage less than 1?"
        ),
        question_type="filter",
        gold_answer_fn=_prod_low_discount,
        answer_type="number",
        difficulty=2,
    ),
    # group-by + aggregation
    Question(
        dataset_name="earthquakes",
        question_id="eq_net_highest_avg_mag",
        question_text=(
            "Which reporting network has the highest average magnitude?"
        ),
        question_type="group_aggregation",
        gold_answer_fn=_eq_net_highest_avg_mag,
        answer_type="string",
        difficulty=3,
    ),
    Question(
        dataset_name="products",
        question_id="prod_category_highest_avg_price",
        question_text=("Which product category has the highest average price?"),
        question_type="group_aggregation",
        gold_answer_fn=_prod_category_highest_avg_price,
        answer_type="string",
        difficulty=3,
    ),
    Question(
        dataset_name="countries",
        question_id="countries_region_highest_avg_pop",
        question_text=(
            "Which region has the highest average population per country?"
        ),
        question_type="group_aggregation",
        gold_answer_fn=_countries_region_highest_avg_pop,
        answer_type="string",
        difficulty=3,
    ),
    # date/time parsing
    Question(
        dataset_name="laureates",
        question_id="laureates_born_1940s",
        question_text=(
            "How many laureates were born in the 1940s (1940-1949)?"
        ),
        question_type="datetime",
        gold_answer_fn=_laureates_born_1940s,
        answer_type="number",
        difficulty=2,
    ),
    Question(
        dataset_name="laureates",
        question_id="laureates_oldest_living_birth_year",
        question_text=(
            "What is the birth year of the oldest "
            "living laureate (one without a death "
            "date)?"
        ),
        question_type="datetime",
        gold_answer_fn=_laureates_oldest_living_birth_year,
        answer_type="number",
        difficulty=3,
    ),
    Question(
        dataset_name="weather",
        question_id="weather_peak_temp_hour",
        question_text=(
            "What hour of the day (0-23) has the highest average temperature?"
        ),
        question_type="datetime",
        gold_answer_fn=_weather_peak_temp_hour,
        answer_type="number",
        difficulty=3,
    ),
    # string operations
    Question(
        dataset_name="earthquakes",
        question_id="eq_california_count",
        question_text=(
            "How many earthquakes have 'California' in their place name?"
        ),
        question_type="string_op",
        gold_answer_fn=_eq_california_count,
        answer_type="number",
        difficulty=2,
    ),
    Question(
        dataset_name="products",
        question_id="prod_vowel_titles",
        question_text=(
            "How many products have a title that "
            "starts with a vowel (A, E, I, O, U)?"
        ),
        question_type="string_op",
        gold_answer_fn=_prod_vowel_titles,
        answer_type="number",
        difficulty=2,
    ),
    # cross-root / correlation
    Question(
        dataset_name="weather",
        question_id="weather_avg_temp_during_precip",
        question_text=(
            "What is the average temperature during "
            "hours with precipitation greater than 0? "
            "Give two decimal places."
        ),
        question_type="cross_root",
        gold_answer_fn=_weather_avg_temp_during_precip,
        answer_type="number",
        tolerance=0.01,
        difficulty=3,
    ),
    Question(
        dataset_name="weather",
        question_id="weather_wind_temp_correlation",
        question_text=(
            "Is the correlation between wind speed "
            "and temperature positive or negative? "
            "Answer 'positive' or 'negative'."
        ),
        question_type="cross_root",
        gold_answer_fn=_weather_wind_temp_correlation,
        answer_type="string",
        difficulty=3,
    ),
    # existence / boolean
    Question(
        dataset_name="countries",
        question_id="countries_any_pop_gt2b",
        question_text=(
            "Is there any country with a population "
            "greater than 2 billion? Answer 'Yes' "
            "or 'No'."
        ),
        question_type="existence",
        gold_answer_fn=_countries_any_pop_gt2b,
        answer_type="boolean",
        difficulty=2,
    ),
    # deep nesting
    Question(
        dataset_name="laureates",
        question_id="laureates_multi_prize",
        question_text=(
            "How many laureates have won more than one Nobel Prize?"
        ),
        question_type="deep_nesting",
        gold_answer_fn=_laureates_multi_prize,
        answer_type="number",
        difficulty=3,
    ),
    Question(
        dataset_name="laureates",
        question_id="laureates_physics_top_affiliation",
        question_text=(
            "What is the most common affiliation name among Physics laureates?"
        ),
        question_type="deep_nesting",
        gold_answer_fn=_laureates_physics_top_affiliation,
        answer_type="string",
        difficulty=3,
    ),
    # -- new dataset questions (24) --
    # github_repos (6)
    Question(
        dataset_name="github_repos",
        question_id="gh_stars_gt100k",
        question_text=("How many repos have more than 100,000 stars?"),
        question_type="count",
        gold_answer_fn=_gh_stars_gt100k,
        answer_type="number",
    ),
    Question(
        dataset_name="github_repos",
        question_id="gh_avg_forks",
        question_text=(
            "Average number of forks across all repos? Give two decimal places."
        ),
        question_type="aggregation",
        gold_answer_fn=_gh_avg_forks,
        answer_type="number",
        tolerance=0.01,
        difficulty=2,
    ),
    Question(
        dataset_name="github_repos",
        question_id="gh_most_starred_name",
        question_text=("What is the name of the repo with the most stars?"),
        question_type="lookup",
        gold_answer_fn=_gh_most_starred_name,
        answer_type="string",
    ),
    Question(
        dataset_name="github_repos",
        question_id="gh_top_language",
        question_text=("Which programming language has the most repos?"),
        question_type="cross_field",
        gold_answer_fn=_gh_top_language,
        answer_type="string",
        difficulty=2,
    ),
    Question(
        dataset_name="github_repos",
        question_id="gh_owner_most_starred",
        question_text=("What is the owner login of the most starred repo?"),
        question_type="lookup",
        gold_answer_fn=_gh_owner_most_starred,
        answer_type="string",
    ),
    Question(
        dataset_name="github_repos",
        question_id="gh_mit_license_count",
        question_text=("How many repos use the MIT License?"),
        question_type="filter",
        gold_answer_fn=_gh_mit_license_count,
        answer_type="number",
    ),
    # pokemon (6)
    Question(
        dataset_name="pokemon",
        question_id="poke_total",
        question_text=("How many Pokemon are in this dataset?"),
        question_type="count",
        gold_answer_fn=_poke_total,
        answer_type="number",
    ),
    Question(
        dataset_name="pokemon",
        question_id="poke_avg_hp",
        question_text=(
            "What is the average base HP across all Pokemon? "
            "Give two decimal places."
        ),
        question_type="aggregation",
        gold_answer_fn=_poke_avg_hp,
        answer_type="number",
        tolerance=0.01,
        difficulty=2,
    ),
    Question(
        dataset_name="pokemon",
        question_id="poke_highest_attack",
        question_text=(
            "What is the English name of the Pokemon with "
            "the highest Attack stat?"
        ),
        question_type="lookup",
        gold_answer_fn=_poke_highest_attack,
        answer_type="string",
    ),
    Question(
        dataset_name="pokemon",
        question_id="poke_fire_count",
        question_text=("How many Pokemon have Fire as one of their types?"),
        question_type="filter",
        gold_answer_fn=_poke_fire_count,
        answer_type="number",
    ),
    Question(
        dataset_name="pokemon",
        question_id="poke_top_type",
        question_text=(
            "What is the most common type? Count each type per Pokemon."
        ),
        question_type="cross_field",
        gold_answer_fn=_poke_top_type,
        answer_type="string",
        difficulty=2,
    ),
    Question(
        dataset_name="pokemon",
        question_id="poke_avg_speed_water",
        question_text=(
            "What is the average Speed stat of Water-type "
            "Pokemon? Give two decimal places."
        ),
        question_type="conditional_aggregation",
        gold_answer_fn=_poke_avg_speed_water,
        answer_type="number",
        tolerance=0.01,
        difficulty=3,
    ),
    # openlibrary (6)
    Question(
        dataset_name="openlibrary",
        question_id="ol_total_works",
        question_text="How many works are in this dataset?",
        question_type="count",
        gold_answer_fn=_ol_total_works,
        answer_type="number",
    ),
    Question(
        dataset_name="openlibrary",
        question_id="ol_avg_editions",
        question_text=(
            "What is the average edition count across all "
            "works? Give two decimal places."
        ),
        question_type="aggregation",
        gold_answer_fn=_ol_avg_editions,
        answer_type="number",
        tolerance=0.01,
        difficulty=2,
    ),
    Question(
        dataset_name="openlibrary",
        question_id="ol_oldest_title",
        question_text=(
            "What is the title of the work with the earliest "
            "first publish year?"
        ),
        question_type="lookup",
        gold_answer_fn=_ol_oldest_title,
        answer_type="string",
    ),
    Question(
        dataset_name="openlibrary",
        question_id="ol_multi_author_count",
        question_text=("How many works have more than 1 author?"),
        question_type="filter",
        gold_answer_fn=_ol_multi_author_count,
        answer_type="number",
    ),
    Question(
        dataset_name="openlibrary",
        question_id="ol_top_author",
        question_text=("Which author name appears in the most works?"),
        question_type="cross_field",
        gold_answer_fn=_ol_top_author,
        answer_type="string",
        difficulty=2,
    ),
    Question(
        dataset_name="openlibrary",
        question_id="ol_has_cover_count",
        question_text=("How many works have a non-null cover_id?"),
        question_type="existence",
        gold_answer_fn=_ol_has_cover_count,
        answer_type="number",
        difficulty=2,
    ),
    # airports (6)
    Question(
        dataset_name="airports",
        question_id="air_count_us",
        question_text=("How many airports are in the US (country='US')?"),
        question_type="count",
        gold_answer_fn=_air_count_us,
        answer_type="number",
    ),
    Question(
        dataset_name="airports",
        question_id="air_avg_elevation",
        question_text=(
            "What is the average elevation of all airports? "
            "Give two decimal places."
        ),
        question_type="aggregation",
        gold_answer_fn=_air_avg_elevation,
        answer_type="number",
        tolerance=0.01,
        difficulty=2,
    ),
    Question(
        dataset_name="airports",
        question_id="air_lookup_lax",
        question_text=("What is the name of the airport with IATA code 'LAX'?"),
        question_type="lookup",
        gold_answer_fn=_air_lookup_lax,
        answer_type="string",
    ),
    Question(
        dataset_name="airports",
        question_id="air_highest_elevation",
        question_text=(
            "What is the name of the airport with the highest elevation?"
        ),
        question_type="lookup",
        gold_answer_fn=_air_highest_elevation,
        answer_type="string",
    ),
    Question(
        dataset_name="airports",
        question_id="air_top_country",
        question_text=("Which country has the most airports?"),
        question_type="cross_field",
        gold_answer_fn=_air_top_country,
        answer_type="string",
        difficulty=2,
    ),
    Question(
        dataset_name="airports",
        question_id="air_northernmost",
        question_text=(
            "What is the name of the northernmost airport (highest latitude)?"
        ),
        question_type="lookup",
        gold_answer_fn=_air_northernmost,
        answer_type="string",
        difficulty=2,
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
        f":{q.difficulty}:{q.gold_answer_fn.__name__}"
        for q in QUESTIONS
    ]
    digest = hashlib.sha256("\n".join(parts).encode()).hexdigest()
    return digest[:12]
