import os
import re
from functools import lru_cache

from db import run_query_with_columns
from insights import (
    detect_anomalies,
    detect_type,
    format_percent,
    format_value,
    generate_ai_insights,
    generate_basic_insights,
)
from llm import ask_llm
from memory import Memory
from planner import create_plan, parse_plan, should_plan
from sql_generator import fix_sql, generate_sql
from visualize import plot_results

RELATIONSHIP_KEYWORDS = (
    " related ",
    " relationship ",
    " relationships ",
    " connect ",
    " connected ",
    " link ",
    " linked ",
    " joined ",
    " join ",
)

MONTH_TO_NUMBER = {
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12,
}

NUMBER_TO_MONTH = {value: key.title() for key, value in MONTH_TO_NUMBER.items()}

FOLLOWUP_PREFIXES = (
    "now",
    "only",
    "and",
    "also",
    "what about",
    "top",
)


def charts_enabled():
    return os.getenv("DATASAGE_SHOW_CHARTS", "1").lower() not in {"0", "false", "no"}


@lru_cache(maxsize=1)
def build_schema_search():
    from schema import get_schema, schema_relationship_hints, schema_to_text
    from vector_store import VectorStore

    schema = get_schema()
    texts = tuple(schema_to_text(schema))
    relationship_hints = tuple(schema_relationship_hints(schema))

    vector_store = VectorStore()
    vector_store.build_index(texts)
    return vector_store, texts, schema, relationship_hints


def run_schema_search(question):
    vector_store, _, schema, _ = build_schema_search()
    return expand_schema_matches(vector_store.search(question), schema)


def singularize(word):
    if word.endswith("ies") and len(word) > 3:
        return word[:-3] + "y"
    if word.endswith("s") and not word.endswith("ss") and len(word) > 1:
        return word[:-1]
    return word


def pluralize(word):
    if word.endswith("y") and len(word) > 1 and word[-2] not in "aeiou":
        return word[:-1] + "ies"
    if word.endswith("s"):
        return word
    return word + "s"


def normalize_text(text):
    return re.sub(r"[^a-z0-9_ ]+", " ", text.lower())


def is_relationship_question(question):
    lowered = f" {normalize_text(question)} "
    return any(keyword in lowered for keyword in RELATIONSHIP_KEYWORDS)


@lru_cache(maxsize=1)
def load_known_countries():
    _columns, rows = run_query_with_columns(
        "SELECT DISTINCT country FROM customers WHERE country IS NOT NULL ORDER BY country;"
    )
    return tuple(row[0] for row in rows)


@lru_cache(maxsize=1)
def load_known_products():
    _columns, rows = run_query_with_columns(
        "SELECT DISTINCT product_name FROM orders WHERE product_name IS NOT NULL ORDER BY product_name;"
    )
    return tuple(row[0] for row in rows)


def month_name_from_number(month_number):
    return NUMBER_TO_MONTH.get(month_number, str(month_number))


def extract_month(question):
    lowered = normalize_text(question)
    for month_name, month_number in MONTH_TO_NUMBER.items():
        if re.search(rf"\b{month_name}\b", lowered):
            return month_number
    return None


def extract_country(question):
    lowered = normalize_text(question)
    for country in load_known_countries():
        if re.search(rf"\b{re.escape(country.lower())}\b", lowered):
            return country
    return None


def extract_product(question):
    lowered = normalize_text(question)
    for product in load_known_products():
        if re.search(rf"\b{re.escape(product.lower())}\b", lowered):
            return product
    return None


def extract_limit(question, default=None):
    match = re.search(r"\btop\s+(\d+)\b", question.lower())
    if match:
        return int(match.group(1))
    return default


def question_has_any(question, patterns):
    lowered = normalize_text(question)
    return any(re.search(pattern, lowered) for pattern in patterns)


def question_mentions_orders(question):
    return question_has_any(question, (r"\border\b", r"\borders\b", r"\bsales records\b"))


def question_mentions_revenue(question):
    return question_has_any(question, (r"\brevenue\b", r"\bsales\b"))


def looks_like_followup_question(question, memory):
    if memory.get_last_entry() is None:
        return False

    lowered = question.strip().lower()
    if any(lowered.startswith(prefix) for prefix in FOLLOWUP_PREFIXES):
        return True

    if len(lowered.split()) > 5:
        return False

    return bool(
        extract_country(question)
        or extract_month(question)
        or re.search(r"\btop\s+\d+\b", lowered)
        or "only" in lowered
    )


def extract_country_from_sql(sql_query):
    match = re.search(r"customers\.country\s*=\s*'([^']+)'", sql_query, flags=re.IGNORECASE)
    return match.group(1) if match else None


def extract_month_from_sql(sql_query):
    match = re.search(
        r"EXTRACT\(MONTH FROM (?:orders\.)?order_date\)\s*=\s*(\d+)",
        sql_query,
        flags=re.IGNORECASE,
    )
    return int(match.group(1)) if match else None


def extract_limit_from_sql(sql_query):
    match = re.search(r"\bLIMIT\s+(\d+)\b", sql_query, flags=re.IGNORECASE)
    return int(match.group(1)) if match else None


def classify_sql_shape(sql_query):
    lowered = " ".join(sql_query.lower().split())

    if re.search(r"group by\s+(?:orders\.)?product_name\b", lowered):
        return "product_ranking"

    if re.search(r"group by\s+(?:customers\.)?country\b", lowered):
        return "country_breakdown"

    if re.search(
        r"group by\s+(?:orders\.)?customer_id,\s*(?:customers\.)?name\b",
        lowered,
    ) or re.search(
        r"group by\s+(?:customers\.)?customer_id,\s*(?:customers\.)?name\b",
        lowered,
    ) or re.search(r"group by\s+(?:customers\.)?name\b", lowered):
        return "customer_ranking"

    if "from orders" in lowered and "group by" not in lowered and "count(" not in lowered:
        return "order_rows"

    return None


def sql_total_revenue():
    return "SELECT SUM(revenue) FROM orders;"


def sql_order_count():
    return "SELECT COUNT(*) FROM orders;"


def sql_all_orders(country=None, month=None):
    filters = []
    joins = ""

    if country is not None:
        joins = " JOIN customers ON orders.customer_id = customers.customer_id"
        filters.append(f"customers.country = '{country}'")

    if month is not None:
        filters.append(f"EXTRACT(MONTH FROM orders.order_date) = {month}")

    where_clause = f" WHERE {' AND '.join(filters)}" if filters else ""
    return (
        "SELECT orders.order_id, orders.customer_id, orders.product_name, orders.revenue, orders.order_date "
        f"FROM orders{joins}{where_clause} ORDER BY orders.order_id;"
    )


def sql_revenue_by_country(limit=None, month=None):
    filters = []
    if month is not None:
        filters.append(f"EXTRACT(MONTH FROM orders.order_date) = {month}")

    where_clause = f" WHERE {' AND '.join(filters)}" if filters else ""
    limit_clause = f" LIMIT {limit}" if limit is not None else ""
    return (
        "SELECT customers.country, SUM(orders.revenue) AS total_revenue "
        "FROM orders JOIN customers ON orders.customer_id = customers.customer_id"
        f"{where_clause} "
        "GROUP BY customers.country "
        "ORDER BY total_revenue DESC"
        f"{limit_clause};"
    )


def sql_top_products(limit=None, country=None, month=None):
    filters = []
    joins = ""

    if country is not None:
        joins = " JOIN customers ON orders.customer_id = customers.customer_id"
        filters.append(f"customers.country = '{country}'")

    if month is not None:
        filters.append(f"EXTRACT(MONTH FROM orders.order_date) = {month}")

    where_clause = f" WHERE {' AND '.join(filters)}" if filters else ""
    limit_clause = f" LIMIT {limit}" if limit is not None else ""
    return (
        "SELECT orders.product_name, SUM(orders.revenue) AS total_revenue "
        f"FROM orders{joins}{where_clause} "
        "GROUP BY orders.product_name "
        "ORDER BY total_revenue DESC"
        f"{limit_clause};"
    )


def sql_revenue_for_country(country):
    return (
        "SELECT SUM(orders.revenue) "
        "FROM orders JOIN customers ON orders.customer_id = customers.customer_id "
        f"WHERE customers.country = '{country}';"
    )


def sql_revenue_by_month():
    return (
        "SELECT EXTRACT(MONTH FROM order_date) AS month, SUM(revenue) AS total_revenue "
        "FROM orders GROUP BY month ORDER BY month;"
    )


def sql_revenue_for_product_month(product, month):
    return (
        "SELECT SUM(revenue) "
        "FROM orders "
        f"WHERE product_name = '{product}' AND EXTRACT(MONTH FROM order_date) = {month};"
    )


def sql_top_customers(limit=None, country=None, month=None):
    filters = []

    if country is not None:
        filters.append(f"customers.country = '{country}'")

    if month is not None:
        filters.append(f"EXTRACT(MONTH FROM orders.order_date) = {month}")

    where_clause = f" WHERE {' AND '.join(filters)}" if filters else ""
    limit_clause = f" LIMIT {limit}" if limit is not None else ""
    return (
        "SELECT customers.name, SUM(orders.revenue) AS total_revenue "
        "FROM orders JOIN customers ON orders.customer_id = customers.customer_id"
        f"{where_clause} "
        "GROUP BY customers.name "
        "ORDER BY total_revenue DESC"
        f"{limit_clause};"
    )


def build_top_country_subquery(month=None):
    month_clause = (
        f"WHERE EXTRACT(MONTH FROM orders.order_date) = {month}" if month is not None else ""
    )
    return (
        "SELECT customers.country "
        "FROM orders JOIN customers ON orders.customer_id = customers.customer_id "
        f"{month_clause} "
        "GROUP BY customers.country "
        "ORDER BY SUM(orders.revenue) DESC "
        "LIMIT 1"
    )


def sql_top_products_for_top_country(limit=2, month=None):
    outer_filters = [f"customers.country = ({build_top_country_subquery(month)})"]
    if month is not None:
        outer_filters.append(f"EXTRACT(MONTH FROM orders.order_date) = {month}")

    return (
        "SELECT orders.product_name, SUM(orders.revenue) AS total_revenue "
        "FROM orders JOIN customers ON orders.customer_id = customers.customer_id "
        f"WHERE {' AND '.join(outer_filters)} "
        "GROUP BY orders.product_name "
        "ORDER BY total_revenue DESC "
        f"LIMIT {limit};"
    )


def sql_top_customers_for_top_country(limit=5, month=None):
    outer_filters = [f"customers.country = ({build_top_country_subquery(month)})"]
    if month is not None:
        outer_filters.append(f"EXTRACT(MONTH FROM orders.order_date) = {month}")

    return (
        "SELECT customers.name, SUM(orders.revenue) AS total_revenue "
        "FROM orders JOIN customers ON orders.customer_id = customers.customer_id "
        f"WHERE {' AND '.join(outer_filters)} "
        "GROUP BY customers.name "
        "ORDER BY total_revenue DESC "
        f"LIMIT {limit};"
    )


def build_direct_route_from_followup(question, memory):
    if not looks_like_followup_question(question, memory):
        return None

    last_entry = memory.get_last_entry()
    previous_sql = last_entry["sql"] if last_entry else ""
    previous_shape = classify_sql_shape(previous_sql)
    if previous_shape is None:
        return None

    country = extract_country(question) or extract_country_from_sql(previous_sql)
    month = extract_month(question) or extract_month_from_sql(previous_sql)
    limit = extract_limit(question, default=extract_limit_from_sql(previous_sql))

    if previous_shape == "product_ranking":
        return {
            "kind": "product_ranking",
            "sql_query": sql_top_products(limit=limit, country=country, month=month),
            "tables": ["orders", "customers"] if country is not None else ["orders"],
            "country": country,
            "month": month,
            "limit": limit,
        }

    if previous_shape == "country_breakdown":
        return {
            "kind": "country_breakdown",
            "sql_query": sql_revenue_by_country(limit=limit, month=month),
            "tables": ["orders", "customers"],
            "month": month,
            "limit": limit,
        }

    if previous_shape == "customer_ranking":
        return {
            "kind": "customer_ranking",
            "sql_query": sql_top_customers(limit=limit or 5, country=country, month=month),
            "tables": ["orders", "customers"],
            "country": country,
            "month": month,
            "limit": limit or 5,
        }

    if previous_shape == "order_rows":
        return {
            "kind": "order_rows",
            "sql_query": sql_all_orders(country=country, month=month),
            "tables": ["orders", "customers"] if country is not None else ["orders"],
            "country": country,
            "month": month,
        }

    return None


def build_direct_route(question, memory):
    followup_route = build_direct_route_from_followup(question, memory)
    if followup_route is not None:
        return followup_route

    lowered = normalize_text(question)
    country = extract_country(question)
    product = extract_product(question)
    month = extract_month(question)
    limit = extract_limit(question)

    if question_mentions_revenue(question) and re.search(
        r"\bcountry with the highest revenue\b|\bcountry has the highest total revenue\b|\btop revenue country\b|\bcountry leads revenue\b",
        lowered,
    ):
        if re.search(r"\bcustomers?\b", lowered):
            return {
                "kind": "top_customers_for_top_country",
                "sql_query": sql_top_customers_for_top_country(limit=limit or 5, month=month),
                "tables": ["orders", "customers"],
                "month": month,
                "limit": limit or 5,
            }

        if re.search(r"\bproducts?\b", lowered):
            inferred_limit = limit or (1 if re.search(r"\bproduct drives it most\b", lowered) else None)
            return {
                "kind": "top_products_for_top_country",
                "sql_query": sql_top_products_for_top_country(limit=inferred_limit or 2, month=month),
                "tables": ["orders", "customers"],
                "month": month,
                "limit": inferred_limit or 2,
            }

    if question_mentions_orders(question) and re.search(
        r"\bcount\b|\bhow many\b|\bnumber of\b",
        lowered,
    ):
        return {
            "kind": "aggregate_order_count",
            "sql_query": sql_order_count(),
            "tables": ["orders"],
        }

    if question_mentions_revenue(question) and re.search(
        r"\bby month\b|\beach month\b|\bmonthly revenue\b|\brevenue for each month\b",
        lowered,
    ):
        return {
            "kind": "breakdown_month_revenue",
            "sql_query": sql_revenue_by_month(),
            "tables": ["orders"],
        }

    if question_mentions_revenue(question) and product is not None and month is not None:
        return {
            "kind": "product_month_revenue",
            "sql_query": sql_revenue_for_product_month(product, month),
            "tables": ["orders"],
            "product": product,
            "month": month,
        }

    if question_mentions_revenue(question) and country is None and re.search(
        r"\bby country\b|\bper country\b|\bcountry revenue\b|\bcountries generate the most revenue\b",
        lowered,
    ):
        return {
            "kind": "country_breakdown",
            "sql_query": sql_revenue_by_country(limit=limit, month=month),
            "tables": ["orders", "customers"],
            "month": month,
            "limit": limit,
        }

    if re.search(r"\btop\s+\d+\s+customers by revenue\b|\btop\s+\d+\s+customer\b", lowered) or re.search(
        r"\bwho are the top\s+\d+\s+customers by revenue\b",
        lowered,
    ):
        return {
            "kind": "customer_ranking",
            "sql_query": sql_top_customers(limit=limit or 5, country=country, month=month),
            "tables": ["orders", "customers"],
            "country": country,
            "month": month,
            "limit": limit or 5,
        }

    if re.search(
        r"\btop products by revenue\b|\blist products by total revenue\b|\btop\s+\d+\s+products by revenue\b|\btop product by revenue\b|\bproduct revenue rankings\b|\bproducts make the most revenue\b|\bproduct brings the most revenue\b",
        lowered,
    ):
        singular_product = re.search(r"\btop product\b|\bproduct brings the most revenue\b", lowered)
        resolved_limit = limit or (1 if singular_product else None)
        return {
            "kind": "product_ranking",
            "sql_query": sql_top_products(limit=resolved_limit, country=country, month=month),
            "tables": ["orders", "customers"] if country is not None else ["orders"],
            "country": country,
            "month": month,
            "limit": resolved_limit,
        }

    if question_mentions_orders(question) and re.search(
        r"\bshow all orders\b|\blist all orders\b|\bdisplay every order\b|\bgive me all order rows\b|\borders from\b|\borders in\b|\bcomplete orders table\b",
        lowered,
    ):
        return {
            "kind": "order_rows",
            "sql_query": sql_all_orders(country=country, month=month),
            "tables": ["orders", "customers"] if country is not None else ["orders"],
            "country": country,
            "month": month,
        }

    if question_mentions_revenue(question) and country is not None and not re.search(
        r"\btop\b|\bproduct\b|\bcustomer\b",
        lowered,
    ):
        return {
            "kind": "country_total_revenue",
            "sql_query": sql_revenue_for_country(country),
            "tables": ["orders", "customers"],
            "country": country,
        }

    if question_mentions_revenue(question) and country is None and not re.search(
        r"\bby\b|\bper\b|\beach\b|\bmonth\b|\bcountry\b|\bproduct\b|\bcustomer\b|\btop\b|\bhighest\b|\blowest\b",
        lowered,
    ):
        return {
            "kind": "aggregate_total_revenue",
            "sql_query": sql_total_revenue(),
            "tables": ["orders"],
        }

    return None


def build_rule_based_answer(question, route, results):
    if not results:
        return "No matching rows were returned."

    kind = route["kind"]
    country = route.get("country")
    product = route.get("product")
    month = route.get("month")
    month_label = month_name_from_number(month) if month is not None else None
    limit = route.get("limit")

    if kind == "aggregate_total_revenue":
        return f"The total revenue is {format_value(results[0][0])}."

    if kind == "aggregate_order_count":
        return f"There are {format_value(results[0][0])} orders."

    if kind == "country_total_revenue":
        return f"The total revenue for {country} is {format_value(results[0][0])}."

    if kind == "product_month_revenue":
        return (
            f"The revenue for {product} in {month_label} is {format_value(results[0][0])}."
        )

    if kind == "country_breakdown":
        leader, value = results[0]
        scope = f" in {month_label}" if month_label else ""
        if limit:
            return (
                f"The top country by revenue{scope} is {leader} with {format_value(value)}. "
                f"I returned the top {limit} countries."
            )
        return f"The revenue breakdown by country{scope} is led by {leader} with {format_value(value)}."

    if kind in {"product_ranking", "top_products_for_top_country"}:
        leader, value = results[0]
        segments = []
        if country:
            segments.append(f"in {country}")
        if month_label:
            segments.append(f"during {month_label}")
        scope = f" {' '.join(segments)}" if segments else ""
        if limit and limit > 1:
            return (
                f"The top product by revenue{scope} is {leader} with {format_value(value)}. "
                f"I returned the top {limit} products."
            )
        if kind == "top_products_for_top_country":
            return (
                f"For the highest-revenue country{f' in {month_label}' if month_label else ''}, "
                f"the leading product is {leader} with {format_value(value)}."
            )
        return f"The top product by revenue{scope} is {leader} with {format_value(value)}."

    if kind == "breakdown_month_revenue":
        pieces = [
            f"{month_name_from_number(int(row[0]))}: {format_value(row[1])}"
            for row in results[: min(len(results), 3)]
        ]
        return f"Revenue by month is {'; '.join(pieces)}."

    if kind == "customer_ranking":
        leader, value = results[0]
        segments = []
        if country:
            segments.append(f"in {country}")
        if month_label:
            segments.append(f"during {month_label}")
        scope = f" {' '.join(segments)}" if segments else ""
        if limit and limit > 1:
            return (
                f"The leading customer by revenue{scope} is {leader} with {format_value(value)}. "
                f"I returned the top {limit} customers."
            )
        return f"The top customer by revenue{scope} is {leader} with {format_value(value)}."

    if kind == "top_customers_for_top_country":
        leader, value = results[0]
        return (
            f"For the highest-revenue country{f' in {month_label}' if month_label else ''}, "
            f"the leading customer is {leader} with {format_value(value)}."
        )

    if kind == "order_rows":
        filters = []
        if country:
            filters.append(f"for {country}")
        if month_label:
            filters.append(f"in {month_label}")
        suffix = f" {' '.join(filters)}" if filters else ""
        return f"I found {len(results)} matching orders{suffix}."

    return ""


def entity_variants(entity):
    normalized = entity.lower().replace("_", " ")
    variants = {
        normalized,
        singularize(normalized),
        pluralize(singularize(normalized)),
    }
    return {variant.strip() for variant in variants if variant.strip()}


def question_mentions_entity(question, entity):
    lowered = normalize_text(question)
    return any(
        re.search(rf"\b{re.escape(variant)}\b", lowered)
        for variant in entity_variants(entity)
    )


def build_table_description(table, columns):
    col_desc = ", ".join(f"{column} ({dtype})" for column, dtype in columns)
    return f"Table {table} has columns: {col_desc}"


def expand_schema_matches(lines, schema):
    expanded = []
    seen = set()

    def add_line(line):
        if line and line not in seen:
            seen.add(line)
            expanded.append(line)

    for line in lines:
        mentioned_tables = [
            table
            for table in schema
            if line.startswith(f"Table {table} ")
            or f"{table}." in line
            or f" {table} " in line
        ]
        for table in mentioned_tables:
            add_line(build_table_description(table, schema[table]))
        add_line(line)

    return expanded


def find_shared_id_column(schema, left_table, right_table):
    left_columns = {column for column, _dtype in schema[left_table]}
    right_columns = {column for column, _dtype in schema[right_table]}

    preferred = (
        f"{singularize(left_table)}_id",
        f"{singularize(right_table)}_id",
    )
    for column in preferred:
        if column in left_columns and column in right_columns:
            return column

    for column in sorted(left_columns & right_columns):
        if column.endswith("_id"):
            return column

    return None


def select_display_columns(schema, table, required_columns=None, limit=5):
    required_columns = required_columns or []
    ordered = []
    available = [column for column, _dtype in schema[table]]
    priority = (
        *required_columns,
        "order_id",
        "customer_id",
        "name",
        "country",
        "product_name",
        "revenue",
        "order_date",
    )

    for column in priority:
        if column in available and column not in ordered:
            ordered.append(column)

    for column in available:
        if column not in ordered:
            ordered.append(column)

    return ordered[:limit]


def build_relevant_schema_lines(schema, relationship_hints, tables, keywords=None):
    keywords = keywords or []
    lines = []
    seen = set()

    for table in tables:
        line = build_table_description(table, schema[table])
        if line not in seen:
            seen.add(line)
            lines.append(line)

    for hint in relationship_hints:
        if any(f"{table}." in hint or f" {table} " in hint for table in tables) or any(
            keyword and keyword in hint for keyword in keywords
        ):
            if hint not in seen:
                seen.add(hint)
                lines.append(hint)

    return lines


def build_embedded_relationship_response(question, table, column, entity, schema, relationship_hints):
    selected_columns = select_display_columns(schema, table, required_columns=[column])
    order_column = "order_id" if "order_id" in selected_columns else selected_columns[0]
    sql_query = f"SELECT {', '.join(selected_columns)} FROM {table} ORDER BY {order_column};"
    columns, results = run_query_with_columns(sql_query)

    schema_lines = build_relevant_schema_lines(
        schema,
        relationship_hints,
        [table],
        keywords=[f"{table}.{column}", f"{pluralize(entity)} table"],
    )
    join_hints = [
        hint
        for hint in relationship_hints
        if f"{table}." in hint and hint not in schema_lines
    ]

    answer_parts = [
        (
            f"In this schema, {pluralize(entity)} are stored directly on "
            f"`{table}.{column}`. There is no separate `{pluralize(entity)}` table, "
            f"so each `{singularize(table)}` row already contains its `{entity}` value."
        )
    ]
    if join_hints:
        answer_parts.append(f"For extra context, {join_hints[0]}")
    answer_parts.append(
        f"I returned `{table}` rows so you can see the relationship in the actual data."
    )

    return {
        "question": question,
        "plan_steps": [],
        "executed_steps": [
            {
                "index": 1,
                "step": f"Inspect how `{entity}` is represented on `{table}` rows",
                "relevant_schema": schema_lines,
                "schema_context": "\n".join(schema_lines),
                "sql_query": sql_query,
                "columns": columns,
                "results": results,
                "error": None,
            }
        ],
        "relevant_schema": schema_lines,
        "sql_query": sql_query,
        "columns": columns,
        "results": results,
        "answer": " ".join(answer_parts),
        "error": None,
        "chart_figure": None,
    }


def build_join_relationship_response(question, left_table, right_table, join_column, schema, relationship_hints):
    primary_table = "orders" if "orders" in {left_table, right_table} else left_table
    secondary_table = right_table if primary_table == left_table else left_table

    primary_columns = select_display_columns(schema, primary_table, required_columns=[join_column], limit=4)
    secondary_columns = select_display_columns(schema, secondary_table, required_columns=[join_column], limit=3)

    select_parts = [
        f"{primary_table}.{column} AS {primary_table}_{column}"
        for column in primary_columns
    ]
    select_parts.extend(
        f"{secondary_table}.{column} AS {secondary_table}_{column}"
        for column in secondary_columns
        if column != join_column
    )

    order_column = (
        f"{primary_table}.order_id"
        if "order_id" in {column for column, _dtype in schema[primary_table]}
        else f"{primary_table}.{primary_columns[0]}"
    )
    sql_query = (
        f"SELECT {', '.join(select_parts)} "
        f"FROM {primary_table} "
        f"JOIN {secondary_table} "
        f"ON {primary_table}.{join_column} = {secondary_table}.{join_column} "
        f"ORDER BY {order_column};"
    )
    columns, results = run_query_with_columns(sql_query)

    schema_lines = build_relevant_schema_lines(
        schema,
        relationship_hints,
        [primary_table, secondary_table],
        keywords=[f"{primary_table}.{join_column}", f"{secondary_table}.{join_column}"],
    )
    answer = (
        f"In this schema, `{primary_table}.{join_column}` joins to "
        f"`{secondary_table}.{join_column}`, so `{singularize(primary_table)}` rows are "
        f"connected to `{singularize(secondary_table)}` rows through that shared key. "
        f"I returned joined rows so you can inspect the relationship in real data."
    )

    return {
        "question": question,
        "plan_steps": [],
        "executed_steps": [
            {
                "index": 1,
                "step": f"Join `{primary_table}` to `{secondary_table}` on `{join_column}`",
                "relevant_schema": schema_lines,
                "schema_context": "\n".join(schema_lines),
                "sql_query": sql_query,
                "columns": columns,
                "results": results,
                "error": None,
            }
        ],
        "relevant_schema": schema_lines,
        "sql_query": sql_query,
        "columns": columns,
        "results": results,
        "answer": answer,
        "error": None,
        "chart_figure": None,
    }


def build_relationship_response(question, schema, relationship_hints):
    mentioned_tables = [
        table for table in schema if question_mentions_entity(question, table)
    ]

    if len(mentioned_tables) >= 2:
        for index, left_table in enumerate(mentioned_tables):
            for right_table in mentioned_tables[index + 1 :]:
                join_column = find_shared_id_column(schema, left_table, right_table)
                if join_column:
                    return build_join_relationship_response(
                        question,
                        left_table,
                        right_table,
                        join_column,
                        schema,
                        relationship_hints,
                    )

    if mentioned_tables:
        for table in mentioned_tables:
            for column, _dtype in schema[table]:
                if column.endswith("_name"):
                    entity = column[:-5]
                    if question_mentions_entity(question, entity):
                        return build_embedded_relationship_response(
                            question,
                            table,
                            column,
                            entity,
                            schema,
                            relationship_hints,
                        )

    return None


def build_retrieval_query(question, memory):
    recent_questions = memory.get_recent_questions()
    if not recent_questions:
        return question

    return "\n".join(recent_questions + [question])


def get_relevant_schema(question, vector_store, memory, k=2):
    retrieval_query = build_retrieval_query(question, memory)
    return vector_store.search(retrieval_query, k=k)


def format_results_preview(columns, results, limit=5):
    if not results:
        return "No rows returned."

    preview_rows = results[:limit]
    return f"Columns: {columns}\nRows: {preview_rows}"


def format_step_results_context(executed_steps):
    if not executed_steps:
        return ""

    chunks = []
    for step in executed_steps:
        chunks.append(
            "\n".join(
                [
                    f"Step {step['index']}: {step['step']}",
                    f"SQL: {step['sql_query']}",
                    format_results_preview(step["columns"], step["results"]),
                ]
            )
        )

    return "\n\n".join(chunks)


def dedupe_schema_lines(step_details):
    seen = set()
    ordered = []

    for step in step_details:
        for line in step["relevant_schema"]:
            if line not in seen:
                seen.add(line)
                ordered.append(line)

    return ordered


def dedupe_text_items(items):
    seen = set()
    ordered = []

    for item in items:
        cleaned = item.strip()
        if cleaned and cleaned not in seen:
            seen.add(cleaned)
            ordered.append(cleaned)

    return ordered


def table_columns(schema, table):
    return {column for column, _dtype in schema[table]}


def is_revenue_focused(question, sql_query, columns):
    combined = " ".join(
        part for part in [question.lower(), sql_query.lower(), " ".join(columns or []).lower()] if part
    )
    return "revenue" in combined


def is_overall_revenue_query(question, sql_query):
    lowered_question = question.lower()
    lowered_sql = sql_query.lower()

    if "where " in lowered_sql:
        return False

    if "group by" in lowered_sql:
        return False

    if any(
        token in lowered_question
        for token in ("country", "countries", "month", "product", "customer", "top", "during")
    ):
        return False

    return "sum(revenue)" in lowered_sql


def run_supporting_analysis_query(title, sql_query):
    try:
        columns, results = run_query_with_columns(sql_query)
    except Exception:
        return None

    if not results:
        return None

    return {
        "title": title,
        "sql_query": sql_query,
        "columns": columns,
        "results": results,
        "data_type": detect_type(results),
    }


def build_country_revenue_analysis(schema):
    revenue_tables = [
        table
        for table in schema
        if "revenue" in table_columns(schema, table)
    ]

    for fact_table in revenue_tables:
        fact_columns = table_columns(schema, fact_table)

        if "country" in fact_columns:
            return run_supporting_analysis_query(
                "Revenue by country",
                (
                    f"SELECT country, SUM(revenue) AS total_revenue "
                    f"FROM {fact_table} GROUP BY country ORDER BY total_revenue DESC;"
                ),
            )

        for dim_table in schema:
            if dim_table == fact_table or "country" not in table_columns(schema, dim_table):
                continue

            join_column = find_shared_id_column(schema, fact_table, dim_table)
            if join_column:
                return run_supporting_analysis_query(
                    "Revenue by country",
                    (
                        f"SELECT {dim_table}.country, SUM({fact_table}.revenue) AS total_revenue "
                        f"FROM {fact_table} JOIN {dim_table} "
                        f"ON {fact_table}.{join_column} = {dim_table}.{join_column} "
                        f"GROUP BY {dim_table}.country ORDER BY total_revenue DESC;"
                    ),
                )

    return None


def build_product_revenue_analysis(schema):
    revenue_tables = [
        table
        for table in schema
        if "revenue" in table_columns(schema, table)
    ]

    for fact_table in revenue_tables:
        fact_columns = table_columns(schema, fact_table)
        product_column = next(
            (
                column
                for column in ("product_name", "item_name", "category", "name")
                if column in fact_columns
            ),
            None,
        )
        if product_column:
            return run_supporting_analysis_query(
                "Revenue by product",
                (
                    f"SELECT {product_column}, SUM(revenue) AS total_revenue "
                    f"FROM {fact_table} GROUP BY {product_column} ORDER BY total_revenue DESC;"
                ),
            )

    return None


def build_contextual_insights(question, sql_query, columns, data_type, schema):
    basic_insights = []
    anomalies = []
    supporting_analyses = []
    supporting_context_lines = []

    if (
        data_type != "single_value"
        or not is_revenue_focused(question, sql_query, columns)
        or not is_overall_revenue_query(question, sql_query)
    ):
        return basic_insights, anomalies, supporting_context_lines, supporting_analyses

    country_analysis = build_country_revenue_analysis(schema)
    if country_analysis is not None and country_analysis["data_type"] == "category_value":
        supporting_analyses.append(country_analysis)
        sorted_countries = sorted(
            country_analysis["results"],
            key=lambda row: float(row[1]),
            reverse=True,
        )
        total_revenue = sum(float(row[1]) for row in sorted_countries)
        top_country, top_country_value = sorted_countries[0]
        if total_revenue > 0:
            country_share = (float(top_country_value) / total_revenue) * 100
            basic_insights.append(
                f"{top_country} contributes {format_percent(country_share)} of total revenue."
            )
        anomalies.extend(detect_anomalies(country_analysis["results"]))
        supporting_context_lines.append(
            f"{country_analysis['title']}: {country_analysis['results']}"
        )

    product_analysis = build_product_revenue_analysis(schema)
    if product_analysis is not None and product_analysis["data_type"] == "category_value":
        supporting_analyses.append(product_analysis)
        sorted_products = sorted(
            product_analysis["results"],
            key=lambda row: float(row[1]),
            reverse=True,
        )
        total_revenue = sum(float(row[1]) for row in sorted_products)
        top_product, top_product_value = sorted_products[0]
        basic_insights.append(
            f"{top_product} is the top product with {format_value(top_product_value)} revenue."
        )

        if len(sorted_products) > 1:
            second_product, second_value = sorted_products[1]
            basic_insights.append(
                f"{second_product} is the second product with {format_value(second_value)} revenue."
            )

        if total_revenue > 0 and len(sorted_products) > 1:
            top_two_share = (
                sum(float(row[1]) for row in sorted_products[:2]) / total_revenue
            ) * 100
            if top_two_share >= 70:
                basic_insights.append(
                    f"Revenue is concentrated in a few products: the top 2 products account for {format_percent(top_two_share)}."
                )

        anomalies.extend(detect_anomalies(product_analysis["results"]))
        supporting_context_lines.append(
            f"{product_analysis['title']}: {product_analysis['results']}"
        )

    return basic_insights, anomalies, supporting_context_lines, supporting_analyses


def enrich_response_with_insights(response, schema):
    response["data_type"] = detect_type(response.get("results", []))
    response["basic_insights"] = []
    response["anomalies"] = []
    response["ai_insights"] = ""
    response["supporting_analyses"] = []

    if response.get("error"):
        return response

    basic_insights = generate_basic_insights(response["results"], response["data_type"])
    anomalies = detect_anomalies(response["results"])
    contextual_basic, contextual_anomalies, supporting_context_lines, supporting_analyses = (
        build_contextual_insights(
            response["question"],
            response["sql_query"],
            response["columns"],
            response["data_type"],
            schema,
        )
    )

    basic_insights = dedupe_text_items(basic_insights + contextual_basic)
    anomalies = dedupe_text_items(anomalies + contextual_anomalies)
    response["basic_insights"] = basic_insights
    response["anomalies"] = anomalies
    response["supporting_analyses"] = supporting_analyses

    if response["data_type"] == "table":
        return response

    if not is_relationship_question(response["question"]):
        response["ai_insights"] = generate_ai_insights(
            response["question"],
            response["columns"],
            response["results"],
            response["data_type"],
            basic_insights=basic_insights,
            anomalies=anomalies,
            supporting_context="\n".join(supporting_context_lines),
        )

    return response


def execute_with_retry(
    sql_query,
    question,
    schema_context,
    memory_context="",
    original_question=None,
    previous_results_context="",
    retries=2,
):
    current_sql = sql_query.replace("\n", " ").strip()
    last_error = None

    for attempt in range(retries + 1):
        try:
            columns, results = run_query_with_columns(current_sql)
            return current_sql, columns, results, None
        except Exception as exc:
            last_error = exc

            if attempt == retries:
                break

            current_sql = fix_sql(
                current_sql,
                str(exc),
                question,
                schema_context,
                memory_context,
                original_question=original_question,
                previous_results_context=previous_results_context,
            )
            current_sql = current_sql.replace("\n", " ").strip()

    return current_sql, [], [], last_error


def build_explain_prompt(question, plan_steps, executed_steps, schema_context):
    step_summaries = []
    for step in executed_steps:
        step_summaries.append(
            "\n".join(
                [
                    f"Step {step['index']}: {step['step']}",
                    f"SQL: {step['sql_query']}",
                    format_results_preview(step["columns"], step["results"]),
                ]
            )
        )

    plan_block = "\n".join(f"{index + 1}. {step}" for index, step in enumerate(plan_steps))
    final_step = executed_steps[-1]

    return f"""
You are a data analyst.

Relevant schema:
{schema_context}

User question:
{question}

Plan:
{plan_block}

Step execution summary:
{chr(10).join(step_summaries)}

Final SQL:
{final_step['sql_query']}

Final result:
{final_step['results']}

Explain:
1. The direct answer to the user's question
2. Briefly connect the SQL result to the question when useful
3. Keep it simple and concise

Rules:
- Use only facts supported by the schema and result
- If you mention columns, use the exact column names from the schema or SQL
- Do not add a currency symbol or currency code unless the question or schema explicitly provides one
- Do not infer trends, causes, operational patterns, or repeated behavior unless the result explicitly proves them
- If the result is just a few rows, summarize only what those rows directly show
- Do not add extra recommendations or deeper analysis
- Respond as a short natural-language answer
- Do not repeat these instruction bullets in the output
""".strip()


def process_question(question, memory=None, show_chart=False):
    if memory is None:
        memory = Memory()

    vector_store, _, schema, relationship_hints = build_schema_search()
    memory_context = memory.get_context()

    if is_relationship_question(question):
        relationship_response = build_relationship_response(question, schema, relationship_hints)
        if relationship_response is not None:
            memory.add(question, relationship_response["sql_query"])
            return enrich_response_with_insights(relationship_response, schema)

    direct_route = build_direct_route(question, memory)
    if direct_route is not None:
        relevant_schema = build_relevant_schema_lines(
            schema,
            relationship_hints,
            direct_route["tables"],
        )
        schema_context = "\n".join(relevant_schema)
        sql_query, columns, results, execution_error = execute_with_retry(
            direct_route["sql_query"],
            question,
            schema_context,
            memory_context,
            original_question=question,
        )
        if execution_error is not None:
            return enrich_response_with_insights(
                {
                    "question": question,
                    "plan_steps": [],
                    "executed_steps": [
                        {
                            "index": 1,
                            "step": question,
                            "relevant_schema": relevant_schema,
                            "schema_context": schema_context,
                            "sql_query": sql_query,
                            "columns": columns,
                            "results": results,
                            "error": str(execution_error),
                        }
                    ],
                    "relevant_schema": relevant_schema,
                    "sql_query": sql_query,
                    "columns": columns,
                    "results": results,
                    "answer": "",
                    "error": str(execution_error),
                    "chart_figure": None,
                },
                schema,
            )

        chart_figure = None
        if show_chart:
            chart_figure = plot_results(results, title=question, show=True)

        answer = build_rule_based_answer(question, direct_route, results)
        if not answer:
            answer = ask_llm(
                build_explain_prompt(
                    question,
                    [],
                    [
                        {
                            "index": 1,
                            "step": question,
                            "sql_query": sql_query,
                            "columns": columns,
                            "results": results,
                        }
                    ],
                    schema_context,
                )
            )

        memory.add(question, sql_query)
        return enrich_response_with_insights(
            {
                "question": question,
                "plan_steps": [],
                "executed_steps": [
                    {
                        "index": 1,
                        "step": question,
                        "relevant_schema": relevant_schema,
                        "schema_context": schema_context,
                        "sql_query": sql_query,
                        "columns": columns,
                        "results": results,
                        "error": None,
                    }
                ],
                "relevant_schema": relevant_schema,
                "sql_query": sql_query,
                "columns": columns,
                "results": results,
                "answer": answer,
                "error": None,
                "chart_figure": chart_figure,
            },
            schema,
        )

    plan_steps = []

    if should_plan(question):
        raw_plan = create_plan(question, memory_context)
        plan_steps = parse_plan(raw_plan)

    if not plan_steps:
        plan_steps = [question]

    executed_steps = []

    for index, step in enumerate(plan_steps, start=1):
        previous_results_context = format_step_results_context(executed_steps)
        retrieval_query = "\n".join(
            part
            for part in [question, step, previous_results_context]
            if part
        )
        relevant_schema = expand_schema_matches(vector_store.search(retrieval_query, k=2), schema)
        schema_context = "\n".join(relevant_schema)
        step_question = question if len(plan_steps) == 1 else step

        sql_query = generate_sql(
            step_question,
            schema_context,
            memory_context,
            original_question=question,
            previous_results_context=previous_results_context,
        )
        sql_query = sql_query.replace("\n", " ").strip()

        sql_query, columns, results, execution_error = execute_with_retry(
            sql_query,
            step_question,
            schema_context,
            memory_context,
            original_question=question,
            previous_results_context=previous_results_context,
        )

        step_record = {
            "index": index,
            "step": step,
            "relevant_schema": relevant_schema,
            "schema_context": schema_context,
            "sql_query": sql_query,
            "columns": columns,
            "results": results,
            "error": str(execution_error) if execution_error else None,
        }
        executed_steps.append(step_record)

        if execution_error is not None:
            return enrich_response_with_insights(
                {
                "question": question,
                "plan_steps": plan_steps,
                "executed_steps": executed_steps,
                "relevant_schema": dedupe_schema_lines(executed_steps),
                "sql_query": sql_query,
                "columns": columns,
                "results": results,
                "answer": "",
                "error": str(execution_error),
                },
                schema,
            )

    final_step = executed_steps[-1]
    all_schema = dedupe_schema_lines(executed_steps)
    answer = ask_llm(
        build_explain_prompt(
            question,
            plan_steps,
            executed_steps,
            "\n".join(all_schema),
        )
    )
    chart_figure = None
    if show_chart:
        chart_figure = plot_results(final_step["results"], title=question, show=True)

    memory.add(question, final_step["sql_query"])

    return enrich_response_with_insights(
        {
        "question": question,
        "plan_steps": plan_steps,
        "executed_steps": executed_steps,
        "relevant_schema": all_schema,
        "sql_query": final_step["sql_query"],
        "columns": final_step["columns"],
        "results": final_step["results"],
        "answer": answer,
        "error": None,
        "chart_figure": chart_figure,
        },
        schema,
    )


def print_response(response):
    if response.get("plan_steps"):
        print("Plan:")
        for index, step in enumerate(response["plan_steps"], start=1):
            print(f"{index}. {step}")

    if response.get("executed_steps"):
        for step in response["executed_steps"]:
            print(f"\nStep {step['index']}: {step['step']}")
            print("Relevant schema:")
            for line in step["relevant_schema"]:
                print(line)
            print("SQL:")
            print(step["sql_query"])
            if step["error"]:
                print(f"Error: {step['error']}")
            else:
                print("Results:")
                print(step["results"])

    print("\nFinal schema context:")
    print("Relevant schema:")
    for result in response["relevant_schema"]:
        print(result)

    print("\nFinal SQL:")
    print(response["sql_query"])

    if response["error"]:
        print(f"\nFinal SQL Error: {response['error']}")
        return

    print("\nResults:")
    print(response["results"])

    print("\nFinal Answer:")
    print(response["answer"])

    if response.get("basic_insights"):
        print("\nInsights:")
        for insight in response["basic_insights"]:
            print(f"- {insight}")

    if response.get("anomalies"):
        print("\nAnomalies:")
        for anomaly in response["anomalies"]:
            print(f"- {anomaly}")

    if response.get("ai_insights"):
        print("\nDeeper Insights:")
        print(response["ai_insights"])

    if response.get("supporting_analyses"):
        print("\nInsight Support:")
        for analysis in response["supporting_analyses"]:
            print(f"{analysis['title']}:")
            print(analysis["sql_query"])
            print(analysis["results"])


def run_data_copilot(question, show_chart=None):
    memory = Memory()
    response = process_question(
        question,
        memory=memory,
        show_chart=charts_enabled() if show_chart is None else show_chart,
    )
    print_response(response)


def run_chat(show_chart=None):
    memory = Memory()
    chart_mode = charts_enabled() if show_chart is None else show_chart

    print("Chat mode started. Type 'exit' or 'quit' to stop.")

    while True:
        question = input("\nAsk: ").strip()

        if not question:
            continue

        if question.lower() in {"exit", "quit"}:
            break

        response = process_question(question, memory=memory, show_chart=chart_mode)
        print_response(response)
