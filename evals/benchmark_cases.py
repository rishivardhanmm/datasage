from dataclasses import dataclass, field


MONTHS = {
    "January": 1,
    "February": 2,
    "March": 3,
}

COUNTRIES = ["UK", "India", "USA", "Germany", "Singapore"]
PRODUCTS = ["Laptop", "Phone", "Camera", "Tablet", "Monitor"]


@dataclass(frozen=True)
class BenchmarkCase:
    case_id: str
    category: str
    question: str
    expected_sql: str
    compare_mode: str = "exact"
    session_id: str | None = None
    answer_fragments: tuple[str, ...] = field(default_factory=tuple)


def total_revenue_sql():
    return "SELECT SUM(revenue) FROM orders;"


def order_count_sql():
    return "SELECT COUNT(*) FROM orders;"


def all_orders_sql():
    return "SELECT order_id, customer_id, product_name, revenue, order_date FROM orders ORDER BY order_id;"


def revenue_by_country_sql(limit=None, month=None):
    where = ""
    if month is not None:
        where = f" WHERE EXTRACT(MONTH FROM orders.order_date) = {month}"
    limit_clause = f" LIMIT {limit}" if limit is not None else ""
    return (
        "SELECT customers.country, SUM(orders.revenue) AS total_revenue "
        "FROM orders JOIN customers ON orders.customer_id = customers.customer_id"
        f"{where} "
        "GROUP BY customers.country "
        "ORDER BY total_revenue DESC"
        f"{limit_clause};"
    )


def top_products_sql(limit=None, country=None, month=None):
    filters = []
    joins = ""
    if country is not None:
        joins = " JOIN customers ON orders.customer_id = customers.customer_id"
        filters.append(f"customers.country = '{country}'")
    if month is not None:
        filters.append(f"EXTRACT(MONTH FROM orders.order_date) = {month}")
    where = f" WHERE {' AND '.join(filters)}" if filters else ""
    limit_clause = f" LIMIT {limit}" if limit is not None else ""
    return (
        "SELECT orders.product_name, SUM(orders.revenue) AS total_revenue "
        "FROM orders"
        f"{joins}"
        f"{where} "
        "GROUP BY orders.product_name "
        "ORDER BY total_revenue DESC"
        f"{limit_clause};"
    )


def orders_in_month_sql(month):
    return (
        "SELECT order_id, customer_id, product_name, revenue, order_date "
        "FROM orders "
        f"WHERE EXTRACT(MONTH FROM order_date) = {month} "
        "ORDER BY order_id;"
    )


def revenue_for_country_sql(country):
    return (
        "SELECT SUM(orders.revenue) "
        "FROM orders JOIN customers ON orders.customer_id = customers.customer_id "
        f"WHERE customers.country = '{country}';"
    )


def product_revenue_in_month_sql(product, month):
    return (
        "SELECT SUM(revenue) "
        "FROM orders "
        f"WHERE product_name = '{product}' AND EXTRACT(MONTH FROM order_date) = {month};"
    )


def top_customers_sql(limit=None, country=None, month=None):
    filters = []
    if country is not None:
        filters.append(f"customers.country = '{country}'")
    if month is not None:
        filters.append(f"EXTRACT(MONTH FROM orders.order_date) = {month}")
    where = f" WHERE {' AND '.join(filters)}" if filters else ""
    limit_clause = f" LIMIT {limit}" if limit is not None else ""
    return (
        "SELECT customers.name, SUM(orders.revenue) AS total_revenue "
        "FROM orders JOIN customers ON orders.customer_id = customers.customer_id"
        f"{where} "
        "GROUP BY customers.name "
        "ORDER BY total_revenue DESC"
        f"{limit_clause};"
    )


def orders_for_country_sql(country, month=None):
    filters = [f"customers.country = '{country}'"]
    if month is not None:
        filters.append(f"EXTRACT(MONTH FROM orders.order_date) = {month}")
    where = " AND ".join(filters)
    return (
        "SELECT orders.order_id, orders.customer_id, orders.product_name, orders.revenue, orders.order_date "
        "FROM orders JOIN customers ON orders.customer_id = customers.customer_id "
        f"WHERE {where} ORDER BY orders.order_id;"
    )


def relationship_orders_products_sql():
    return (
        "SELECT product_name, order_id, customer_id, revenue, order_date "
        "FROM orders ORDER BY order_id;"
    )


def relationship_orders_customers_sql():
    return (
        "SELECT orders.customer_id AS orders_customer_id, "
        "orders.order_id AS orders_order_id, "
        "orders.product_name AS orders_product_name, "
        "orders.revenue AS orders_revenue, "
        "customers.name AS customers_name, "
        "customers.country AS customers_country "
        "FROM orders JOIN customers ON orders.customer_id = customers.customer_id "
        "ORDER BY orders.order_id;"
    )


def top_products_for_top_country_sql(month=None, limit=2):
    month_filter = ""
    month_filter_alias = ""
    if month is not None:
        month_filter = f"WHERE EXTRACT(MONTH FROM orders.order_date) = {month}"
        month_filter_alias = f" AND EXTRACT(MONTH FROM orders.order_date) = {month}"
    return (
        "SELECT orders.product_name, SUM(orders.revenue) AS total_revenue "
        "FROM orders JOIN customers ON orders.customer_id = customers.customer_id "
        "WHERE customers.country = ("
        "SELECT customers.country "
        "FROM orders JOIN customers ON orders.customer_id = customers.customer_id "
        f"{month_filter} "
        "GROUP BY customers.country "
        "ORDER BY SUM(orders.revenue) DESC "
        "LIMIT 1"
        f"){month_filter_alias} "
        "GROUP BY orders.product_name "
        "ORDER BY total_revenue DESC "
        f"LIMIT {limit};"
    )


def top_customer_for_top_country_sql(limit=5):
    return (
        "SELECT customers.name, SUM(orders.revenue) AS total_revenue "
        "FROM orders JOIN customers ON orders.customer_id = customers.customer_id "
        "WHERE customers.country = ("
        "SELECT customers.country "
        "FROM orders JOIN customers ON orders.customer_id = customers.customer_id "
        "GROUP BY customers.country "
        "ORDER BY SUM(orders.revenue) DESC "
        "LIMIT 1"
        ") "
        "GROUP BY customers.name "
        "ORDER BY total_revenue DESC "
        f"LIMIT {limit};"
    )


def revenue_by_month_sql():
    return (
        "SELECT EXTRACT(MONTH FROM order_date) AS month, SUM(revenue) AS total_revenue "
        "FROM orders GROUP BY month ORDER BY month;"
    )


def build_cases():
    cases = []
    index = 1

    def add(category, question, expected_sql, compare_mode="exact", session_id=None, answer_fragments=()):
        nonlocal index
        cases.append(
            BenchmarkCase(
                case_id=f"C{index:03d}",
                category=category,
                question=question,
                expected_sql=expected_sql,
                compare_mode=compare_mode,
                session_id=session_id,
                answer_fragments=tuple(answer_fragments),
            )
        )
        index += 1

    for question in [
        "Show revenue",
        "Show total revenue",
        "What is the total revenue?",
        "How much revenue did we make?",
        "Give me overall revenue",
        "What is the sum of revenue?",
    ]:
        add("aggregate_total_revenue", question, total_revenue_sql())

    for question in [
        "How many orders are there?",
        "Count all orders",
        "What is the total number of orders?",
        "Show order count",
        "How many sales records do we have?",
        "Give me the number of orders",
    ]:
        add("aggregate_order_count", question, order_count_sql())

    for question in [
        "Show all orders",
        "List all orders",
        "Display every order",
        "Give me all order rows",
        "Show the complete orders table",
    ]:
        add("table_all_orders", question, all_orders_sql())

    for question in [
        "Show revenue by country",
        "Revenue per country",
        "List country revenue totals",
        "Which countries generate the most revenue?",
    ]:
        add("breakdown_country_revenue", question, revenue_by_country_sql())

    for question in [
        "Top products by revenue",
        "Show product revenue rankings",
        "List products by total revenue",
        "Which products make the most revenue?",
    ]:
        add("breakdown_product_revenue", question, top_products_sql())

    for limit in range(1, 6):
        add(
            "top_n_products",
            f"Top {limit} products by revenue",
            top_products_sql(limit=limit),
        )
        add(
            "top_n_products",
            f"Show the top {limit} products by revenue",
            top_products_sql(limit=limit),
        )

    for month_name, month_number in MONTHS.items():
        add("orders_in_month", f"Show orders in {month_name}", orders_in_month_sql(month_number))
        add("orders_in_month", f"List all orders for {month_name}", orders_in_month_sql(month_number))
        add("orders_in_month", f"Orders from {month_name}", orders_in_month_sql(month_number))

    for country in COUNTRIES:
        add("country_total_revenue", f"Show total revenue for {country}", revenue_for_country_sql(country))
        add("country_total_revenue", f"What revenue came from {country}?", revenue_for_country_sql(country))

    for country in COUNTRIES:
        add(
            "country_top_product",
            f"Top product by revenue in {country}",
            top_products_sql(limit=1, country=country),
        )
        add(
            "country_top_product",
            f"Which product brings the most revenue in {country}?",
            top_products_sql(limit=1, country=country),
        )

    for country in ["UK", "India", "USA"]:
        for month_name, month_number in MONTHS.items():
            add(
                "country_month_top_products",
                f"Top 2 products by revenue in {country} during {month_name}",
                top_products_sql(limit=2, country=country, month=month_number),
            )

    for question in [
        "Show revenue by month",
        "Revenue for each month",
        "Monthly revenue totals",
    ]:
        add("breakdown_month_revenue", question, revenue_by_month_sql())

    for product in PRODUCTS:
        for month_name, month_number in MONTHS.items():
            add(
                "product_month_revenue",
                f"What revenue did {product} generate in {month_name}?",
                product_revenue_in_month_sql(product, month_number),
            )

    add(
        "relationship",
        "Show me how product and orders are related",
        relationship_orders_products_sql(),
        answer_fragments=("orders.product_name",),
    )
    add(
        "relationship",
        "Show me how customers and orders are related",
        relationship_orders_customers_sql(),
        answer_fragments=("orders.customer_id", "customers.customer_id"),
    )

    add(
        "complex_multi_step",
        "Which country has the highest total revenue in February and what are the top 2 products contributing to it?",
        top_products_for_top_country_sql(month=2, limit=2),
    )
    add(
        "complex_multi_step",
        "Which country has the highest total revenue in March and what are the top 2 products contributing to it?",
        top_products_for_top_country_sql(month=3, limit=2),
    )
    add(
        "complex_multi_step",
        "Show the top 3 products for the country with the highest revenue overall",
        top_products_for_top_country_sql(limit=3),
    )
    add(
        "complex_multi_step",
        "Which country leads revenue in January and what product drives it most?",
        top_products_for_top_country_sql(month=1, limit=1),
    )
    add(
        "complex_multi_step",
        "For the top revenue country overall, show its top 5 customers",
        top_customer_for_top_country_sql(limit=5),
    )

    for limit in [3, 5]:
        add(
            "customer_ranking",
            f"Show top {limit} customers by revenue",
            top_customers_sql(limit=limit),
        )
        add(
            "customer_ranking",
            f"Who are the top {limit} customers by revenue?",
            top_customers_sql(limit=limit),
        )

    add(
        "followup_top_products",
        "Show top products by revenue",
        top_products_sql(),
        session_id="S1",
    )
    add(
        "followup_top_products",
        "Now only for UK",
        top_products_sql(country="UK"),
        session_id="S1",
    )
    add(
        "followup_top_products",
        "Only in February",
        top_products_sql(country="UK", month=2),
        session_id="S1",
    )

    add(
        "followup_country_breakdown",
        "Show revenue by country",
        revenue_by_country_sql(),
        session_id="S2",
    )
    add(
        "followup_country_breakdown",
        "Only in March",
        revenue_by_country_sql(month=3),
        session_id="S2",
    )
    add(
        "followup_country_breakdown",
        "Top 3 only",
        revenue_by_country_sql(limit=3, month=3),
        session_id="S2",
    )

    add(
        "followup_orders",
        "Show all orders",
        all_orders_sql(),
        session_id="S3",
    )
    add(
        "followup_orders",
        "Only for India",
        orders_for_country_sql("India"),
        session_id="S3",
    )
    add(
        "followup_orders",
        "Only in January",
        orders_for_country_sql("India", month=1),
        session_id="S3",
    )

    add(
        "followup_customers",
        "Show top 5 customers by revenue",
        top_customers_sql(limit=5),
        session_id="S4",
    )
    add(
        "followup_customers",
        "Now only for USA",
        top_customers_sql(limit=5, country="USA"),
        session_id="S4",
    )
    add(
        "followup_customers",
        "Only in March",
        top_customers_sql(limit=5, country="USA", month=3),
        session_id="S4",
    )

    assert len(cases) >= 100, f"Expected at least 100 cases, found {len(cases)}"
    return cases
