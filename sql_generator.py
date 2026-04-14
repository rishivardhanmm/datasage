from datetime import date
import re

from llm import ask_llm


FORBIDDEN_KEYWORDS = (
    "delete",
    "update",
    "drop",
    "insert",
    "alter",
    "truncate",
    "create",
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

TIME_KEYWORDS = (
    "today",
    "yesterday",
    "tomorrow",
    "week",
    "month",
    "year",
    "date",
    "quarter",
    "february",
    "january",
    "march",
    "april",
    "may",
    "june",
    "july",
    "august",
    "september",
    "october",
    "november",
    "december",
)

RELATIONSHIP_KEYWORDS = (
    "related",
    "relationship",
    "relationships",
    "connect",
    "connected",
    "link",
    "linked",
    "join",
    "joined",
)


def clean_sql(response):
    text = response.strip()

    fenced_blocks = re.findall(r"```(?:sql)?\s*(.*?)```", text, flags=re.IGNORECASE | re.DOTALL)
    if fenced_blocks:
        text = fenced_blocks[0].strip()

    match = re.search(r"\b(select|with)\b.*", text, flags=re.IGNORECASE | re.DOTALL)
    if match:
        text = match.group(0).strip()

    lines = [line.strip() for line in text.splitlines() if line.strip()]
    cleaned = " ".join(lines)

    if ";" in cleaned:
        cleaned = cleaned.split(";", 1)[0].strip()

    if cleaned and not cleaned.endswith(";"):
        cleaned = f"{cleaned};"

    return cleaned


def validate_sql(sql_query):
    if not sql_query:
        raise ValueError("The model did not return a SQL query.")

    lowered = sql_query.lower()

    if not lowered.startswith(("select", "with")):
        raise ValueError("Only SELECT queries are allowed.")

    if ";" in sql_query[:-1]:
        raise ValueError("Only a single SQL statement is allowed.")

    for keyword in FORBIDDEN_KEYWORDS:
        if re.search(rf"\b{keyword}\b", lowered):
            raise ValueError(f"Unsafe SQL detected: {keyword.upper()} is not allowed.")

    return sql_query


def normalize_sql(sql_query):
    return validate_sql(clean_sql(sql_query))


def build_question_hints(question, memory_context=""):
    lowered = question.lower()
    hints = []
    has_memory = bool(memory_context.strip())
    followup_prefixes = ("now", "and", "only", "also", "what about", "for ", "with ")
    looks_like_followup = has_memory and (
        lowered.startswith(followup_prefixes) or len(lowered.split()) <= 5
    )
    asks_about_time = any(keyword in lowered for keyword in TIME_KEYWORDS) or bool(
        re.search(r"\b(19|20)\d{2}\b", lowered)
    )
    asks_about_relationships = any(keyword in lowered for keyword in RELATIONSHIP_KEYWORDS)

    if re.search(r"\btop\s+product\b", lowered):
        hints.append(
            "If the user asks for a single top product without giving a number, "
            "return only one row with LIMIT 1."
        )
    elif re.search(r"\btop\s+products\b", lowered) and not re.search(r"\btop\s+\d+\b", lowered):
        hints.append(
            "If the user asks for top products in plural without giving a number, "
            "sort by the relevant metric in descending order but do not force LIMIT 1."
        )

    if "order_date" in lowered:
        hints.append(
            "Use the order_date column for date filtering only when the question "
            "explicitly asks about time."
        )

    if "orders" in lowered and "total" not in lowered and "sum" not in lowered:
        hints.append(
            "If the question asks for orders, return matching order rows instead "
            "of aggregated totals unless aggregation is explicitly requested."
        )

    if "revenue" in lowered and not re.search(
        r"\b(by|per|for each|top|highest|lowest|country|product|customer|month|date)\b",
        lowered,
    ):
        hints.append(
            "If the user asks for revenue without a breakdown dimension, return a single "
            "total using SUM(revenue)."
        )

    if asks_about_relationships:
        hints.append(
            "If the user asks how entities are related, first check whether each entity "
            "exists as a table in the schema."
        )
        hints.append(
            "If an entity appears only as a column such as product_name, use that column "
            "directly instead of inventing a separate table or junction table."
        )
        hints.append(
            "For relationship questions, prefer a simple query that demonstrates the real "
            "connection using existing tables and columns."
        )

    for month_name, month_number in MONTH_TO_NUMBER.items():
        if month_name in lowered and not re.search(r"\b(19|20)\d{2}\b", lowered):
            hints.append(
                f"The question mentions {month_name} without a year. Filter by month "
                f"only using EXTRACT(MONTH FROM order_date) = {month_number}. "
                "Do not assume a specific year."
            )
            break

    if looks_like_followup:
        hints.append(
            "This is a follow-up question. Start from the most recent SQL in the previous "
            "conversation and modify it instead of writing a brand-new unrelated query."
        )
        hints.append(
            "When the follow-up adds a new filter such as country or month, keep the "
            "existing filters, joins, grouping, ordering, and limits unless the user "
            "explicitly asks to remove or replace them."
        )

    if has_memory and not asks_about_time:
        hints.append(
            "Do not add a new date or time filter unless the current question explicitly "
            "asks for one or the previous SQL already had one."
        )

    return hints


def generate_sql(
    question,
    schema_context,
    memory_context="",
    original_question=None,
    previous_results_context="",
):
    hint_question = original_question or question
    hints = build_question_hints(hint_question, memory_context)
    hint_block = "\n".join(f"- {hint}" for hint in hints) or "- No extra hints"
    memory_block = memory_context or "No previous conversation."
    prior_results_block = previous_results_context or "No previous step results."
    original_question_block = original_question or question

    prompt = f"""
You are an expert PostgreSQL SQL generator.

Current date:
{date.today().isoformat()}

Previous conversation:
{memory_block}

Database schema:
{schema_context}

Original user question:
{original_question_block}

Current question:
{question}

Previous step results:
{prior_results_block}

Question-specific hints:
{hint_block}

Instructions:
- Understand if the current question is a follow-up to the previous conversation
- If it is a follow-up, refine the previous SQL instead of starting over when possible
- Preserve previous filters, grouping, and limits unless the user asks to change them
- Do not invent extra filters, date ranges, or conditions that were not requested
- If previous results are provided, use them to complete the query
- Use only available tables and columns
- Use correct PostgreSQL syntax
- Use aggregation if needed (SUM, COUNT, AVG, MAX, MIN)
- Use LIMIT when the user asks for top results
- Prefer the simplest valid query
- Only join tables when the question actually needs data from more than one table
- Do not add JOIN, GROUP BY, ORDER BY, or LIMIT unless the question needs them
- ONLY return a single SQL query
- Do NOT explain anything
- Do NOT include text like 'Here is the query'
- Do NOT use DELETE, UPDATE, DROP, INSERT, ALTER, TRUNCATE, or CREATE

SQL:
""".strip()

    response = ask_llm(prompt)
    return normalize_sql(response)


def fix_sql(
    sql_query,
    error_message,
    question,
    schema_context,
    memory_context="",
    original_question=None,
    previous_results_context="",
):
    hint_question = original_question or question
    hints = build_question_hints(hint_question, memory_context)
    hint_block = "\n".join(f"- {hint}" for hint in hints) or "- No extra hints"
    memory_block = memory_context or "No previous conversation."
    prior_results_block = previous_results_context or "No previous step results."
    original_question_block = original_question or question

    prompt = f"""
You are fixing a PostgreSQL SELECT query.

Current date:
{date.today().isoformat()}

Previous conversation:
{memory_block}

Original user question:
{original_question_block}

Current step or question:
{question}

Schema:
{schema_context}

Previous step results:
{prior_results_block}

Question-specific hints:
{hint_block}

The following SQL query has an error:
{sql_query}

Error:
{error_message}

Instructions:
- Fix the SQL query using only the available tables and columns
- If this is a follow-up question, preserve the intent of the prior conversation
- Do not invent extra filters, date ranges, or conditions that were not requested
- If previous results are provided, use them to complete the query
- Keep it as a single SELECT query
- Return ONLY the corrected SQL
- Do NOT include explanations or markdown
""".strip()

    response = ask_llm(prompt)
    return normalize_sql(response)
