import re
from numbers import Number

from llm import ask_llm

BANNED_AI_PHRASES = (
    "double",
    "triple",
    "times more",
    "times less",
    "opportun",
    "recommend",
    "could",
    "should",
    "suggest",
    "potential",
    "may indicate",
    "likely",
    "demand",
    "market presence",
    "market share",
    "strong market",
    "focus on",
)


def is_numeric(value):
    return isinstance(value, Number) and not isinstance(value, bool)


def to_float(value):
    if is_numeric(value):
        return float(value)
    return None


def format_value(value):
    numeric = to_float(value)
    if numeric is None:
        return str(value)

    if numeric.is_integer():
        return f"{int(numeric):,}"

    return f"{numeric:,.2f}".rstrip("0").rstrip(".")


def format_percent(value):
    text = f"{value:.1f}".rstrip("0").rstrip(".")
    return f"{text}%"


def detect_type(results):
    if not results:
        return "empty"

    first_row = results[0]

    if len(first_row) == 1:
        if len(results) == 1 and is_numeric(first_row[0]):
            return "single_value"
        return "single_column"

    if len(first_row) == 2 and all(len(row) >= 2 and is_numeric(row[1]) for row in results):
        return "category_value"

    return "table"


def sort_category_values(results):
    return sorted(results, key=lambda row: float(row[1]), reverse=True)


def generate_basic_insights(results, data_type):
    insights = []

    if data_type == "single_value":
        insights.append(f"Total value is {format_value(results[0][0])}.")

    elif data_type == "category_value":
        sorted_data = sort_category_values(results)
        total = sum(float(row[1]) for row in sorted_data)
        top = sorted_data[0]
        insights.append(f"{top[0]} is the top contributor with {format_value(top[1])}.")

        if total > 0 and len(sorted_data) > 1:
            insights.append(
                f"{top[0]} contributes {format_percent((float(top[1]) / total) * 100)} of the total."
            )

        if len(sorted_data) > 1:
            second = sorted_data[1]
            insights.append(
                f"{second[0]} is the second contributor with {format_value(second[1])}."
            )
            if total > 0:
                top_two_share = ((float(top[1]) + float(second[1])) / total) * 100
                if len(sorted_data) > 2 and top_two_share >= 70:
                    insights.append(
                        f"The top 2 categories account for {format_percent(top_two_share)} of the total."
                    )

    return insights


def detect_anomalies(results):
    if detect_type(results) != "category_value":
        return []

    values = [float(row[1]) for row in results if is_numeric(row[1])]
    if len(values) < 2:
        return []

    average = sum(values) / len(values)
    anomalies = []

    for label, value in results:
        numeric = to_float(value)
        if numeric is not None and numeric >= average * 1.4:
            anomalies.append(f"{label} is significantly higher than average.")

    return anomalies


def generate_ai_insights(
    question,
    columns,
    results,
    data_type,
    basic_insights=None,
    anomalies=None,
    supporting_context="",
):
    if not results:
        return ""

    if data_type == "category_value" and len(results) == 1:
        return ""

    basic_insights = basic_insights or []
    anomalies = anomalies or []
    preview = results[:10]
    columns_block = columns or []
    supporting_lines = [line for line in supporting_context.splitlines() if line.strip()]
    fact_lines = list(basic_insights) + list(anomalies)
    if not fact_lines:
        fact_lines.extend(
            [
                f"Columns: {columns_block}",
                f"Result preview: {preview}",
            ]
        )
    fact_block = "\n".join(f"- {item}" for item in fact_lines)
    supporting_block = "\n".join(f"- {line}" for line in supporting_lines) or "- None"

    prompt = f"""
You are a senior data analyst.

User question:
{question}

Data type:
{data_type}

Columns:
{columns_block}

Primary result:
{preview}

Facts you may use:
{fact_block}

Supporting analysis:
{supporting_block}

Write 2 to 4 short sentences of deeper insights.

Rules:
- Use only the listed facts above
- Highlight concentration, comparisons, or unusual patterns when the data supports them
- Do not invent causes, recommendations, or missing dimensions
- Do not perform new calculations
- Do not introduce a number, ratio, percentage, or ranking unless it already appears in the listed facts
- If the data is limited, keep the insight modest and literal
- Do not repeat the rule-based insights word-for-word
- Do not number the output
""".strip()

    cleaned = clean_ai_insights(ask_llm(prompt))
    if not is_safe_ai_insight(cleaned):
        return build_fallback_ai_insights(basic_insights, anomalies)
    return cleaned


def clean_ai_insights(text):
    lines = [line.strip() for line in text.strip().splitlines() if line.strip()]
    cleaned_lines = [
        re.sub(r"^\s*(?:\d+[\).\s-]+|[-*]\s+)", "", line).strip()
        for line in lines
    ]
    return "\n".join(line for line in cleaned_lines if line)


def is_safe_ai_insight(text):
    lowered = text.lower()
    return not any(phrase in lowered for phrase in BANNED_AI_PHRASES)


def build_fallback_ai_insights(basic_insights, anomalies):
    candidates = []

    for insight in basic_insights:
        lowered = insight.lower()
        if "contributes" in lowered or "concentrated" in lowered or "account for" in lowered:
            candidates.append(insight)

    for anomaly in anomalies:
        if "significantly higher than average" in anomaly.lower():
            candidates.append(anomaly)

    if not candidates:
        candidates = list(basic_insights) + list(anomalies)

    return " ".join(candidates[:2]).strip()
