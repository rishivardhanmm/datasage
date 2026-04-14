import re

from llm import ask_llm


PLANNING_TRIGGERS = (
    " and ",
    " then ",
    " after ",
    " based on ",
    " contributing ",
    " contribution ",
    " compared to ",
    " versus ",
    " vs ",
    " for that ",
    " for the top ",
    " that country",
    " that product",
    " using that ",
)

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


def create_plan(question, memory_context=""):
    conversation = memory_context or "No previous conversation."

    prompt = f"""
You are a data-analysis planner for a PostgreSQL agent.

Previous conversation:
{conversation}

Question:
{question}

Break the question into the minimum logical SQL steps needed to answer it.

Rules:
- For a simple question, return exactly 1 step
- For a complex question, return 2 to 4 steps
- Each step must be executable with SQL
- Later steps may depend on earlier step results
- Return only a plain numbered list of steps, one step per line
- Each step must be short natural language, not SQL
- Do not include code fences, inline code, example queries, explanations, or sub-bullets
- Do not include explanations
""".strip()

    return ask_llm(prompt)


def should_plan(question):
    lowered = f" {question.strip().lower()} "

    if len(question.split()) < 8:
        return False

    if any(keyword in lowered for keyword in RELATIONSHIP_KEYWORDS):
        return False

    if any(trigger in lowered for trigger in PLANNING_TRIGGERS):
        return True

    comparative_terms = ("highest", "lowest", "top", "bottom", "best", "worst")
    return sum(term in lowered for term in comparative_terms) >= 2


def looks_like_sql(text):
    lowered = text.strip().lower()
    sql_prefixes = (
        "select ",
        "with ",
        "from ",
        "where ",
        "group by ",
        "order by ",
        "limit ",
        "join ",
        "inner join ",
        "left join ",
        "right join ",
        "having ",
    )
    return lowered.startswith(sql_prefixes) or lowered.endswith(";")


def parse_plan(plan_text):
    stripped = re.sub(r"```.*?```", "", plan_text, flags=re.DOTALL).strip()
    if not stripped:
        return []

    steps = []
    numbered_items = re.findall(
        r"(?:^|\n)\s*\d+[\).:-]?\s*(.*?)(?=(?:\n\s*\d+[\).:-]?\s)|\Z)",
        stripped,
        flags=re.DOTALL,
    )

    for item in numbered_items:
        cleaned = item.split("`", 1)[0]
        cleaned = re.sub(r"\s*[:\-]\s*(?:select|with)\b.*", "", cleaned, flags=re.IGNORECASE)
        cleaned = " ".join(cleaned.split()).rstrip(":.-").strip()
        if cleaned and not looks_like_sql(cleaned):
            steps.append(cleaned)

    if steps:
        return steps

    for raw_line in stripped.splitlines():
        line = re.sub(r"^\s*(?:[-*]|\d+[\).:-]?)\s*", "", raw_line).strip()
        line = line.split("`", 1)[0].strip()
        line = re.sub(r"\s*[:\-]\s*(?:select|with)\b.*", "", line, flags=re.IGNORECASE)
        if line and not looks_like_sql(line) and not line.startswith("```"):
            steps.append(line.rstrip(":.-").strip())

    return steps
