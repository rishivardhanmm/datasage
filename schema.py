from db import get_connection


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


def get_schema():
    schema = {}

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                ORDER BY table_name;
                """
            )
            tables = [row[0] for row in cur.fetchall()]

            for table_name in tables:
                cur.execute(
                    """
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_schema = 'public' AND table_name = %s
                    ORDER BY ordinal_position;
                    """,
                    (table_name,),
                )
                schema[table_name] = cur.fetchall()

    return schema


def schema_relationship_hints(schema):
    hints = []
    seen = set()
    table_columns = {table: [column for column, _dtype in columns] for table, columns in schema.items()}
    table_names = list(schema.keys())

    for index, left_table in enumerate(table_names):
        for right_table in table_names[index + 1 :]:
            shared_id_columns = [
                column
                for column in table_columns[left_table]
                if column in table_columns[right_table] and column.endswith("_id")
            ]
            for column in shared_id_columns:
                hint = (
                    f"Relationship hint: {left_table}.{column} can join to "
                    f"{right_table}.{column}."
                )
                if hint not in seen:
                    seen.add(hint)
                    hints.append(hint)

    existing_tables = set(schema.keys())
    for table, columns in schema.items():
        for column, _dtype in columns:
            if column.endswith("_name"):
                entity = column[:-5]
                attribute_hint = f"Attribute hint: {entity} is stored directly on {table}.{column}."
                if attribute_hint not in seen:
                    seen.add(attribute_hint)
                    hints.append(attribute_hint)

                singular_entity = singularize(entity)
                plural_entity = pluralize(singular_entity)
                if singular_entity not in existing_tables and plural_entity not in existing_tables:
                    missing_table_hint = (
                        f"Schema hint: there is no separate table named {plural_entity}; "
                        f"use {table}.{column}."
                    )
                    if missing_table_hint not in seen:
                        seen.add(missing_table_hint)
                        hints.append(missing_table_hint)

    return hints


def schema_to_text(schema):
    descriptions = []

    for table, columns in schema.items():
        col_desc = ", ".join(f"{col} ({dtype})" for col, dtype in columns)
        descriptions.append(f"Table {table} has columns: {col_desc}")

    descriptions.extend(schema_relationship_hints(schema))
    return descriptions
