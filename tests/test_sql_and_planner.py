import unittest

from planner import parse_plan
from sql_generator import clean_sql, normalize_sql


class SQLAndPlannerTests(unittest.TestCase):
    def test_clean_sql_extracts_query_from_fenced_block(self):
        response = "```sql\nSELECT * FROM orders;\n```"
        self.assertEqual(clean_sql(response), "SELECT * FROM orders;")

    def test_normalize_sql_rejects_non_select_queries(self):
        with self.assertRaises(ValueError):
            normalize_sql("DELETE FROM orders;")

    def test_parse_plan_filters_out_sql_lines(self):
        raw_plan = """
        1. Find the top country
        2. SELECT * FROM orders;
        3. Find the top products in that country
        """
        self.assertEqual(
            parse_plan(raw_plan),
            ["Find the top country", "Find the top products in that country"],
        )


if __name__ == "__main__":
    unittest.main()
