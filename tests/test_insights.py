import unittest

from insights import detect_anomalies, detect_type, generate_basic_insights


class InsightsTests(unittest.TestCase):
    def test_detect_type_for_single_value(self):
        self.assertEqual(detect_type([(123.45,)]), "single_value")

    def test_detect_type_for_category_value(self):
        self.assertEqual(detect_type([("Laptop", 1200), ("Phone", 800)]), "category_value")

    def test_generate_basic_insights_for_category_values(self):
        insights = generate_basic_insights(
            [("Laptop", 1200), ("Phone", 800), ("Tablet", 500)],
            "category_value",
        )
        self.assertIn("Laptop is the top contributor with 1,200.", insights)
        self.assertIn("Phone is the second contributor with 800.", insights)

    def test_single_row_category_value_does_not_claim_total_share(self):
        insights = generate_basic_insights([("Laptop", 1200)], "category_value")
        self.assertEqual(insights, ["Laptop is the top contributor with 1,200."])

    def test_detect_anomalies_flags_high_values(self):
        anomalies = detect_anomalies([("A", 100), ("B", 60), ("C", 20)])
        self.assertIn("A is significantly higher than average.", anomalies)


if __name__ == "__main__":
    unittest.main()
