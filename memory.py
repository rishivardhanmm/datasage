class Memory:
    def __init__(self):
        self.history = []

    def add(self, question, sql):
        self.history.append(
            {
                "question": question,
                "sql": sql,
            }
        )

    def get_context(self):
        context = ""
        for item in self.history[-3:]:
            context += f"Q: {item['question']}\nSQL: {item['sql']}\n"
        return context.strip()

    def get_recent_questions(self):
        return [item["question"] for item in self.history[-3:]]

    def get_last_entry(self):
        if not self.history:
            return None
        return self.history[-1]
