import sys

from main_logic import run_chat, run_data_copilot, run_schema_search


def main():
    args = sys.argv[1:]

    if args and args[0] == "schema":
        question = " ".join(args[1:]).strip() or "Which table contains revenue?"
        results = run_schema_search(question)
        print("Relevant schema:")
        for result in results:
            print(result)
        return

    if args:
        question = " ".join(args).strip()
        run_data_copilot(question)
        return

    run_chat()


if __name__ == "__main__":
    main()
