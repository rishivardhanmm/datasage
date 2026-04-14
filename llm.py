import os

import requests


def ask_llm(prompt):
    response = requests.post(
        os.getenv("OLLAMA_URL", "http://127.0.0.1:11434/api/generate"),
        json={
            "model": os.getenv("OLLAMA_MODEL", "mistral"),
            "prompt": prompt,
            "stream": False,
        },
        timeout=600,
    )
    response.raise_for_status()
    return response.json()["response"]

