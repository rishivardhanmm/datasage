import matplotlib.pyplot as plt
import pandas as pd
import streamlit as st

from main_logic import process_question
from memory import Memory
from visualize import plot_results


st.set_page_config(page_title="DataSage", page_icon="DS", layout="wide")

st.markdown(
    """
    <style>
    :root {
        --ds-ink: #12343b;
        --ds-muted: #48676e;
        --ds-surface: rgba(255, 255, 255, 0.68);
        --ds-surface-strong: rgba(255, 255, 255, 0.88);
        --ds-border: rgba(18, 52, 59, 0.12);
    }
    .stApp {
        color: var(--ds-ink);
        background:
            radial-gradient(circle at top left, rgba(31, 111, 139, 0.14), transparent 28%),
            radial-gradient(circle at top right, rgba(244, 162, 97, 0.12), transparent 24%),
            linear-gradient(180deg, #f7fbfc 0%, #eef4f6 100%);
    }
    .stApp p,
    .stApp li,
    .stApp label,
    .stApp .stMarkdown,
    .stApp .stText,
    .stApp .stSubheader,
    .stApp [data-testid="stMarkdownContainer"],
    .stApp [data-testid="stChatMessageContent"],
    .stApp [data-testid="stExpander"] summary,
    .stApp [data-testid="stSidebar"] * {
        color: var(--ds-ink);
    }
    .stApp [data-testid="stCaptionContainer"],
    .stApp [data-testid="stCaptionContainer"] p,
    .stApp small {
        color: var(--ds-muted) !important;
    }
    .stApp [data-testid="stSidebar"] {
        background: linear-gradient(180deg, var(--ds-surface-strong) 0%, rgba(240, 246, 248, 0.92) 100%);
        border-right: 1px solid var(--ds-border);
    }
    .stApp [data-testid="stExpander"] {
        background: var(--ds-surface);
        border: 1px solid var(--ds-border);
        border-radius: 16px;
    }
    .stApp [data-testid="stCodeBlock"] {
        border-radius: 14px;
    }
    .stApp [data-testid="stChatInput"] textarea,
    .stApp [data-testid="stChatInput"] input {
        color: var(--ds-ink) !important;
        background: var(--ds-surface-strong) !important;
        border: 1px solid var(--ds-border) !important;
    }
    .stApp [data-testid="stChatInput"] textarea::placeholder,
    .stApp [data-testid="stChatInput"] input::placeholder {
        color: var(--ds-muted) !important;
        opacity: 1;
    }
    .stApp button {
        color: #f7fbfc;
        background: #12343b;
        border: 1px solid rgba(18, 52, 59, 0.32);
    }
    .stApp button:hover {
        color: #f7fbfc;
        background: #1f6f8b;
        border-color: #1f6f8b;
    }
    .hero {
        padding: 1.2rem 1.4rem;
        border-radius: 20px;
        background: linear-gradient(135deg, #12343b 0%, #1f6f8b 60%, #4fa3b8 100%);
        color: #f7fbfc !important;
        box-shadow: 0 18px 45px rgba(18, 52, 59, 0.18);
        margin-bottom: 1rem;
    }
    .hero h1 {
        margin: 0;
        font-size: 2.2rem;
        letter-spacing: -0.03em;
        color: #f7fbfc !important;
    }
    .hero p {
        margin: 0.45rem 0 0;
        max-width: 48rem;
        opacity: 0.92;
        color: #f7fbfc !important;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

st.markdown(
    """
    <div class="hero">
        <h1>DataSage</h1>
        <p>Ask questions about your PostgreSQL data, get SQL you can inspect, see chartable results, and follow up conversationally.</p>
    </div>
    """,
    unsafe_allow_html=True,
)

st.caption(
    "The first question in a fresh session can take 30-90 seconds while the "
    "embedding model and local Ollama model warm up."
)

if "memory" not in st.session_state:
    st.session_state.memory = Memory()

if "conversation" not in st.session_state:
    st.session_state.conversation = []


def reset_chat():
    st.session_state.memory = Memory()
    st.session_state.conversation = []


def render_response(response):
    if response.get("error"):
        st.error(response["error"])
        if response.get("plan_steps"):
            st.write("### Plan")
            for index, step in enumerate(response["plan_steps"], start=1):
                st.write(f"{index}. {step}")

        with st.expander("Debug details", expanded=True):
            for step in response.get("executed_steps", []):
                st.write(f"Step {step['index']}: {step['step']}")
                st.code(step["sql_query"], language="sql")
                if step["error"]:
                    st.error(step["error"])
                elif step["results"]:
                    if step["columns"]:
                        st.dataframe(
                            pd.DataFrame(step["results"], columns=step["columns"]),
                            width="stretch",
                        )
                    else:
                        st.write(step["results"])
        return

    if response["results"]:
        st.caption(f"{len(response['results'])} row(s) returned")
    else:
        st.caption("No rows returned")

    st.write("### Answer")
    st.write(response["answer"])

    if response.get("basic_insights"):
        st.write("### Insights")
        for insight in response["basic_insights"]:
            st.write(f"- {insight}")

    if response.get("anomalies"):
        st.write("### Anomalies")
        for anomaly in response["anomalies"]:
            st.warning(anomaly)

    if response.get("ai_insights"):
        st.write("### Deeper insights")
        st.write(response["ai_insights"])

    figure = plot_results(response["results"], title=response["question"], show=False)
    if figure is not None:
        st.pyplot(figure, width="stretch")
        plt.close(figure)

    st.write("### Results")
    if response["results"]:
        if response["columns"]:
            dataframe = pd.DataFrame(response["results"], columns=response["columns"])
            st.dataframe(dataframe, width="stretch")
        else:
            st.write(response["results"])
    else:
        st.info("The query ran successfully but returned no matching rows.")

    with st.expander("Query details"):
        if response.get("plan_steps"):
            st.write("Plan")
            for index, step in enumerate(response["plan_steps"], start=1):
                st.write(f"{index}. {step}")

        if response.get("executed_steps"):
            st.write("Executed steps")
            for step in response["executed_steps"]:
                st.write(f"Step {step['index']}: {step['step']}")
                st.code(step["sql_query"], language="sql")
                if step["error"]:
                    st.error(step["error"])
                elif step["results"]:
                    if step["columns"]:
                        st.dataframe(
                            pd.DataFrame(step["results"], columns=step["columns"]),
                            width="stretch",
                        )
                    else:
                        st.write(step["results"])
                else:
                    st.caption("No rows returned for this step")

        st.write("Final SQL")
        st.code(response["sql_query"], language="sql")
        st.write("Relevant schema")
        for schema_line in response["relevant_schema"]:
            st.write(f"- {schema_line}")

        if response.get("supporting_analyses"):
            st.write("Insight support")
            for analysis in response["supporting_analyses"]:
                st.write(analysis["title"])
                st.code(analysis["sql_query"], language="sql")
                if analysis["columns"]:
                    st.dataframe(
                        pd.DataFrame(analysis["results"], columns=analysis["columns"]),
                        width="stretch",
                    )
                else:
                    st.write(analysis["results"])


with st.sidebar:
    st.subheader("Conversation")
    st.write("Follow-up prompts work best when you keep asking in the same thread.")
    st.write("Try:")
    st.code(
        "Show top products\nNow only for UK\nOnly in February",
        language="text",
    )
    if st.button("Clear conversation", width="stretch"):
        reset_chat()
        st.rerun()


for response in st.session_state.conversation:
    with st.chat_message("user"):
        st.write(response["question"])
    with st.chat_message("assistant"):
        render_response(response)


prompt = st.chat_input("Ask your data")

if prompt:
    with st.chat_message("user"):
        st.write(prompt)

    with st.chat_message("assistant"):
        st.caption("Thinking through schema, SQL, results, and insights...")
        with st.spinner("Thinking through the schema, SQL, and result..."):
            response = process_question(prompt, memory=st.session_state.memory, show_chart=False)
        response.pop("chart_figure", None)
        render_response(response)

    st.session_state.conversation.append(response)
