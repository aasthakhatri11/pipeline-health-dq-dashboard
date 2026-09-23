import streamlit as st
import pandas as pd
import snowflake.connector

st.set_page_config(page_title="Pipeline Health & Data Quality Dashboard", layout="wide")

st.title("Pipeline Health & Data Quality Dashboard")
st.caption("Capstone #30 — Databricks & Snowflake — Bronze / Silver / Gold medallion pipeline")


@st.cache_resource
def get_connection():
    return snowflake.connector.connect(
        user=st.secrets["snowflake"]["user"],
        password=st.secrets["snowflake"]["password"],
        account=st.secrets["snowflake"]["account"],
        warehouse=st.secrets["snowflake"]["warehouse"],
        database=st.secrets["snowflake"]["database"],
        schema=st.secrets["snowflake"]["schema"],
    )


@st.cache_data(ttl=600)
def run_query(query: str) -> pd.DataFrame:
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(query)
        cols = [c[0] for c in cur.description]
        rows = cur.fetchall()
        return pd.DataFrame(rows, columns=cols)
    finally:
        cur.close()


# ---------- Load data ----------
dq = run_query("SELECT * FROM gold_dq_results")
freshness = run_query("SELECT * FROM gold_freshness")

# ---------- Top-level metrics ----------
col1, col2, col3, col4 = st.columns(4)
col1.metric("Total rules checked", len(dq))
col2.metric("Rules failing", int((dq["RULE_STATUS"] == "fail").sum()))
col3.metric("Total rows failed", int(dq["ROWS_FAILED"].sum()))
col4.metric(
    "Tables stale",
    int((freshness["FRESHNESS_STATUS"] == "fail").sum()),
)

st.divider()

# ---------- Question 1: Rows rejected per load, per rule, per day ----------
st.subheader("1. Rows rejected per load, per rule")
st.caption("Every load × rule combination, showing where rejects concentrated")

failing_only = st.toggle("Show only failing rules", value=True)
display_dq = dq[dq["RULE_STATUS"] == "fail"] if failing_only else dq

st.dataframe(
    display_dq[
        ["TABLE_NAME", "LOAD_ID", "RULE_ID", "RULE_NAME", "RULE_SCOPE",
         "ROWS_CHECKED", "ROWS_FAILED", "FAIL_RATE", "RULE_STATUS"]
    ].sort_values("ROWS_FAILED", ascending=False),
    use_container_width=True,
    hide_index=True,
)

st.divider()

# ---------- Question 2: Freshness ----------
st.subheader("2. How stale is each table?")
st.caption("Measured against the data's own latest business date, not wall-clock ingest time")

for _, row in freshness.iterrows():
    status_color = "🟢" if row["FRESHNESS_STATUS"] == "pass" else "🔴"
    st.write(
        f"{status_color} **{row['TABLE_NAME']}** — "
        f"{row['HOURS_BEHIND']} hours behind "
        f"(expected ≤ {row['EXPECTED_INTERVAL_HOURS']}h) — "
        f"**{row['FRESHNESS_STATUS'].upper()}**"
    )

st.dataframe(freshness, use_container_width=True, hide_index=True)

st.divider()

# ---------- Question 3: Which rule fires most, and is it the data or the rule? ----------
st.subheader("3. Which rule fires most — is it the data, or the rule?")
st.caption("A rule concentrated in one load is describing one broken file. A rule spread across many loads is describing the data itself.")

row_rules = dq[(dq["RULE_SCOPE"] == "row") & (dq["TABLE_NAME"] == "orders")]

summary = (
    row_rules.groupby(["RULE_ID", "RULE_NAME"])
    .agg(
        total_failed=("ROWS_FAILED", "sum"),
        loads_fired=("ROWS_FAILED", lambda x: (x > 0).sum()),
        loads_checked=("ROWS_FAILED", "count"),
        worst_single_load=("ROWS_FAILED", "max"),
    )
    .reset_index()
)
summary["concentration"] = pd.to_numeric(
    summary["worst_single_load"] / summary["total_failed"].replace(0, pd.NA),
    errors="coerce"
).round(3)
summary["fail_rate"] = pd.to_numeric(
    summary["total_failed"] / row_rules.groupby(["RULE_ID", "RULE_NAME"])["ROWS_CHECKED"].sum().values,
    errors="coerce"
).round(5)

summary = summary.sort_values("total_failed", ascending=False)

st.bar_chart(summary.set_index("RULE_NAME")["total_failed"])
st.dataframe(summary, use_container_width=True, hide_index=True)

st.caption(
    "Concentration = 1.0 means every failure for that rule came from a single load — "
    "a broken file, not a systemic data problem."
)
