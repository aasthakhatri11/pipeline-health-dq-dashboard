import streamlit as st
import pandas as pd
import snowflake.connector
from datetime import datetime, timezone

st.set_page_config(page_title="Pipeline Health & Data Quality Dashboard", layout="wide")
st.title("Pipeline Health & Data Quality Dashboard")
st.caption("Capstone #30 - Databricks & Snowflake - Bronze / Silver / Gold medallion pipeline")


@st.cache_resource(ttl=3600)  # reconnect hourly so a stale session never breaks the app
def get_connection():
    s = st.secrets["snowflake"]
    extra = {"role": s["role"]} if "role" in s else {}  # optional read-only role
    return snowflake.connector.connect(
        user=s["user"], password=s["password"], account=s["account"],
        warehouse=s["warehouse"], database=s["database"], schema=s["schema"], **extra,
    )


@st.cache_data(ttl=600, show_spinner=False)
def run_query(query: str) -> pd.DataFrame:
    for attempt in (1, 2):  # retry once with a fresh connection
        try:
            cur = get_connection().cursor()
            try:
                cur.execute(query)
                cols = [c[0] for c in cur.description]
                return pd.DataFrame(cur.fetchall(), columns=cols)
            finally:
                cur.close()
        except snowflake.connector.errors.Error:
            get_connection.clear()
            if attempt == 2:
                raise


def to_num(df, cols):
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def color_cells(styler, fn):
    return styler.map(fn) if hasattr(styler, "map") else styler.applymap(fn)


with st.sidebar:
    st.header("Controls")
    if st.button("Refresh data now"):
        st.cache_data.clear()
        st.rerun()
    st.caption("Results are cached for 10 minutes.")

# ---------- Load data ----------
dq = to_num(run_query("SELECT * FROM gold_dq_results"), ["ROWS_CHECKED", "ROWS_FAILED", "FAIL_RATE"])
freshness = to_num(run_query("SELECT * FROM gold_freshness"), ["HOURS_BEHIND", "EXPECTED_INTERVAL_HOURS"])

# Optional lookup (load_id -> file name and business date). The app still works without it.
try:
    man = run_query("SELECT * FROM gold_load_manifest")
except Exception:
    man = None
if man is not None and not man.empty:
    dq = dq.merge(man[["LOAD_ID", "LOAD_LABEL", "BUSINESS_DATE"]], on="LOAD_ID", how="left")
fallback = dq["TABLE_NAME"] + " / " + dq["LOAD_ID"].str[:8]
dq["LOAD_LABEL"] = (dq["LOAD_LABEL"] if "LOAD_LABEL" in dq else pd.Series(index=dq.index, dtype=object)).fillna(fallback)
dq["BUSINESS_DATE"] = (dq["BUSINESS_DATE"] if "BUSINESS_DATE" in dq else pd.Series(index=dq.index, dtype=object)).fillna("").astype(str)

# ---------- Top-level metrics ----------
failing = dq["RULE_STATUS"] == "fail"
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Rule checks run", len(dq))
c2.metric("Rule checks failing", int(failing.sum()))
c3.metric("Rows rejected (row-scope)", int(dq.loc[failing & (dq["RULE_SCOPE"] == "row"), "ROWS_FAILED"].sum()))
c4.metric("Files failing (file-scope)", int((failing & (dq["RULE_SCOPE"] == "file")).sum()))
c5.metric("Tables stale", int((freshness["FRESHNESS_STATUS"] == "fail").sum()))
st.divider()

# ---------- Question 1 ----------
st.subheader("1. Rows rejected per load, per rule, per day")
st.caption("Row-scope rules count rejected rows. File-scope rules (R6, R7) count 1 when the whole file fails.")
table = st.radio("Table", sorted(dq["TABLE_NAME"].unique()), horizontal=True)
sub = dq[dq["TABLE_NAME"] == table]
tab_heat, tab_detail = st.tabs(["Heatmap: load x rule", "Detail table"])
with tab_heat:
    pivot = sub.pivot_table(index=["BUSINESS_DATE", "LOAD_LABEL"], columns="RULE_ID",
                            values="ROWS_FAILED", aggfunc="sum")
    hi = lambda v: "background-color:#f8b4b4;font-weight:600" if pd.notna(v) and v > 0 else "color:#9aa0a6"
    st.dataframe(color_cells(pivot.style, hi), use_container_width=True)
with tab_detail:
    only_fail = st.toggle("Show only failing rules", value=True)
    show = sub[sub["RULE_STATUS"] == "fail"] if only_fail else sub
    st.dataframe(
        show[["BUSINESS_DATE", "LOAD_LABEL", "RULE_ID", "RULE_NAME", "RULE_SCOPE",
              "ROWS_CHECKED", "ROWS_FAILED", "FAIL_RATE", "RULE_STATUS"]]
        .sort_values(["BUSINESS_DATE", "ROWS_FAILED"], ascending=[True, False]),
        use_container_width=True, hide_index=True)
st.divider()

# ---------- Question 2 ----------
st.subheader("2. How stale is each table?")
st.caption("Measured against the data's own latest business date, not wall-clock ingest time.")
for _, r in freshness.iterrows():
    icon = "🟢" if r["FRESHNESS_STATUS"] == "pass" else "🔴"
    st.write(f"{icon} **{r['TABLE_NAME']}** - {int(r['HOURS_BEHIND'])} hours behind "
             f"(expected <= {int(r['EXPECTED_INTERVAL_HOURS'])}h) - **{str(r['FRESHNESS_STATUS']).upper()}**")
st.bar_chart(freshness.set_index("TABLE_NAME")[["HOURS_BEHIND", "EXPECTED_INTERVAL_HOURS"]])
st.dataframe(freshness, use_container_width=True, hide_index=True)
st.divider()

# ---------- Question 3 ----------
st.subheader("3. Which rule fires most - is it the data, or the rule?")
st.caption("A rule that fires all at once on one load describes one broken file. "
           "A rule that fires a little on every load describes the data itself.")
q3 = to_num(run_query("""
    SELECT rule_id, rule_name,
           SUM(rows_failed) AS total_failed,
           COUNT_IF(rows_failed > 0) AS loads_fired,
           COUNT(*) AS loads_checked,
           MAX(rows_failed) AS worst_single_load,
           ROUND(MAX(rows_failed) / NULLIF(SUM(rows_failed), 0), 3) AS concentration,
           ROUND(SUM(rows_failed) / NULLIF(SUM(rows_checked), 0), 5) AS fail_rate
    FROM gold_dq_results
    WHERE rule_scope = 'row' AND table_name = 'orders'
    GROUP BY 1, 2 ORDER BY 3 DESC"""),
    ["TOTAL_FAILED", "LOADS_FIRED", "LOADS_CHECKED", "WORST_SINGLE_LOAD", "CONCENTRATION", "FAIL_RATE"])


def verdict(r):
    if r["TOTAL_FAILED"] == 0:
        return "never fired"
    return "one broken file" if r["LOADS_FIRED"] == 1 else "spread across loads: check the rule"


q3["VERDICT"] = q3.apply(verdict, axis=1)
q3["CONCENTRATION"] = q3["CONCENTRATION"].apply(lambda v: "-" if pd.isna(v) else f"{v:.3f}")
st.bar_chart(q3.set_index("RULE_NAME")["TOTAL_FAILED"])
st.dataframe(q3, use_container_width=True, hide_index=True)
st.caption("Concentration = worst single load / total failures. 1.000 means every failure came from one load.")

st.divider()
st.caption(f"Fetched {datetime.now(timezone.utc):%Y-%m-%d %H:%M UTC} from Snowflake.")
