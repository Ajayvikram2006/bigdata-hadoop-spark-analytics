# VIVA NOTES - Streamlit dashboard: reads the SAVED files in metrics/ (ml_rf_evaluation, ml_kmeans_anomaly, ml_drift_detection) with regexes - not live data.
# Run: python -m streamlit run src/python/dashboard.py --server.port 8501   (docs/RUNBOOK.md section E12).
import streamlit as st
import pandas as pd
import os
import re

# ==============================================================================
# Phase 18 - System Dashboard (Streamlit)
# Reads saved metrics files from /metrics; it does not query live data.
# ==============================================================================

st.set_page_config(page_title="Hadoop Analytics Dashboard", layout="wide", page_icon="📊")

st.markdown("""
    <style>
    .block-container { padding-top: 2rem; padding-bottom: 2rem; max-width: 1200px; }
    .metric-card {
        background-color: #1E2127; border-radius: 10px; padding: 16px 20px;
        margin: 8px 0; border-left: 5px solid #00E676; box-shadow: 0 4px 6px rgba(0,0,0,0.3);
    }
    .metric-card h4 { margin: 0 0 6px 0; color: #B0B8C4; font-size: 0.95rem; font-weight: 500; }
    .metric-card h2 { margin: 0; color: #FFFFFF; }
    .metric-card-warning { border-left: 5px solid #FF3D00; }
    .banner {
        background-color: #263238; border: 1px solid #FFB300; color: #FFE082;
        border-radius: 8px; padding: 10px 16px; margin-bottom: 16px; font-weight: 600;
    }
    </style>
""", unsafe_allow_html=True)

METRICS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'metrics')


def read_metrics(name):
    path = os.path.join(METRICS_DIR, name)
    if not os.path.exists(path):
        return None
    with open(path, 'r', encoding='utf-8-sig', errors='replace') as f:
        return f.read()


def grab(text, pattern):
    m = re.search(pattern, text)
    return float(m.group(1)) if m else None


def card(title, value, warning=False):
    cls = "metric-card metric-card-warning" if warning else "metric-card"
    st.markdown(f'<div class="{cls}"><h4>{title}</h4><h2>{value}</h2></div>', unsafe_allow_html=True)


st.title("📊 Distributed Analytics & ML Dashboard")
st.caption("CMU Hadoop cluster: job failure prediction, HDFS anomaly detection and drift monitoring.")
st.markdown('<div class="banner">Reads saved metrics, not live data.</div>', unsafe_allow_html=True)

# ------------------------------------------------------------------------------
# 1. Random Forest
# ------------------------------------------------------------------------------
st.header("🎯 Job Failure Prediction (Random Forest)")
rf = read_metrics('ml_rf_evaluation.txt')
if rf:
    vals = {
        "Accuracy": grab(rf, r'Accuracy\s+:\s+([\d\.]+)'),
        "Weighted Precision": grab(rf, r'Weighted Precision\s+:\s+([\d\.]+)'),
        "Weighted Recall": grab(rf, r'Weighted Recall\s+:\s+([\d\.]+)'),
        "Weighted F1": grab(rf, r'Weighted F1-Score\s+:\s+([\d\.]+)'),
        "AUC (ROC)": grab(rf, r'Area Under ROC\s+:\s+([\d\.]+)'),
        "FAILED-class Recall": grab(rf, r'Recall\s+\(FAILED\)\s+:\s+([\d\.]+)'),
    }
    cols = st.columns(len(vals))
    for col, (name, v) in zip(cols, vals.items()):
        with col:
            card(name, f"{v:.4f}" if v is not None else "N/A", warning=(name == "FAILED-class Recall"))
    chart_df = pd.DataFrame({"score": {k: v for k, v in vals.items() if v is not None}})
    st.bar_chart(chart_df, y_label="score (0-1)")
    st.caption("FAILED-class recall is low: the model misses most failed jobs despite high overall accuracy.")
else:
    st.warning("Random Forest metrics not found.")

st.markdown("---")

# ------------------------------------------------------------------------------
# 2. K-Means anomalies
# ------------------------------------------------------------------------------
st.header("🚨 HDFS Anomaly Detection (K-Means)")
km = read_metrics('ml_kmeans_anomaly.txt')
if km:
    anomalies = re.findall(r'\d+\.\s+(\w+)\s+\((.*?)\)\s+\|\s+Score:\s+([\d\.]+)\s+\|\s+Events:\s+([\d,]+)', km)
    if anomalies:
        df = pd.DataFrame(
            [(f"{n} {t}", float(s), int(e.replace(',', ''))) for n, t, s, e in anomalies],
            columns=["window", "anomaly score", "events in 5 min"],
        )
        st.bar_chart(df.set_index("window")["anomaly score"], y_label="anomaly score")
        st.dataframe(df, hide_index=True, use_container_width=True)
    else:
        st.info("No anomalies listed in saved metrics.")
else:
    st.info("K-Means metrics not found.")

st.markdown("---")

# ------------------------------------------------------------------------------
# 3. Drift
# ------------------------------------------------------------------------------
st.header("📈 Data Drift (Z-score vs 1.0 threshold)")
dr = read_metrics('ml_drift_detection.txt')
if dr:
    drifts = re.findall(r'(\w+)\s+\|\s+([\d\.]+)\s+\|\s+([\d\.]+)\s+\|\s+([\d\.]+)\s+\|\s+(STABLE|[^\n]*DRIFT[^\n]*)', dr)
    if drifts:
        ddf = pd.DataFrame(
            [(f, float(z), s.strip()) for f, _, _, z, s in drifts],
            columns=["feature", "z-score", "status"],
        )
        ddf["threshold"] = 1.0
        st.line_chart(ddf.set_index("feature")[["z-score", "threshold"]], y_label="z-score")
        cols = st.columns(len(ddf))
        for col, row in zip(cols, ddf.itertuples()):
            with col:
                card(row.feature, f"z={row[2]:.2f} · {row.status}", warning=(row.status != "STABLE"))
    else:
        st.info("No drift rows found in saved metrics.")
else:
    st.info("Drift metrics not found.")

st.markdown("---")

# ------------------------------------------------------------------------------
# 4. Demo alert
# ------------------------------------------------------------------------------
st.header("🔔 Alert Preview")
st.warning("DEMO / SIMULATED ALERT: HIGH FAILURE RISK on a sample job (not a real event). "
           "Real alerts come from `src/python/alert_monitor.py`.")

st.caption("CMU Distributed Analytics Platform · Final Project")
