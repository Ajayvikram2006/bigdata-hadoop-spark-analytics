import streamlit as st
import os
import re

# ==============================================================================
# Phase 18 - System Dashboard (Streamlit)
# ==============================================================================

st.set_page_config(page_title="Hadoop Analytics Dashboard", layout="wide", page_icon="📊")

# Custom CSS for a premium dark mode UI
st.markdown("""
    <style>
    .main {
        background-color: #0E1117;
        color: #FAFAFA;
    }
    .metric-card {
        background-color: #1E2127;
        border-radius: 10px;
        padding: 20px;
        margin: 10px 0;
        border-left: 5px solid #00E676;
        box-shadow: 0 4px 6px rgba(0,0,0,0.3);
    }
    .metric-card-warning {
        border-left: 5px solid #FF3D00;
    }
    h1, h2, h3 {
        color: #FFFFFF;
    }
    </style>
""", unsafe_allow_html=True)

st.title("📊 Distributed Analytics & ML Dashboard")
st.markdown("Real-time telemetry, model drift, and predictive analytics for the CMU Hadoop Cluster.")

METRICS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'metrics')

# ------------------------------------------------------------------------------
# 1. Random Forest Predictive Model Health
# ------------------------------------------------------------------------------
st.header("🎯 Job Failure Prediction Model (Random Forest)")

rf_file = os.path.join(METRICS_DIR, 'ml_rf_evaluation.txt')
if os.path.exists(rf_file):
    with open(rf_file, 'r') as f:
        rf_text = f.read()
    
    auc_match = re.search(r'Area Under ROC\s+:\s+([\d\.]+)', rf_text)
    auc = auc_match.group(1) if auc_match else "N/A"
    
    acc_match = re.search(r'Accuracy\s+:\s+([\d\.]+)', rf_text)
    acc = acc_match.group(1) if acc_match else "N/A"

    col1, col2, col3 = st.columns(3)
    with col1:
        st.markdown(f"""
        <div class="metric-card">
            <h3>Model Accuracy</h3>
            <h2>{float(acc)*100:.2f}%</h2>
        </div>
        """, unsafe_allow_html=True)
    with col2:
        st.markdown(f"""
        <div class="metric-card">
            <h3>AUC (ROC)</h3>
            <h2>{float(auc):.4f}</h2>
        </div>
        """, unsafe_allow_html=True)
    with col3:
        st.markdown(f"""
        <div class="metric-card">
            <h3>Status</h3>
            <h2 style="color: #00E676;">ACTIVE</h2>
        </div>
        """, unsafe_allow_html=True)
else:
    st.warning("Random Forest metrics not found.")

st.markdown("---")

# ------------------------------------------------------------------------------
# 2. HDFS Anomaly Detection (K-Means)
# ------------------------------------------------------------------------------
st.header("🚨 HDFS Anomaly Detection (Metadata Storms)")

kmeans_file = os.path.join(METRICS_DIR, 'ml_kmeans_anomaly.txt')
if os.path.exists(kmeans_file):
    with open(kmeans_file, 'r') as f:
        k_text = f.read()
    
    anomalies = re.findall(r'\d+\.\s+(\w+)\s+\((.*?)\)\s+\|\s+Score:\s+([\d\.]+)\s+\|\s+Events:\s+([\d,]+)', k_text)
    
    if anomalies:
        for node, timestamp, score, events in anomalies:
            if float(score) > 3000:
                st.markdown(f"""
                <div class="metric-card metric-card-warning">
                    <h3 style="color: #FF3D00;">CRITICAL: Metadata Storm on {node}</h3>
                    <p><strong>Time:</strong> {timestamp}</p>
                    <p><strong>Event Count:</strong> {events} events in 5m</p>
                    <p><strong>Anomaly Distance:</strong> {score}</p>
                </div>
                """, unsafe_allow_html=True)
else:
    st.info("No anomalies detected.")

st.markdown("---")

# ------------------------------------------------------------------------------
# 3. Model Concept Drift Monitor
# ------------------------------------------------------------------------------
st.header("📈 Data Distribution & Concept Drift")

drift_file = os.path.join(METRICS_DIR, 'ml_drift_detection.txt')
if os.path.exists(drift_file):
    with open(drift_file, 'r') as f:
        drift_text = f.read()

    drifts = re.findall(r'(\w+)\s+\|\s+([\d\.]+)\s+\|\s+([\d\.]+)\s+\|\s+([\d\.]+)\s+\|\s+(STABLE|🚨 MODEL DRIFT WARNING)', drift_text)
    
    if drifts:
        cols = st.columns(len(drifts))
        for i, (feature, ref, cur, zscore, status) in enumerate(drifts):
            with cols[i]:
                border_color = "#00E676" if status == "STABLE" else "#FF3D00"
                status_color = "#00E676" if status == "STABLE" else "#FF3D00"
                st.markdown(f"""
                <div class="metric-card" style="border-left: 5px solid {border_color};">
                    <h4>Feature: {feature}</h4>
                    <p><strong>Z-Score:</strong> {zscore}</p>
                    <p><strong>Status:</strong> <span style="color: {status_color};">{status}</span></p>
                </div>
                """, unsafe_allow_html=True)
else:
    st.info("Drift monitoring metrics not found.")

st.markdown("<br><br><center><p style='color: gray;'>CMU Distributed Analytics Platform • Final Project</p></center>", unsafe_allow_html=True)
