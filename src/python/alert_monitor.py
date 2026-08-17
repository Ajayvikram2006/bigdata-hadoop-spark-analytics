import time
import os
import re

# ==============================================================================
# Phase 17 - Alert Generation Monitor
# ==============================================================================
# This script monitors the outputs of our ML pipeline and generates 
# operational alerts (Slack-style) in the terminal.
# 
# Alert Types:
# 1. HIGH FAILURE RISK (from CMU RF Model Phase)
# 2. HDFS ANOMALOUS BEHAVIOR (from K-Means Model Phase)
# 3. MODEL DRIFT WARNING (from Drift Detection Phase)
# ==============================================================================

# ANSI Color Codes for terminal
RED = '\033[91m'
YELLOW = '\033[93m'
GREEN = '\033[92m'
CYAN = '\033[96m'
RESET = '\033[0m'

METRICS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'metrics')

def parse_and_alert_kmeans():
    kmeans_file = os.path.join(METRICS_DIR, 'ml_kmeans_anomaly.txt')
    if not os.path.exists(kmeans_file):
        return

    print(f"{CYAN}--- Checking HDFS Anomaly Detection (K-Means) ---{RESET}")
    with open(kmeans_file, 'r') as f:
        content = f.read()
        
    # Extract lines like: 1. namenode (2016-11-18 16:25:00) | Score: 7584.49 | Events: 108,385
    anomalies = re.findall(r'\d+\.\s+(\w+)\s+\((.*?)\)\s+\|\s+Score:\s+([\d\.]+)\s+\|\s+Events:\s+([\d,]+)', content)
    
    for i, anomaly in enumerate(anomalies):
        node, timestamp, score, events = anomaly
        if float(score) > 3000:
            print(f"{RED}[CRITICAL ALERT] HDFS ANOMALOUS BEHAVIOR DETECTED!{RESET}")
            print(f"  Time  : {timestamp}")
            print(f"  Node  : {node}")
            print(f"  Score : {score} (High distance from centroid)")
            print(f"  Desc  : Massive metadata storm detected ({events} events in 5m window)\n")
            time.sleep(1)

def parse_and_alert_drift():
    drift_file = os.path.join(METRICS_DIR, 'ml_drift_detection.txt')
    if not os.path.exists(drift_file):
        return

    print(f"{CYAN}--- Checking Model Drift Monitor ---{RESET}")
    with open(drift_file, 'r') as f:
        content = f.read()

    # Extract lines like: totalTasks         |     629.73 |     445.08 |     0.08 | STABLE
    drifts = re.findall(r'(\w+)\s+\|\s+([\d\.]+)\s+\|\s+([\d\.]+)\s+\|\s+([\d\.]+)\s+\|\s+(STABLE|🚨 MODEL DRIFT WARNING)', content)
    
    for drift in drifts:
        feature, ref, cur, zscore, status = drift
        if status != "STABLE":
            print(f"{YELLOW}[WARNING] MODEL DRIFT DETECTED!{RESET}")
            print(f"  Feature : {feature}")
            print(f"  Z-Score : {zscore} (> 1.0 std dev shift)")
            print(f"  Action  : Triggering model retraining pipeline...\n")
        else:
            print(f"{GREEN}[OK] Feature '{feature}' is STABLE (Z-score: {zscore}){RESET}")
    print()

def demo_simulated_rf_alert():
    print(f"{YELLOW}--- DEMO SIMULATION (not a live model call) ---{RESET}")
    print(f"{CYAN}--- Checking Random Forest Predictive Monitor ---{RESET}")
    print(f"{YELLOW}[ALERT] HIGH FAILURE RISK PREDICTED!{RESET}")
    print(f"  Job ID    : CMU-Job-883419 (DEMO)")
    print(f"  Metrics   : totalTasks=8500, mapReduceRatio=400.0")
    print(f"  Prediction: FAILED (Confidence: 89.2%)")
    print(f"  Action    : Auto-killing job to free cluster resources.\n")

if __name__ == '__main__':
    print(f"\n{CYAN}======================================================{RESET}")
    print(f"{CYAN}   DISTRIBUTED ANALYTICS ALERT MONITOR DASHBOARD      {RESET}")
    print(f"{CYAN}======================================================{RESET}\n")
    
    time.sleep(1)
    parse_and_alert_kmeans()
    
    time.sleep(1)
    parse_and_alert_drift()
    
    time.sleep(1)
    demo_simulated_rf_alert()
    
    print(f"{GREEN}--- Alert Monitoring Complete ---{RESET}\n")
