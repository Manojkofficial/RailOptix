"""
RailOptix - CLI Entry Point
============================
Runs all modules in sequence without the Streamlit dashboard.
Useful for testing the pipeline standalone.

Usage:
  python main.py             # Run full pipeline
  python main.py --test      # Run tests only
  python main.py --spark     # Run Spark analytics only
"""

import os
import sys
import time
import argparse
import logging

# Force UTF-8 output on Windows
if sys.platform == "win32":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("RailOptix")

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config.settings import DATASET_PATH, OUTPUT_DIR


def banner():
    print("\n" + "=" * 65)
    print("  [TRAIN] RailOptix - Distributed Railway Network Analytics")
    print("          B.Tech Cluster Computing Project")
    print("=" * 65 + "\n")


def run_full_pipeline():
    """Run the complete analytical pipeline."""
    banner()
    t_start = time.time()

    # Phase 3: Data Processing
    print("[Phase 3] Loading and cleaning dataset...")
    from src.data_processing import load_and_clean_data, get_dataset_summary
    df = load_and_clean_data()
    summary = get_dataset_summary(df)
    print(f"    [OK] Records loaded   : {len(df):,}")
    print(f"    [OK] Unique trains    : {summary['unique_trains']:,}")
    print(f"    [OK] Unique zones     : {summary['unique_zones']}")
    print(f"    [OK] Missing values   : {summary['missing_values']}")

    # Phase 5: Graph Building
    print("\n[Phase 5] Constructing railway graph...")
    from src.graph_builder import build_railway_graph, get_graph_summary
    G = build_railway_graph(df)
    gsummary = get_graph_summary(G)
    print(f"    [OK] Stations (nodes) : {gsummary['total_stations']:,}")
    print(f"    [OK] Connections      : {gsummary['total_connections']:,}")
    print(f"    [OK] Components       : {gsummary['total_components']}")
    print(f"    [OK] Is connected     : {gsummary['is_connected']}")

    # Phase 6: Connectivity Analysis
    print("\n[Phase 6] Connectivity analysis (Tasks B, C, D)...")
    from src.connectivity_analysis import (
        compute_station_degrees, identify_top_hubs, detect_weak_connectivity
    )
    degree_df = compute_station_degrees(G)
    print(f"    [OK] [Task B] Station degrees computed for {len(degree_df):,} stations")

    hubs = identify_top_hubs(degree_df, n=10)
    print(f"    [OK] [Task C] Top 10 hubs identified:")
    for _, row in hubs.head(5).iterrows():
        print(f"              #{int(row['rank'])}: {row['station']} ({int(row['degree'])} connections)")

    weak = detect_weak_connectivity(G, degree_df)
    s = weak["summary"]
    print(f"    [OK] [Task D] Dead-ends     : {s['dead_end_count']:,} ({s['dead_end_pct']}%)")
    print(f"                  Poorly conn.  : {s['poorly_connected_count']:,}")
    print(f"                  Isolated      : {s['isolated_count']:,}")

    # Phase 7: Route Optimizer
    print("\n[Phase 7] Route optimizer (Task E)...")
    from src.route_optimizer import find_shortest_route

    test_routes = [
        ("DADAR", "THANE"),
        ("KALYAN JN", "DADAR"),
    ]
    for src, dst in test_routes:
        result = find_shortest_route(G, src, dst)
        if result["success"]:
            print(f"    [OK] [Task E] {src} -> {dst}: {len(result['path'])} stations, "
                  f"~{result['estimated_time_str']} (estimate)")
        else:
            print(f"    [WARN] [Task E] {src} -> {dst}: {result['error']}")

    # Phase 4: Spark
    print("\n[Phase 4] Apache Spark analytics...")
    from src.spark_engine import get_spark_info, run_rdd_operations
    info = get_spark_info()
    if info.get("available"):
        print(f"    [OK] SparkSession : version {info['version']}, {info['default_parallelism']} threads")
        rdd = run_rdd_operations()
        print(f"    [OK] RDD count    : {rdd.get('total_records_rdd', 0):,} records")
        print(f"    [OK] RDD time     : {rdd.get('rdd_execution_time_sec', 0):.2f}s")
    else:
        print("    [WARN] Spark not available - using pandas fallback")

    # Phase 8: Deliverable Exports
    print("\n[Phase 8] Exporting analytical deliverables to outputs/...")
    import json
    from src.analytics import compute_all_kpis, compute_zone_analytics
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    degree_df.to_csv(os.path.join(OUTPUT_DIR, "station_connectivity.csv"), index=False)
    hubs.to_csv(os.path.join(OUTPUT_DIR, "top_railway_hubs.csv"), index=False)
    if "dead_end_stations" in weak and not weak["dead_end_stations"].empty:
        weak["dead_end_stations"].to_csv(os.path.join(OUTPUT_DIR, "dead_end_stations.csv"), index=False)
    if "poorly_connected" in weak and not weak["poorly_connected"].empty:
        weak["poorly_connected"].to_csv(os.path.join(OUTPUT_DIR, "poorly_connected_stations.csv"), index=False)
    kpis = compute_all_kpis(df, G)
    with open(os.path.join(OUTPUT_DIR, "network_summary.json"), "w", encoding="utf-8") as f:
        json.dump(kpis, f, indent=2)
    zone_df = compute_zone_analytics(df)
    zone_df.to_csv(os.path.join(OUTPUT_DIR, "zone_analytics.csv"), index=False)
    print(f"    [OK] Generated deliverables saved in '{OUTPUT_DIR}'")

    # Summary
    elapsed = round(time.time() - t_start, 2)
    print(f"\n{'='*65}")
    print(f"  [OK] Full pipeline complete in {elapsed}s")
    print(f"  [>>] Start dashboard: streamlit run dashboard/app.py")
    print(f"{'='*65}\n")


def run_tests():
    """Run the test suite."""
    banner()
    import subprocess
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "tests/", "-v", "--tb=short"],
        cwd=os.path.dirname(os.path.abspath(__file__))
    )
    sys.exit(result.returncode)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="RailOptix CLI")
    parser.add_argument("--test", action="store_true", help="Run test suite")
    parser.add_argument("--spark", action="store_true", help="Run Spark analytics only")
    args = parser.parse_args()

    if args.test:
        run_tests()
    elif args.spark:
        from src.spark_engine import get_spark_info, run_rdd_operations, run_spark_sql_analytics
        banner()
        print("Running Spark analytics...")
        info = get_spark_info()
        print(f"Spark available: {info.get('available')}")
        if info.get("available"):
            rdd = run_rdd_operations()
            print(f"RDD result: {rdd.get('total_records_rdd')} records in {rdd.get('rdd_execution_time_sec')}s")
    else:
        run_full_pipeline()
