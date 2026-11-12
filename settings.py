"""
RailOptix – Central Configuration
All paths, Spark settings, and application constants.
"""

import os

# ── Paths ──────────────────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
OUTPUT_DIR = os.path.join(BASE_DIR, "outputs")

DATASET_PATH = os.path.join(DATA_DIR, "trains.csv")

# ── Spark ──────────────────────────────────────────────────────────────────────
SPARK_APP_NAME = "RailOptix"
SPARK_MASTER = "local[*]"
SPARK_DRIVER_MEMORY = "2g"
SPARK_EXECUTOR_MEMORY = "2g"

# ── Graph ──────────────────────────────────────────────────────────────────────
# Stations with degree <= this threshold are "poorly connected"
POORLY_CONNECTED_THRESHOLD = 2
# Stations with degree == 1 are dead-ends
DEAD_END_DEGREE = 1
# Top N hubs to highlight
TOP_HUBS_N = 10

# ── Network Visualization ──────────────────────────────────────────────────────
# Subsample node limit for the interactive graph (performance)
GRAPH_DISPLAY_MAX_NODES = 300
GRAPH_DISPLAY_MAX_EDGES = 800

# ── Dashboard ──────────────────────────────────────────────────────────────────
APP_TITLE = "RailOptix"
APP_SUBTITLE = "Distributed Railway Network Analytics & Optimization"
APP_ICON = "🚆"

# ── Colours ────────────────────────────────────────────────────────────────────
COLOR_PRIMARY = "#00D4FF"       # cyan accent
COLOR_SECONDARY = "#1E90FF"     # blue accent
COLOR_BG = "#0A0F1E"            # dark navy background
COLOR_CARD = "#111827"          # card background
COLOR_SUCCESS = "#00FF9F"       # green highlight
COLOR_WARNING = "#FFB800"       # amber warning
COLOR_DANGER = "#FF4B6E"        # red danger

PLOTLY_TEMPLATE = "plotly_dark"

# ── Columns ────────────────────────────────────────────────────────────────────
NUMERIC_COLS = [
    "Distance_km", "AverageSpeed_kmph", "TravelTime_hr",
    "CoachCount", "OccupancyPercentage", "DelayProbability",
    "PunctualityScore", "YearIntroduced", "AvgStops",
    "PlatformCountAtOrigin", "ACPercentageCoaches",
    "PeakSeasonMultiplier", "MaintenanceScore", "Revenue_INR", "Rating",
]
