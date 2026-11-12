# 🚆 RailOptix — Distributed Railway Network Analytics & Optimization Platform

[![Python 3.11](https://img.shields.io/badge/Python-3.11-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5.4-E25A1C?logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![NetworkX](https://img.shields.io/badge/NetworkX-3.0+-blue)](https://networkx.org/)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.30+-FF4B4B?logo=streamlit&logoColor=white)](https://streamlit.io/)
[![Plotly](https://img.shields.io/badge/Plotly-5.18+-3F4F75?logo=plotly&logoColor=white)](https://plotly.com/)
[![Tests](https://img.shields.io/badge/Tests-45%20Passing-brightgreen)](tests/)

> **B.Tech Final Year Engineering Project**  
> **Domain:** Distributed Computing, Graph Theory, Big Data Engineering & Interactive Visual Analytics.

---

## 📌 Executive Summary & Problem Statement

Modern national railway systems (such as Indian Railways) operate vast, intricate networks with thousands of stations and tens of thousands of active train routes. Analyzing connectivity, bottlenecks, hub dependencies, and path optimizations across such scale introduces severe computational challenges:
1. **Network Complexity:** Over 9,000 physical stations and 17,000 distinct operational segments create dense topological graphs with isolated sub-networks and critical bridge junctions.
2. **Big Data Processing:** Stop sequence parsing, route frequency aggregation, and distributed metric computations require scalable cluster-computing abstractions (RDDs, DataFrames, and Catalyst-optimized Spark SQL) to process efficiently without single-node memory bottlenecks.
3. **Operational Decision-Making:** Network planners require real-time visibility into hub centrality, dead-end terminus vulnerabilities, and dynamic Dijkstra-based route simulations with segment duration breakdowns.

**RailOptix** addresses these challenges by uniting distributed data engineering (**Apache Spark 3.5**) with algorithmic graph analytics (**NetworkX**) and an enterprise-grade interactive dashboard (**Streamlit + Plotly**).

---

## 🎯 Project Objectives

- **High-Throughput Data Pipeline:** Ingest, clean, validate, and parse raw railway transit records (~14,783 train routes) from pipe-delimited station sequences.
- **Topological Graph Construction:** Model the railway network as an undirected, weighted graph $G = (V, E)$ where vertices $V$ represent stations and edges $E$ represent direct scheduled train services weighted by inverse train frequency and estimated segment duration.
- **Station Connectivity & Hub Profiling:** Compute station degrees, classify connectivity brackets, rank national railway hubs, and analyze multi-dimensional centrality (Degree, Betweenness, and Closeness).
- **Vulnerability & Weak Connectivity Detection:** Identify network dead-ends (degree = 1), poorly connected stations (degree $\le$ 2), and catalog disconnected network components.
- **Optimal Pathfinding Engine:** Implement Dijkstra's shortest-path algorithm and Yen's k-shortest paths to evaluate multi-hop routes between arbitrary stations with segment-by-segment duration estimation.
- **Distributed Big Data Engine:** Demonstrate Apache Spark capabilities via Resilient Distributed Datasets (RDDs), Spark SQL with temporary relational views, and Scala GraphX reference implementations.
- **Production Dashboard:** Deliver 9 synchronized, responsive analytics pages with clear scientific disclaimers (e.g., force-directed topological layouts vs. geographic positioning).

---

## 🏗️ System Architecture

```
                  +----------------------------------------------+
                  |         Raw Dataset: data/trains.csv         |
                  |     (~14,783 routes, stop sequences '|')     |
                  +----------------------------------------------+
                                         |
                                         v
                  +----------------------------------------------+
                  |     Phase 3: Data Processing & Cleaning      |
                  |  - Schema coercion & validation              |
                  |  - Pipe-delimited stop sequence extraction   |
                  |  - Upper-case station canonicalization       |
                  +----------------------------------------------+
                                   /            \
                                  /              \
                                 v                v
+------------------------------------+  +-------------------------------------+
|    Phase 4: Apache Spark Engine    |  |  Phase 5 & 6: Graph & Connectivity  |
| - PySpark 3.5.4 Local Driver/Work  |  | - NetworkX Undirected Graph G=(V,E) |
| - Native RDD (map, filter, reduce) |  | - Station Degree & Hub Ranking      |
| - Spark SQL Catalyst View 'trains' |  | - Weak Connectivity & Dead-ends     |
| - Spark GraphX Reference Pipeline  |  | - Multi-component Graph Breakdown   |
+------------------------------------+  +-------------------------------------+
                                 \                /
                                  \              /
                                   v            v
                  +----------------------------------------------+
                  |        Phase 7: Dijkstra Route Optimizer     |
                  |  - Shortest pathfinding & intermediate hops  |
                  |  - Segment duration & travel-time estimates  |
                  |  - Yen's K-Shortest alternative paths        |
                  +----------------------------------------------+
                                         |
                                         v
                  +----------------------------------------------+
                  |  Streamlit 9-Page Enterprise Analytics UI   |
                  |  (Plotly Dark Theme, KPI Cards, Interactive) |
                  +----------------------------------------------+
```

---

## 🔬 Graph Theory & Mathematical Algorithms

### 1. Network Modeling
The railway infrastructure is modeled as an undirected multigraph flattened into a weighted simple graph $G = (V, E)$:
- **Vertices ($V$):** Station names canonicalized to uppercase. $|V| = 9,049$ stations.
- **Edges ($E$):** Consecutive station stops along any train route. $|E| = 17,067$ unique connections.
- **Edge Weight ($w_{u,v}$):** Represents path cost derived from estimated segment travel time:
  $$\text{Segment Time} = \frac{\text{Total Train Travel Time}}{\text{Total Route Segments}}$$
  Multiple trains between the same pair $(u, v)$ increment edge frequency and update minimum estimated traversal cost.

### 2. Node Degree & Hub Centrality
- **Degree:** $k_i = \sum_{j \in V} A_{ij}$, measuring the number of directly accessible neighboring stations.
- **Betweenness Centrality (Approximated):**
  $$C_B(v) = \sum_{s \ne v \ne t} \frac{\sigma_{st}(v)}{\sigma_{st}}$$
  Quantifies how often station $v$ acts as a bridge along the shortest route between randomly sampled origin-destination pairs.

### 3. Dijkstra Shortest Path
Given origin $s$ and destination $t$, Dijkstra's algorithm computes the path minimizing total traversal weight:
$$D(t) = \min_{p \in \mathcal{P}_{s,t}} \sum_{e \in p} w_e$$
The path is reconstructed via predecessor backtracking and unpacked into intermediate hops, segment distances, and aggregate estimated travel times.

---

## ⚡ Apache Spark Distributed Architecture

The big data analytics module (`src/spark_engine.py`) provides cluster-ready data processing:

| Spark Component | Implementation Details | Academic Purpose |
| :--- | :--- | :--- |
| **SparkSession** | Master: `local[*]`, Memory: 2GB Driver / 2GB Executor | Coordinates execution plan across multi-core CPU workers |
| **RDD Transformations** | `.filter()` (premium trains), `.flatMap()` (stop extraction), `.map()` (key-value pairs), `.reduceByKey()` (zone/frequency aggregation), `.mapValues()` | Distributed functional programming without relational schemas |
| **RDD Actions** | `.count()`, `.first()`, `.take(n)`, `.collect()` | Materializes lazy evaluation DAGs into memory |
| **Spark SQL** | Temporary view `trains`, Catalyst query optimizer, GROUP BY, HAVING, AVG, SUM | Distributed relational analytics and query plan inspection |
| **GraphX** | Scala reference pipeline (`graphx_demo/RailwayGraphX.scala`) | Demonstrates distributed graph computation with `VertexRDD` and `EdgeRDD` |

---

## 📊 Dataset Specifications

- **Source:** Indian Railways Time Table Dataset (`data/trains.csv`)
- **Total Records:** 14,783 train journeys
- **Columns:** 18 fields including:
  - `TrainNo`, `TrainName`: Official identifiers
  - `RailwayZone`: Operating zone (e.g., NR, WR, CR, SR, ER)
  - `TrainCategory`: Express, Superfast, Passenger, Rajdhani, Shatabdi, etc.
  - `stops`: Pipe-delimited string sequence of scheduled station stops (`STATION_A|STATION_B|...`)
  - `Distance_km`, `TravelTime_hr`, `AverageSpeed_kmph`: Operational metrics
  - `OccupancyPercentage`, `PunctualityScore`, `DelayProbability`: Quality of service indicators

> **Scientific Disclaimer:**  
> The dataset contains topological station stop sequences but does not include GPS latitude/longitude coordinates. Network graphs are rendered using force-directed spring layouts reflecting connectivity clustering, **not** physical geospatial maps. Segment travel times are proportional analytical estimates.

---

## 🖥️ The Ten Dashboard Modules

1. **Executive Overview (`01_executive_overview.py`):** Executive KPI metrics (total trains, nodes, edges, average speed, occupancy), zone distributions, train type breakdown, and graph properties.
2. **Network Explorer (`02_network_explorer.py`):** Interactive force-directed network graph with dynamic node count filtering, degree thresholds, and connection explorer.
3. **Connectivity Analysis (`03_connectivity_analysis.py`):** Comprehensive degree distributions, degree classification brackets (isolated, dead-end, moderate, hub), and searchable station connectivity table.
4. **Hub Detection (`04_hub_detection.py`):** Identification of top national hubs (Kanpur Central, Varanasi Jn, etc.), radar chart of Degree/Betweenness/Closeness centrality, and hub network projection.
5. **Weak Connectivity (`05_weak_connectivity.py`):** Automated detection of dead-end terminus stations (degree = 1), vulnerable stations (degree $\le$ 2), and cataloging of disconnected network components.
6. **Route Optimizer (`06_route_optimizer.py`):** Dijkstra shortest-route finder with interactive station search, step-by-step intermediate station roadmap, segment metrics, and alternative routes.
7. **Travel & Train Analytics (`07_travel_analytics.py`):** Multi-tab analytics on zonal operations, train category performance, seat occupancy distributions, punctuality vs delay scatter plots, and speed trends.
8. **Apache Spark Analytics (`08_spark_analytics.py`):** Live execution of PySpark RDD transformations, Spark SQL queries with physical plan inspection, distributed station degree aggregation, and Scala GraphX documentation.
9. **Dataset Explorer (`09_dataset_explorer.py`):** Full data table browser with multi-column filtering (Zone, Category, Speed, Distance, Train Name/Number) and summary statistics.
10. **ML Model Comparison (`10_ml_model_comparison.py`):** Interactive model comparison and benchmarking of supervised regression algorithms (Gradient Boosting, Decision Tree, Linear Regression), featuring $R^2$, RMSE, MAE benchmarks, actual vs. predicted scatter plots, residual distributions, feature importance drivers, and a real-time multi-model prediction simulator.

---

## 📂 Repository Structure

```
RailOptix/
│
├── config/
│   ├── __init__.py
│   └── settings.py               # Central paths, Spark configs, constants, colors
│
├── dashboard/
│   ├── __init__.py
│   ├── app.py                    # Streamlit entry point, CSS styling, shared loaders
│   └── pages/                    # 9 Modular Analytics Dashboard Pages
│       ├── 01_executive_overview.py
│       ├── 02_network_explorer.py
│       ├── 03_connectivity_analysis.py
│       ├── 04_hub_detection.py
│       ├── 05_weak_connectivity.py
│       ├── 06_route_optimizer.py
│       ├── 07_travel_analytics.py
│       ├── 08_spark_analytics.py
│       ├── 09_dataset_explorer.py
│       └── 10_ml_model_comparison.py
│
├── data/
│   └── trains.csv                # Complete Indian Railways dataset (~14,783 rows)
│
├── graphx_demo/
│   └── RailwayGraphX.scala       # Apache Spark GraphX Scala implementation
│
├── outputs/                      # Generated pipeline outputs & degree metrics
│
├── src/
│   ├── __init__.py
│   ├── analytics.py              # Statistical KPI and aggregation engine
│   ├── connectivity_analysis.py  # Degree, hub, and weak-connectivity algorithms
│   ├── data_processing.py        # Dataset loading, cleaning, and validation
│   ├── graph_builder.py          # NetworkX graph construction from stop sequences
│   ├── ml_models.py              # Supervised ML algorithms, training & inference
│   ├── route_optimizer.py        # Dijkstra shortest path & Yen's alternative paths
│   ├── spark_engine.py           # PySpark session, RDD operations, Spark SQL views
│   └── visualizations.py         # Plotly Dark visualization components
│
├── tests/
│   ├── __init__.py
│   └── test_railoptix.py         # 45 Comprehensive Unit & Integration Tests
│
├── .gitignore                    # Version control exclusions
├── main.py                       # CLI execution entry point
├── requirements.txt              # Production dependency specifications
└── README.md                     # Comprehensive academic project documentation
```

---

## ⚙️ Setup and Installation

### Prerequisites
- **Python:** 3.11+
- **Java:** JRE/JDK 8 or 11 (required for Apache Spark / PySpark)
- **RAM:** Minimum 4GB (8GB recommended for full Spark in-memory analytics)

### Step-by-Step Installation

1. **Clone or Navigate to the Repository:**
   ```bash
   cd RailOptix
   ```

2. **Activate the Virtual Environment:**
   - On Windows (PowerShell):
     ```powershell
     .\.venv\Scripts\Activate.ps1
     ```
   - On Linux / macOS:
     ```bash
     source .venv/bin/activate
     ```

3. **Install Dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

---

## 🚀 Running the Project

### 1. Launch the Interactive Web Dashboard
```bash
streamlit run dashboard/app.py
```
Open your browser to: **`http://localhost:8501`**

### 2. Run the Full CLI Analytics Pipeline
```bash
python main.py
```

### 3. Run Spark Analytics Standalone
```bash
python main.py --spark
```

---

## 🧪 Testing and Quality Assurance

The test suite covers data processing, graph construction, topological algorithms, route optimization, operational KPIs, and genuine PySpark RDD/SQL execution:

```bash
pytest tests/ -v
```

### Test Suite Summary:
```
tests/test_railoptix.py::TestDataProcessing::test_dataset_loads PASSED
tests/test_railoptix.py::TestDataProcessing::test_expected_columns_present PASSED
tests/test_railoptix.py::TestDataProcessing::test_no_empty_stops PASSED
tests/test_railoptix.py::TestDataProcessing::test_stops_format PASSED
tests/test_railoptix.py::TestDataProcessing::test_station_names_uppercase PASSED
tests/test_railoptix.py::TestDataProcessing::test_numeric_columns_coerced PASSED
tests/test_railoptix.py::TestDataProcessing::test_no_negative_distances PASSED
tests/test_railoptix.py::TestDataProcessing::test_stop_sequences_extraction PASSED
tests/test_railoptix.py::TestDataProcessing::test_dataset_summary PASSED
tests/test_railoptix.py::TestGraphBuilder::test_graph_has_nodes PASSED
tests/test_railoptix.py::TestGraphBuilder::test_graph_has_edges PASSED
tests/test_railoptix.py::TestGraphBuilder::test_nodes_are_strings PASSED
tests/test_railoptix.py::TestGraphBuilder::test_edges_have_weight PASSED
tests/test_railoptix.py::TestGraphBuilder::test_edges_have_train_count PASSED
tests/test_railoptix.py::TestGraphBuilder::test_edges_marked_as_estimates PASSED
tests/test_railoptix.py::TestGraphBuilder::test_graph_summary PASSED
tests/test_railoptix.py::TestGraphBuilder::test_known_station_in_graph PASSED
tests/test_railoptix.py::TestConnectivityAnalysis::test_degree_df_not_empty PASSED
tests/test_railoptix.py::TestConnectivityAnalysis::test_degree_df_columns PASSED
tests/test_railoptix.py::TestConnectivityAnalysis::test_degrees_non_negative PASSED
tests/test_railoptix.py::TestConnectivityAnalysis::test_ranks_sequential PASSED
tests/test_railoptix.py::TestConnectivityAnalysis::test_top_hubs_returned PASSED
tests/test_railoptix.py::TestConnectivityAnalysis::test_top_hub_has_highest_degree PASSED
tests/test_railoptix.py::TestConnectivityAnalysis::test_weak_connectivity_detection PASSED
tests/test_railoptix.py::TestConnectivityAnalysis::test_dead_end_degrees_are_one PASSED
tests/test_railoptix.py::TestRouteOptimizer::test_searchable_stations_not_empty PASSED
tests/test_railoptix.py::TestRouteOptimizer::test_searchable_stations_sorted PASSED
tests/test_railoptix.py::TestRouteOptimizer::test_same_station_returns_error PASSED
tests/test_railoptix.py::TestRouteOptimizer::test_nonexistent_station_returns_error PASSED
tests/test_railoptix.py::TestRouteOptimizer::test_empty_station_returns_error PASSED
tests/test_railoptix.py::TestRouteOptimizer::test_valid_connected_route PASSED
tests/test_railoptix.py::TestRouteOptimizer::test_route_path_is_valid PASSED
tests/test_railoptix.py::TestRouteOptimizer::test_segment_count_matches_path PASSED
tests/test_railoptix.py::TestRouteOptimizer::test_error_result_structure PASSED
tests/test_railoptix.py::TestRouteOptimizer::test_alternative_routes PASSED
tests/test_railoptix.py::TestRouteOptimizer::test_validate_stations PASSED
tests/test_railoptix.py::TestAnalytics::test_compute_all_kpis PASSED
tests/test_railoptix.py::TestAnalytics::test_zone_analytics PASSED
tests/test_railoptix.py::TestAnalytics::test_category_analytics PASSED
tests/test_railoptix.py::TestAnalytics::test_occupancy_analysis PASSED
tests/test_railoptix.py::TestSparkEngine::test_spark_info PASSED
tests/test_railoptix.py::TestSparkEngine::test_load_dataframe_spark PASSED
tests/test_railoptix.py::TestSparkEngine::test_spark_sql_analytics PASSED
tests/test_railoptix.py::TestSparkEngine::test_spark_rdd_operations PASSED
tests/test_railoptix.py::TestSparkEngine::test_distributed_connectivity PASSED

================= 45 passed, 39 warnings in 64.89s ==================
```

---

## 🔮 Limitations & Future Work

1. **Geographic Coordinates:** The current dataset models station stop sequences without GPS latitudes/longitudes. Future iterations can integrate geospatial lookups to project the network onto OpenStreetMap / GIS layers.
2. **Timetable & Platform Granularity:** Travel times are estimated by segment division. Future work can incorporate real-time scheduled arrival/departure timestamps and platform occupancy conflict constraints.
3. **Multi-Node Cluster Deployment:** Spark is configured for local multi-core cluster emulation (`local[*]`). The pipeline can be submitted without code modification to a multi-node YARN or Kubernetes Spark cluster (`spark-submit`).

---

## 📜 Academic Integrity & License
This project was developed as a B.Tech Final Year Capstone Project in Computer Science and Engineering (Distributed Systems / Data Engineering).  
Licensed under the [MIT License](LICENSE).
