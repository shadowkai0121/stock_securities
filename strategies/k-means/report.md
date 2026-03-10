# Empirical Log-Return Risk-Return Geometry for Taiwan 50 Constituents: A Silhouette-Optimized K-Means Segmentation Study

## Abstract
This study implements a local, reproducible K-Means clustering framework for Taiwan equity data using empirical daily log-returns extracted from SQLite databases produced by the repository's existing `finmind-dl` pipeline. For each stock, the model estimates two sufficient statistics of the log-return process: the empirical mean return and the empirical volatility. These features are standardized with Z-scores and clustered across candidate partitions from K = 2 to K = 10, where the final number of clusters is selected by the Silhouette Score. The resulting workflow yields an interpretable risk-return map and a reusable research pipeline for portfolio segmentation and risk monitoring. In the current workspace execution dated 2026-03-10 03:30:06 UTC, the code and report were generated successfully, but empirical clustering results remain conditional on the availability of populated `price_adj_daily` histories in `data/*.sqlite`.

## 1. Introduction
Traditional sector taxonomies such as GICS assume that industrial similarity is a stable proxy for co-movement and risk exposure. In stressed or rapidly rotating markets, that assumption becomes weak because securities from different industries can converge toward similar return-volatility behavior, while companies within the same sector can diverge materially. A clustering model built directly on empirical log-return statistics therefore offers a more behavior-based representation of market structure.

The present implementation focuses on a parsimonious two-dimensional feature space: the empirical mean and standard deviation of daily log-returns. This design preserves interpretability, aligns naturally with modern portfolio theory's risk-return lens, and makes the cluster geometry visually inspectable. By selecting the number of clusters through an internal validation metric rather than a fixed prior belief, the framework adapts to the actual cross-sectional structure embedded in the sample.

## 2. Methodology

### 2.1 Data Collection
This strategy is deliberately isolated from data acquisition. It reads existing `data/<stock_id>.sqlite` files created by the repository's `finmind-dl` workflow, loads `.env` via `python-dotenv` for project-level consistency, and performs no API requests inside the clustering pipeline.

At runtime, the script scans `data/*.sqlite`, reads the `price_adj_daily` table from each database, filters invalid or placeholder rows, and merges all eligible adjusted close series into a single `pandas.DataFrame` indexed by trading date. Securities with fewer than `60` valid log-return observations are excluded from the feature stage to avoid unstable moment estimates.

### 2.2 Feature Engineering
For each stock \(i\) with adjusted close price \(P_{i,t}\), the daily empirical log-return is defined as:

$$
X_{i,t} = \ln \left( \frac{P_{i,t}}{P_{i,t-1}} \right)
$$

The two clustering features are the empirical mean return and empirical volatility:

$$
\bar{\mu}_i = \frac{1}{T_i} \sum_{t=1}^{T_i} X_{i,t}
$$

$$
\bar{\sigma}_i = \sqrt{\frac{1}{T_i - 1} \sum_{t=1}^{T_i} \left( X_{i,t} - \bar{\mu}_i \right)^2 }
$$

The raw feature vector \((\bar{\mu}_i, \bar{\sigma}_i)\) is standardized using Z-scores before clustering:

$$
Z_{i,j} = \frac{f_{i,j} - \mu_j^f}{\sigma_j^f}
$$

where \(f_{i,j}\) denotes feature \(j\) of stock \(i\), and \(\mu_j^f\), \(\sigma_j^f\) are the cross-sectional mean and standard deviation of feature \(j\).

### 2.3 K-Means Clustering & Evaluation
Let \(K \in \{2, \ldots, 10\}\). For each candidate \(K\), the algorithm fits a K-Means model on the standardized feature matrix and computes the Silhouette Score:

$$
s(i) = \frac{b(i) - a(i)}{\max \{a(i), b(i)\}}
$$

where \(a(i)\) is the average within-cluster distance for stock \(i\) and \(b(i)\) is the minimum average distance to the nearest alternative cluster. The selected model is the one with the highest mean silhouette value; ties are broken toward the smaller \(K\) for parsimony.

## 3. Empirical Results
The current workspace snapshot on March 10, 2026 did not contain enough eligible adjusted-close histories to estimate a silhouette-validated clustering solution. The `data` directory contained 1 SQLite file(s), yet only 0 file(s) exposed usable `price_adj_daily` rows, and fewer than three stocks satisfied the minimum observation threshold.

### 3.1 Data Inventory Snapshot
The table below summarizes the first available database records inspected by the pipeline during the latest execution.

| db_file | stock_id | status | row_count | start_date | end_date | reason |
| --- | --- | --- | --- | --- | --- | --- |
| 2330.sqlite | 2330 | empty_table | 0 |  |  | Table 'price_adj_daily' contains no rows. |

### 3.2 Model Selection
| K | Silhouette Score |
| --- | --- |
| N/A | Not available |

### 3.3 Cluster Interpretation
| Cluster | Stocks | Avg. Mean Return | Avg. Volatility |
| --- | --- | --- | --- |
| N/A | N/A | Not available | Not available |

- Clustering was not executed in this run.

### 3.4 Visual Output
The risk-return scatter plot is written to `Not generated` when at least one valid clustering solution is available. The X-axis denotes empirical volatility, the Y-axis denotes empirical mean log-return, colors identify clusters, and black `X` markers indicate centroids.

## 4. Conclusion
The implemented pipeline provides a clean separation between data engineering and research logic: `finmind-dl` remains responsible for populating SQLite databases, while the present strategy focuses exclusively on feature construction, unsupervised learning, visualization, and reporting. Once the Taiwan 50 constituent histories are fully populated under `data/*.sqlite`, the model can be rerun without modification to produce a behavior-driven segmentation of the universe.

From an applied perspective, this partition can support portfolio construction, peer comparison, and regime-sensitive risk management. Investors may use the clusters to distinguish defensive from aggressive exposures, identify cross-industry substitutes with similar realized dynamics, and monitor whether the latent market structure changes as new data arrive. The current implementation is intentionally lightweight, reproducible, and extensible, making it suitable as a base layer for more advanced hierarchical, regime-switching, or factor-augmented clustering studies.

## Code Availability
- Main entry point: `strategies/k-means/main.py`
- Core analysis module: `strategies/k-means/kmeans_clustering.py`
- Output directory: `strategies/k-means/outputs/`
