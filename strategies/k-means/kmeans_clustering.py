from __future__ import annotations

import argparse
import json
import sqlite3
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import matplotlib
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from matplotlib.ticker import PercentFormatter
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from sklearn.preprocessing import StandardScaler

matplotlib.use("Agg")
from matplotlib import pyplot as plt


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DATA_DIR = REPO_ROOT / "data"
DEFAULT_OUTPUT_DIR = REPO_ROOT / "strategies" / "k-means" / "outputs"
DEFAULT_REPORT_PATH = REPO_ROOT / "strategies" / "k-means" / "report.md"
DEFAULT_PRICE_TABLE = "price_adj_daily"
FIGURE_FILENAME = "kmeans_risk_return_scatter.png"


@dataclass(slots=True)
class ClusteringConfig:
    data_dir: Path = DEFAULT_DATA_DIR
    output_dir: Path = DEFAULT_OUTPUT_DIR
    report_path: Path = DEFAULT_REPORT_PATH
    table_name: str = DEFAULT_PRICE_TABLE
    min_k: int = 2
    max_k: int = 10
    min_observations: int = 60
    random_state: int = 42


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="kmeans-empirical-log-returns",
        description=(
            "Cluster per-stock empirical log-return features from local SQLite price_adj_daily "
            "tables without making any API calls."
        ),
    )
    parser.add_argument("--data-dir", default=str(DEFAULT_DATA_DIR))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--report-path", default=str(DEFAULT_REPORT_PATH))
    parser.add_argument("--table-name", default=DEFAULT_PRICE_TABLE)
    parser.add_argument("--min-k", type=int, default=2)
    parser.add_argument("--max-k", type=int, default=10)
    parser.add_argument("--min-observations", type=int, default=60)
    parser.add_argument("--random-state", type=int, default=42)
    return parser


def parse_args(argv: list[str] | None = None) -> ClusteringConfig:
    args = build_parser().parse_args(argv)
    if args.min_k < 2:
        raise ValueError("--min-k must be at least 2.")
    if args.max_k < args.min_k:
        raise ValueError("--max-k must be greater than or equal to --min-k.")
    if args.min_observations < 2:
        raise ValueError("--min-observations must be at least 2.")
    return ClusteringConfig(
        data_dir=Path(args.data_dir),
        output_dir=Path(args.output_dir),
        report_path=Path(args.report_path),
        table_name=args.table_name,
        min_k=args.min_k,
        max_k=args.max_k,
        min_observations=args.min_observations,
        random_state=args.random_state,
    )


def ensure_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def load_project_environment() -> None:
    load_dotenv(REPO_ROOT / ".env")


def display_path(path: Path | None) -> str:
    if path is None:
        return "Not generated"
    try:
        return path.relative_to(REPO_ROOT).as_posix()
    except ValueError:
        return path.as_posix()


def read_price_series_from_db(db_path: Path, table_name: str) -> tuple[pd.DataFrame, dict[str, Any]]:
    metadata: dict[str, Any] = {
        "db_file": db_path.name,
        "stock_id": db_path.stem,
        "table_name": table_name,
        "status": "unknown",
        "reason": "",
        "row_count": 0,
        "start_date": None,
        "end_date": None,
    }

    if not db_path.exists():
        metadata["status"] = "missing_db"
        metadata["reason"] = "SQLite file not found."
        return pd.DataFrame(columns=["date", "stock_id", "close"]), metadata

    conn = sqlite3.connect(db_path)
    try:
        exists = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name = ?",
            (table_name,),
        ).fetchone()
        if not exists:
            metadata["status"] = "missing_table"
            metadata["reason"] = f"Table '{table_name}' does not exist."
            return pd.DataFrame(columns=["date", "stock_id", "close"]), metadata

        frame = pd.read_sql_query(
            f"""
            SELECT date, stock_id, close, is_placeholder
            FROM "{table_name}"
            ORDER BY date
            """,
            conn,
        )
    except sqlite3.Error as exc:
        metadata["status"] = "sqlite_error"
        metadata["reason"] = str(exc)
        return pd.DataFrame(columns=["date", "stock_id", "close"]), metadata
    finally:
        conn.close()

    if frame.empty:
        metadata["status"] = "empty_table"
        metadata["reason"] = f"Table '{table_name}' contains no rows."
        return pd.DataFrame(columns=["date", "stock_id", "close"]), metadata

    frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame["close"] = pd.to_numeric(frame["close"], errors="coerce")
    frame["is_placeholder"] = pd.to_numeric(frame["is_placeholder"], errors="coerce").fillna(0).astype(int)
    frame["stock_id"] = frame["stock_id"].astype(str).str.strip()

    frame = frame[
        (frame["date"].notna())
        & (frame["close"].notna())
        & (frame["close"] > 0)
        & (frame["is_placeholder"] == 0)
    ].copy()

    if frame.empty:
        metadata["status"] = "no_valid_prices"
        metadata["reason"] = "Rows exist but no valid non-placeholder close prices were found."
        return pd.DataFrame(columns=["date", "stock_id", "close"]), metadata

    unique_stock_ids = sorted(stock_id for stock_id in frame["stock_id"].unique().tolist() if stock_id)
    if len(unique_stock_ids) == 1:
        metadata["stock_id"] = unique_stock_ids[0]
    elif len(unique_stock_ids) > 1:
        metadata["reason"] = (
            "Multiple stock_id values were found in one DB; filename stem was used as the canonical ID."
        )

    frame = (
        frame[["date", "stock_id", "close"]]
        .drop_duplicates(subset=["date"], keep="last")
        .sort_values("date")
        .reset_index(drop=True)
    )

    metadata["status"] = "loaded"
    metadata["row_count"] = int(len(frame))
    metadata["start_date"] = frame["date"].min().strftime("%Y-%m-%d")
    metadata["end_date"] = frame["date"].max().strftime("%Y-%m-%d")
    return frame, metadata


def load_price_matrix(data_dir: Path, table_name: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    inventory_rows: list[dict[str, Any]] = []
    price_series: dict[str, pd.Series] = {}

    for db_path in sorted(data_dir.glob("*.sqlite")):
        frame, metadata = read_price_series_from_db(db_path, table_name)
        inventory_rows.append(metadata)
        if frame.empty:
            continue
        stock_id = str(metadata["stock_id"])
        price_series[stock_id] = frame.set_index("date")["close"].astype("float64")

    inventory = pd.DataFrame(inventory_rows)
    if not price_series:
        return pd.DataFrame(), inventory

    matrix = pd.DataFrame(price_series).sort_index()
    matrix.index.name = "date"
    matrix = matrix.sort_index(axis=1)
    return matrix, inventory


def compute_log_returns(price_matrix: pd.DataFrame) -> pd.DataFrame:
    if price_matrix.empty:
        return pd.DataFrame(columns=price_matrix.columns)
    log_returns = np.log(price_matrix / price_matrix.shift(1))
    return log_returns.replace([np.inf, -np.inf], np.nan)


def build_feature_matrix(log_returns: pd.DataFrame, min_observations: int) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for stock_id in log_returns.columns:
        series = log_returns[stock_id].dropna()
        if len(series) < min_observations:
            continue
        rows.append(
            {
                "stock_id": stock_id,
                "mean_return": float(series.mean()),
                "volatility": float(series.std(ddof=1)),
                "observations": int(series.shape[0]),
            }
        )

    if not rows:
        return pd.DataFrame(columns=["stock_id", "mean_return", "volatility", "observations"])
    return pd.DataFrame(rows).sort_values("stock_id").reset_index(drop=True)


def relabel_clusters(
    labels: np.ndarray,
    centroids_original_scale: np.ndarray,
) -> tuple[np.ndarray, pd.DataFrame]:
    centroid_frame = pd.DataFrame(
        {
            "old_cluster": np.arange(centroids_original_scale.shape[0]),
            "mean_return": centroids_original_scale[:, 0],
            "volatility": centroids_original_scale[:, 1],
        }
    )
    centroid_frame = centroid_frame.sort_values(
        ["volatility", "mean_return"],
        ascending=[True, True],
    ).reset_index(drop=True)
    centroid_frame["cluster"] = np.arange(1, len(centroid_frame) + 1)
    mapping = {
        int(row.old_cluster): int(row.cluster)
        for row in centroid_frame.itertuples(index=False)
    }
    relabeled = np.array([mapping[int(label)] for label in labels], dtype=int)
    centroids = centroid_frame[["cluster", "mean_return", "volatility"]].copy()
    return relabeled, centroids


def select_optimal_kmeans(
    feature_matrix: pd.DataFrame,
    *,
    min_k: int,
    max_k: int,
    random_state: int,
) -> dict[str, Any]:
    if len(feature_matrix) < 3:
        raise ValueError("At least three eligible stocks are required for silhouette-based K-Means.")

    raw_features = feature_matrix[["mean_return", "volatility"]].to_numpy()
    scaler = StandardScaler()
    scaled_features = scaler.fit_transform(raw_features)

    effective_max_k = min(max_k, len(feature_matrix) - 1)
    if effective_max_k < min_k:
        raise ValueError(
            "The requested K range is incompatible with the number of eligible stocks."
        )

    candidate_scores: list[dict[str, Any]] = []
    best_result: dict[str, Any] | None = None

    for k in range(min_k, effective_max_k + 1):
        model = KMeans(
            n_clusters=k,
            n_init=25,
            random_state=random_state,
        )
        labels = model.fit_predict(scaled_features)
        if len(np.unique(labels)) < 2:
            continue

        score = float(silhouette_score(scaled_features, labels))
        candidate_scores.append({"k": k, "silhouette_score": score})

        should_replace = (
            best_result is None
            or score > best_result["silhouette_score"] + 1e-12
            or (
                abs(score - best_result["silhouette_score"]) <= 1e-12
                and k < best_result["k"]
            )
        )
        if should_replace:
            centroids_original_scale = scaler.inverse_transform(model.cluster_centers_)
            best_result = {
                "k": k,
                "silhouette_score": score,
                "labels": labels,
                "centroids_original_scale": centroids_original_scale,
            }

    if best_result is None:
        raise ValueError("Unable to fit a valid K-Means model for any candidate K.")

    relabeled, centroids = relabel_clusters(
        best_result["labels"],
        best_result["centroids_original_scale"],
    )
    assignments = feature_matrix.copy()
    assignments["cluster"] = relabeled
    assignments = assignments.sort_values(["cluster", "stock_id"]).reset_index(drop=True)

    summary = (
        assignments.groupby("cluster", as_index=False)
        .agg(
            stock_count=("stock_id", "size"),
            avg_mean_return=("mean_return", "mean"),
            avg_volatility=("volatility", "mean"),
            min_mean_return=("mean_return", "min"),
            max_mean_return=("mean_return", "max"),
        )
    )
    summary["member_stock_ids"] = (
        assignments.groupby("cluster")["stock_id"]
        .apply(lambda stock_ids: ", ".join(sorted(stock_ids)))
        .values
    )

    score_frame = pd.DataFrame(candidate_scores).sort_values("k").reset_index(drop=True)
    return {
        "best_k": int(best_result["k"]),
        "best_silhouette_score": float(best_result["silhouette_score"]),
        "assignments": assignments,
        "centroids": centroids,
        "cluster_summary": summary,
        "silhouette_scores": score_frame,
    }


def plot_clusters(
    assignments: pd.DataFrame,
    centroids: pd.DataFrame,
    *,
    best_k: int,
    best_score: float,
    output_path: Path,
) -> None:
    figure, axis = plt.subplots(figsize=(10, 7))
    cmap = plt.get_cmap("tab10", max(best_k, 3))

    for cluster_id, cluster_frame in assignments.groupby("cluster", sort=True):
        color = cmap(cluster_id - 1)
        axis.scatter(
            cluster_frame["volatility"],
            cluster_frame["mean_return"],
            s=90,
            alpha=0.85,
            color=color,
            label=f"Cluster {cluster_id}",
            edgecolors="white",
            linewidths=0.8,
        )
        for row in cluster_frame.itertuples(index=False):
            axis.annotate(
                row.stock_id,
                (row.volatility, row.mean_return),
                textcoords="offset points",
                xytext=(5, 5),
                fontsize=8,
                color=color,
            )

    axis.scatter(
        centroids["volatility"],
        centroids["mean_return"],
        marker="X",
        s=260,
        color="black",
        linewidths=1.2,
        label="Centroids",
    )
    for row in centroids.itertuples(index=False):
        axis.annotate(
            f"C{row.cluster}",
            (row.volatility, row.mean_return),
            textcoords="offset points",
            xytext=(8, -12),
            fontsize=10,
            fontweight="bold",
            color="black",
        )

    axis.set_title(
        f"Taiwan Equity K-Means Clusters (best K={best_k}, silhouette={best_score:.4f})"
    )
    axis.set_xlabel("Daily Log-Return Volatility")
    axis.set_ylabel("Daily Mean Log-Return")
    axis.xaxis.set_major_formatter(PercentFormatter(xmax=1.0, decimals=2))
    axis.yaxis.set_major_formatter(PercentFormatter(xmax=1.0, decimals=2))
    axis.grid(alpha=0.25, linestyle="--")
    axis.legend(frameon=False)
    figure.tight_layout()
    figure.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close(figure)


def format_decimal(value: Any, digits: int = 6) -> str:
    if value is None:
        return "N/A"
    if isinstance(value, float) and np.isnan(value):
        return "N/A"
    return f"{float(value):.{digits}f}"


def format_percent(value: Any, digits: int = 4) -> str:
    if value is None:
        return "N/A"
    if isinstance(value, float) and np.isnan(value):
        return "N/A"
    return f"{float(value) * 100:.{digits}f}%"


def dataframe_to_markdown(frame: pd.DataFrame) -> str:
    if frame.empty:
        return "_No rows available._"
    headers = [str(column) for column in frame.columns]
    separator = ["---"] * len(headers)
    rows = ["| " + " | ".join(headers) + " |", "| " + " | ".join(separator) + " |"]
    for values in frame.itertuples(index=False, name=None):
        rows.append("| " + " | ".join(str(value) for value in values) + " |")
    return "\n".join(rows)


def build_empirical_result_paragraph(result: dict[str, Any]) -> str:
    if result["status"] != "success":
        return (
            "The current workspace snapshot on March 10, 2026 did not contain enough eligible "
            "adjusted-close histories to estimate a silhouette-validated clustering solution. "
            "The `data` directory contained "
            f"{result['database_files_scanned']} SQLite file(s), yet only "
            f"{result['stocks_with_valid_prices']} file(s) exposed usable `{result['table_name']}` "
            "rows, and fewer than three stocks satisfied the minimum observation threshold."
        )

    return (
        f"The silhouette search selected **K = {result['best_k']}** with a best score of "
        f"**{result['best_silhouette_score']:.4f}**. The resulting partition contains "
        f"{result['stocks_clustered']} stocks and can be interpreted as a cross-sectional "
        "risk-return segmentation in which each centroid approximates a characteristic "
        "mean-volatility profile."
    )


def describe_clusters(cluster_summary: pd.DataFrame) -> list[str]:
    if cluster_summary.empty:
        return []

    overall_mean = float(cluster_summary["avg_mean_return"].mean())
    overall_vol = float(cluster_summary["avg_volatility"].mean())
    descriptions: list[str] = []
    for row in cluster_summary.itertuples(index=False):
        mean_descriptor = "higher-return" if row.avg_mean_return >= overall_mean else "lower-return"
        vol_descriptor = "higher-volatility" if row.avg_volatility >= overall_vol else "lower-volatility"
        descriptions.append(
            f"Cluster {row.cluster} groups {row.stock_count} stock(s) with a {mean_descriptor}, "
            f"{vol_descriptor} profile; representative members are {row.member_stock_ids}."
        )
    return descriptions


def build_inventory_preview(inventory: pd.DataFrame) -> pd.DataFrame:
    if inventory.empty:
        return pd.DataFrame(
            [{"db_file": "N/A", "stock_id": "N/A", "status": "no_db_files", "reason": "No SQLite files found."}]
        )
    preview = inventory[
        ["db_file", "stock_id", "status", "row_count", "start_date", "end_date", "reason"]
    ].copy()
    return preview.head(10).fillna("")


def build_report_tables(result: dict[str, Any]) -> tuple[pd.DataFrame, pd.DataFrame]:
    if result["silhouette_scores"].empty:
        silhouette_table = pd.DataFrame(
            [{"K": "N/A", "Silhouette Score": "Not available"}]
        )
    else:
        silhouette_table = result["silhouette_scores"].copy()
        silhouette_table["K"] = silhouette_table["k"].astype(int)
        silhouette_table["Silhouette Score"] = silhouette_table["silhouette_score"].map(
            lambda value: format_decimal(value, digits=4)
        )
        silhouette_table = silhouette_table[["K", "Silhouette Score"]]

    if result["cluster_summary"].empty:
        cluster_table = pd.DataFrame(
            [{"Cluster": "N/A", "Stocks": "N/A", "Avg. Mean Return": "Not available", "Avg. Volatility": "Not available"}]
        )
    else:
        cluster_table = result["cluster_summary"].copy()
        cluster_table["Cluster"] = cluster_table["cluster"].astype(int)
        cluster_table["Stocks"] = cluster_table["stock_count"].astype(int)
        cluster_table["Avg. Mean Return"] = cluster_table["avg_mean_return"].map(
            lambda value: format_percent(value, digits=4)
        )
        cluster_table["Avg. Volatility"] = cluster_table["avg_volatility"].map(
            lambda value: format_percent(value, digits=4)
        )
        cluster_table["Members"] = cluster_table["member_stock_ids"]
        cluster_table = cluster_table[
            ["Cluster", "Stocks", "Avg. Mean Return", "Avg. Volatility", "Members"]
        ]

    return silhouette_table, cluster_table


def render_report(result: dict[str, Any], config: ClusteringConfig) -> str:
    timestamp = result["run_timestamp"]
    relative_figure = display_path(result["figure_path"])

    inventory_table = dataframe_to_markdown(result["inventory_preview"].copy())
    silhouette_table = dataframe_to_markdown(result["silhouette_table"])
    cluster_table = dataframe_to_markdown(result["cluster_table"])
    cluster_descriptions = describe_clusters(result["cluster_summary"])
    cluster_text = "\n".join(f"- {line}" for line in cluster_descriptions) if cluster_descriptions else "- Clustering was not executed in this run."

    methodology_note = (
        "This strategy is deliberately isolated from data acquisition. It reads existing "
        "`data/<stock_id>.sqlite` files created by the repository's `finmind-dl` workflow, "
        "loads `.env` via `python-dotenv` for project-level consistency, and performs no API "
        "requests inside the clustering pipeline."
    )

    return f"""# Empirical Log-Return Risk-Return Geometry for Taiwan 50 Constituents: A Silhouette-Optimized K-Means Segmentation Study

## Abstract
This study implements a local, reproducible K-Means clustering framework for Taiwan equity data using empirical daily log-returns extracted from SQLite databases produced by the repository's existing `finmind-dl` pipeline. For each stock, the model estimates two sufficient statistics of the log-return process: the empirical mean return and the empirical volatility. These features are standardized with Z-scores and clustered across candidate partitions from K = {config.min_k} to K = {config.max_k}, where the final number of clusters is selected by the Silhouette Score. The resulting workflow yields an interpretable risk-return map and a reusable research pipeline for portfolio segmentation and risk monitoring. In the current workspace execution dated {timestamp}, the code and report were generated successfully, but empirical clustering results remain conditional on the availability of populated `{config.table_name}` histories in `data/*.sqlite`.

## 1. Introduction
Traditional sector taxonomies such as GICS assume that industrial similarity is a stable proxy for co-movement and risk exposure. In stressed or rapidly rotating markets, that assumption becomes weak because securities from different industries can converge toward similar return-volatility behavior, while companies within the same sector can diverge materially. A clustering model built directly on empirical log-return statistics therefore offers a more behavior-based representation of market structure.

The present implementation focuses on a parsimonious two-dimensional feature space: the empirical mean and standard deviation of daily log-returns. This design preserves interpretability, aligns naturally with modern portfolio theory's risk-return lens, and makes the cluster geometry visually inspectable. By selecting the number of clusters through an internal validation metric rather than a fixed prior belief, the framework adapts to the actual cross-sectional structure embedded in the sample.

## 2. Methodology

### 2.1 Data Collection
{methodology_note}

At runtime, the script scans `data/*.sqlite`, reads the `{config.table_name}` table from each database, filters invalid or placeholder rows, and merges all eligible adjusted close series into a single `pandas.DataFrame` indexed by trading date. Securities with fewer than `{config.min_observations}` valid log-return observations are excluded from the feature stage to avoid unstable moment estimates.

### 2.2 Feature Engineering
For each stock \\(i\\) with adjusted close price \\(P_{{i,t}}\\), the daily empirical log-return is defined as:

$$
X_{{i,t}} = \\ln \\left( \\frac{{P_{{i,t}}}}{{P_{{i,t-1}}}} \\right)
$$

The two clustering features are the empirical mean return and empirical volatility:

$$
\\bar{{\\mu}}_i = \\frac{{1}}{{T_i}} \\sum_{{t=1}}^{{T_i}} X_{{i,t}}
$$

$$
\\bar{{\\sigma}}_i = \\sqrt{{\\frac{{1}}{{T_i - 1}} \\sum_{{t=1}}^{{T_i}} \\left( X_{{i,t}} - \\bar{{\\mu}}_i \\right)^2 }}
$$

The raw feature vector \\((\\bar{{\\mu}}_i, \\bar{{\\sigma}}_i)\\) is standardized using Z-scores before clustering:

$$
Z_{{i,j}} = \\frac{{f_{{i,j}} - \\mu_j^f}}{{\\sigma_j^f}}
$$

where \\(f_{{i,j}}\\) denotes feature \\(j\\) of stock \\(i\\), and \\(\\mu_j^f\\), \\(\\sigma_j^f\\) are the cross-sectional mean and standard deviation of feature \\(j\\).

### 2.3 K-Means Clustering & Evaluation
Let \\(K \\in \\{{{config.min_k}, \\ldots, {config.max_k}\\}}\\). For each candidate \\(K\\), the algorithm fits a K-Means model on the standardized feature matrix and computes the Silhouette Score:

$$
s(i) = \\frac{{b(i) - a(i)}}{{\\max \\{{a(i), b(i)\\}}}}
$$

where \\(a(i)\\) is the average within-cluster distance for stock \\(i\\) and \\(b(i)\\) is the minimum average distance to the nearest alternative cluster. The selected model is the one with the highest mean silhouette value; ties are broken toward the smaller \\(K\\) for parsimony.

## 3. Empirical Results
{build_empirical_result_paragraph(result)}

### 3.1 Data Inventory Snapshot
The table below summarizes the first available database records inspected by the pipeline during the latest execution.

{inventory_table}

### 3.2 Model Selection
{silhouette_table}

### 3.3 Cluster Interpretation
{cluster_table}

{cluster_text}

### 3.4 Visual Output
The risk-return scatter plot is written to `{relative_figure}` when at least one valid clustering solution is available. The X-axis denotes empirical volatility, the Y-axis denotes empirical mean log-return, colors identify clusters, and black `X` markers indicate centroids.

## 4. Conclusion
The implemented pipeline provides a clean separation between data engineering and research logic: `finmind-dl` remains responsible for populating SQLite databases, while the present strategy focuses exclusively on feature construction, unsupervised learning, visualization, and reporting. Once the Taiwan 50 constituent histories are fully populated under `data/*.sqlite`, the model can be rerun without modification to produce a behavior-driven segmentation of the universe.

From an applied perspective, this partition can support portfolio construction, peer comparison, and regime-sensitive risk management. Investors may use the clusters to distinguish defensive from aggressive exposures, identify cross-industry substitutes with similar realized dynamics, and monitor whether the latent market structure changes as new data arrive. The current implementation is intentionally lightweight, reproducible, and extensible, making it suitable as a base layer for more advanced hierarchical, regime-switching, or factor-augmented clustering studies.

## Code Availability
- Main entry point: `strategies/k-means/main.py`
- Core analysis module: `strategies/k-means/kmeans_clustering.py`
- Output directory: `strategies/k-means/outputs/`
"""


def run_analysis(config: ClusteringConfig) -> dict[str, Any]:
    load_project_environment()
    ensure_directory(config.output_dir)
    ensure_directory(config.report_path.parent)

    price_matrix, inventory = load_price_matrix(config.data_dir, config.table_name)
    log_returns = compute_log_returns(price_matrix)
    features = build_feature_matrix(log_returns, config.min_observations)

    inventory = inventory.copy() if not inventory.empty else pd.DataFrame()
    observation_counts = log_returns.notna().sum().to_dict() if not log_returns.empty else {}
    if not inventory.empty:
        inventory["valid_log_return_observations"] = (
            inventory["stock_id"].map(lambda stock_id: int(observation_counts.get(stock_id, 0)))
        )
        inventory["analysis_status"] = inventory["status"]
        inventory.loc[
            (inventory["status"] == "loaded")
            & (inventory["valid_log_return_observations"] < config.min_observations),
            "analysis_status",
        ] = "excluded_short_history"
        inventory.loc[
            (inventory["status"] == "loaded")
            & (inventory["valid_log_return_observations"] >= config.min_observations),
            "analysis_status",
        ] = "eligible"

    result: dict[str, Any] = {
        "run_timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "status": "pending_data",
        "table_name": config.table_name,
        "database_files_scanned": int(len(list(config.data_dir.glob("*.sqlite")))),
        "stocks_with_valid_prices": int((inventory["status"] == "loaded").sum()) if not inventory.empty else 0,
        "stocks_clustered": int(len(features)),
        "best_k": None,
        "best_silhouette_score": None,
        "figure_path": None,
        "inventory": inventory,
        "features": features,
        "silhouette_scores": pd.DataFrame(columns=["k", "silhouette_score"]),
        "assignments": pd.DataFrame(columns=["stock_id", "mean_return", "volatility", "observations", "cluster"]),
        "centroids": pd.DataFrame(columns=["cluster", "mean_return", "volatility"]),
        "cluster_summary": pd.DataFrame(columns=["cluster", "stock_count", "avg_mean_return", "avg_volatility", "member_stock_ids"]),
    }

    if len(features) >= 3:
        clustering_result = select_optimal_kmeans(
            features,
            min_k=config.min_k,
            max_k=config.max_k,
            random_state=config.random_state,
        )
        result.update(
            {
                "status": "success",
                "best_k": clustering_result["best_k"],
                "best_silhouette_score": clustering_result["best_silhouette_score"],
                "silhouette_scores": clustering_result["silhouette_scores"],
                "assignments": clustering_result["assignments"],
                "centroids": clustering_result["centroids"],
                "cluster_summary": clustering_result["cluster_summary"],
            }
        )
        figure_path = config.output_dir / FIGURE_FILENAME
        plot_clusters(
            result["assignments"],
            result["centroids"],
            best_k=result["best_k"],
            best_score=result["best_silhouette_score"],
            output_path=figure_path,
        )
        result["figure_path"] = figure_path

    inventory_path = config.output_dir / "data_inventory.csv"
    features_path = config.output_dir / "feature_matrix.csv"
    silhouette_path = config.output_dir / "silhouette_scores.csv"
    assignments_path = config.output_dir / "cluster_assignments.csv"
    summary_path = config.output_dir / "cluster_summary.csv"
    run_summary_path = config.output_dir / "run_summary.json"

    if not inventory.empty:
        inventory.to_csv(inventory_path, index=False)
    else:
        pd.DataFrame(
            [{"db_file": "", "stock_id": "", "status": "no_db_files", "reason": "No SQLite files were discovered."}]
        ).to_csv(inventory_path, index=False)
    features.to_csv(features_path, index=False)
    result["silhouette_scores"].to_csv(silhouette_path, index=False)
    result["assignments"].to_csv(assignments_path, index=False)
    result["cluster_summary"].to_csv(summary_path, index=False)

    run_summary = {
        "status": result["status"],
        "run_timestamp": result["run_timestamp"],
        "config": {
            key: str(value) if isinstance(value, Path) else value
            for key, value in asdict(config).items()
        },
        "database_files_scanned": result["database_files_scanned"],
        "stocks_with_valid_prices": result["stocks_with_valid_prices"],
        "stocks_clustered": result["stocks_clustered"],
        "best_k": result["best_k"],
        "best_silhouette_score": result["best_silhouette_score"],
        "figure_path": None if result["figure_path"] is None else display_path(result["figure_path"]),
    }
    run_summary_path.write_text(
        json.dumps(run_summary, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    silhouette_table, cluster_table = build_report_tables(result)
    result["inventory_preview"] = build_inventory_preview(inventory)
    result["silhouette_table"] = silhouette_table
    result["cluster_table"] = cluster_table

    report_text = render_report(result, config)
    config.report_path.write_text(report_text, encoding="utf-8")
    return result


def main(argv: list[str] | None = None) -> int:
    config = parse_args(argv)
    result = run_analysis(config)
    print(
        json.dumps(
            {
                "status": result["status"],
                "best_k": result["best_k"],
                "best_silhouette_score": result["best_silhouette_score"],
                "stocks_clustered": result["stocks_clustered"],
                "report_path": str(config.report_path),
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
