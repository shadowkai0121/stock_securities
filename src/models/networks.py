"""複雜網路拓撲與非線性距離測度。

重點是用 Mutual Information (MI) 捕捉非線性共變動，再建立金融網路：
- MST: 保留全市場最關鍵連線，觀察主幹風險傳導路徑。
- PMFG: 在平面限制下保留更多邊，兼顧資訊量與可解釋性。
"""

from __future__ import annotations

from itertools import combinations
from typing import Iterable

import networkx as nx
import numpy as np
import pandas as pd
from sklearn.metrics import mutual_info_score


def _discretize_series(series: pd.Series, n_bins: int) -> pd.Series:
    """將連續報酬離散化，供 MI 計算。

    為了降低厚尾與極端值對分箱的影響，優先用分位數分箱（qcut）；
    若資料重複值太多導致 qcut 失敗，再退回等寬分箱（cut）。
    """
    clean = pd.to_numeric(series, errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
    if clean.empty:
        return pd.Series(dtype="int64")

    unique_count = clean.nunique(dropna=True)
    bins = int(max(2, min(n_bins, unique_count)))
    if bins < 2:
        return pd.Series(np.zeros(len(clean), dtype=int), index=clean.index)

    try:
        bucket = pd.qcut(clean, q=bins, labels=False, duplicates="drop")
    except ValueError:
        bucket = pd.cut(clean, bins=bins, labels=False, duplicates="drop")

    return pd.Series(bucket, index=clean.index, dtype="float64").astype("Int64")


def _normalized_mi(x: pd.Series, y: pd.Series, n_bins: int, min_obs: int) -> float:
    """計算 Normalized Mutual Information (NMI)。

    為避免不同 pair 的熵尺度不一致，將 MI 除以 sqrt(H(X)H(Y))：
    NMI in [0,1]，值越大代表非線性依賴越強。
    """
    pair = pd.concat([x.rename("x"), y.rename("y")], axis=1).dropna()
    if len(pair) < min_obs:
        return np.nan

    x_bin = _discretize_series(pair["x"], n_bins=n_bins)
    y_bin = _discretize_series(pair["y"], n_bins=n_bins)
    pair_bin = pd.concat([x_bin.rename("x"), y_bin.rename("y")], axis=1).dropna()
    if len(pair_bin) < min_obs:
        return np.nan

    xi = pair_bin["x"].astype(int).to_numpy()
    yi = pair_bin["y"].astype(int).to_numpy()
    mi = float(mutual_info_score(xi, yi))

    hx = float(mutual_info_score(xi, xi))
    hy = float(mutual_info_score(yi, yi))
    denom = float(np.sqrt(hx * hy))
    if denom <= 0 or not np.isfinite(denom):
        return 0.0

    nmi = mi / denom
    return float(np.clip(nmi, 0.0, 1.0))


def compute_mutual_information_matrix(
    returns_df: pd.DataFrame,
    *,
    n_bins: int = 10,
    min_obs: int = 120,
) -> pd.DataFrame:
    """計算股票間 NMI 相依矩陣。

    參數假設：
    - index: 日期
    - columns: stock_id
    - values: 報酬率（建議用殘差報酬或 log-return）

    學術意義：
    - Pearson 只捕捉線性共變，MI 可識別更廣泛的非線性關係。
    - 在高噪音市場中，MI 通常較能保留真實連動結構。
    """
    if returns_df.empty:
        raise ValueError("returns_df is empty.")
    if returns_df.shape[1] < 2:
        raise ValueError("At least 2 assets are required to compute MI matrix.")

    cols = list(returns_df.columns)
    mi = pd.DataFrame(np.eye(len(cols), dtype=float), index=cols, columns=cols)

    numeric_df = returns_df.apply(pd.to_numeric, errors="coerce")
    for left, right in combinations(cols, 2):
        value = _normalized_mi(
            numeric_df[left],
            numeric_df[right],
            n_bins=n_bins,
            min_obs=min_obs,
        )
        mi.loc[left, right] = value
        mi.loc[right, left] = value

    return mi


def mi_to_distance(mi_matrix: pd.DataFrame) -> pd.DataFrame:
    """將 MI 相似度矩陣轉換成距離矩陣。

    轉換式：
        d_ij = sqrt(2 * (1 - s_ij))
    其中 s_ij 為 [0,1] 相似度（此處為 NMI）。
    形式上對齊 Mantegna correlation distance，便於套用圖論濾波。
    """
    if mi_matrix.empty:
        raise ValueError("mi_matrix is empty.")
    if mi_matrix.shape[0] != mi_matrix.shape[1]:
        raise ValueError("mi_matrix must be square.")
    if not mi_matrix.index.equals(mi_matrix.columns):
        raise ValueError("mi_matrix index/columns must match.")

    sim = mi_matrix.astype(float).clip(lower=0.0, upper=1.0).fillna(0.0)
    dist_values = np.sqrt(np.clip(2.0 * (1.0 - sim.to_numpy(dtype=float)), a_min=0.0, a_max=None))
    np.fill_diagonal(dist_values, 0.0)
    return pd.DataFrame(dist_values, index=sim.index, columns=sim.columns)


def _build_complete_graph_from_distance(distance_matrix: pd.DataFrame) -> nx.Graph:
    graph = nx.Graph()
    nodes = list(distance_matrix.index)
    graph.add_nodes_from(nodes)

    for left, right in combinations(nodes, 2):
        distance = float(distance_matrix.loc[left, right])
        if not np.isfinite(distance):
            continue
        similarity = float(1.0 - np.clip(distance**2 / 2.0, 0.0, 1.0))
        graph.add_edge(left, right, weight=distance, similarity=similarity)
    return graph


def build_mst(
    *,
    distance_matrix: pd.DataFrame | None = None,
    mi_matrix: pd.DataFrame | None = None,
) -> nx.Graph:
    """基於距離矩陣建立最小生成樹（MST）。"""
    if distance_matrix is None:
        if mi_matrix is None:
            raise ValueError("Either distance_matrix or mi_matrix must be provided.")
        distance_matrix = mi_to_distance(mi_matrix)

    if distance_matrix.shape[0] < 2:
        raise ValueError("Need at least 2 nodes for MST.")
    if not distance_matrix.index.equals(distance_matrix.columns):
        raise ValueError("distance_matrix index/columns must match.")

    full_graph = _build_complete_graph_from_distance(distance_matrix)
    if not nx.is_connected(full_graph):
        raise ValueError("Input graph is disconnected; MST is not uniquely definable.")

    return nx.minimum_spanning_tree(full_graph, algorithm="kruskal", weight="weight")


def build_pmfg(
    similarity_matrix: pd.DataFrame,
    *,
    min_similarity: float = 0.0,
) -> nx.Graph:
    """建立 PMFG（Planar Maximally Filtered Graph）的 greedy 版本。

    演算法：
    1. 將邊依相似度由高到低排序。
    2. 逐條嘗試加入，若破壞平面性則回退。
    3. 達到 3*(N-2) 條邊（最大平面圖）即停止。
    """
    if similarity_matrix.empty:
        raise ValueError("similarity_matrix is empty.")
    if similarity_matrix.shape[0] < 3:
        raise ValueError("PMFG requires at least 3 nodes.")
    if not similarity_matrix.index.equals(similarity_matrix.columns):
        raise ValueError("similarity_matrix index/columns must match.")

    sim = similarity_matrix.astype(float).clip(lower=0.0, upper=1.0).fillna(0.0)
    nodes = list(sim.index)
    target_edges = 3 * (len(nodes) - 2)

    candidate_edges: list[tuple[str, str, float]] = []
    for left, right in combinations(nodes, 2):
        s = float(sim.loc[left, right])
        if np.isfinite(s) and s >= min_similarity:
            candidate_edges.append((left, right, s))
    candidate_edges.sort(key=lambda row: row[2], reverse=True)

    graph = nx.Graph()
    graph.add_nodes_from(nodes)
    for left, right, similarity in candidate_edges:
        graph.add_edge(left, right, weight=float(1.0 - similarity), similarity=similarity)
        is_planar, _ = nx.check_planarity(graph)
        if not is_planar:
            graph.remove_edge(left, right)
            continue
        if graph.number_of_edges() >= target_edges:
            break
    return graph


def identify_core_periphery(
    graph: nx.Graph,
    *,
    core_quantile: float = 0.7,
) -> pd.DataFrame:
    """根據圖中心性指標標記核心股 / 邊緣股。

    指標融合：
    - Degree centrality: 連結數，反映「關聯廣度」。
    - Betweenness: 中介性，反映「風險傳導橋樑」。
    - Eigenvector centrality: 鄰居品質加權，反映「系統重要性」。
    """
    if graph.number_of_nodes() == 0:
        return pd.DataFrame(columns=["stock_id", "degree", "betweenness", "eigenvector", "core_score", "role"])

    degree = nx.degree_centrality(graph)
    betweenness = nx.betweenness_centrality(graph, weight="weight", normalized=True)
    try:
        eigen = nx.eigenvector_centrality_numpy(graph, weight="similarity")
    except Exception:  # pragma: no cover - fallback for singular cases
        eigen = {node: np.nan for node in graph.nodes}

    stats = pd.DataFrame(
        {
            "stock_id": list(graph.nodes),
            "degree": [float(degree.get(n, np.nan)) for n in graph.nodes],
            "betweenness": [float(betweenness.get(n, np.nan)) for n in graph.nodes],
            "eigenvector": [float(eigen.get(n, np.nan)) for n in graph.nodes],
        }
    )
    for col in ["degree", "betweenness", "eigenvector"]:
        stats[f"{col}_rank"] = stats[col].rank(pct=True, method="average")

    stats["core_score"] = stats[
        ["degree_rank", "betweenness_rank", "eigenvector_rank"]
    ].mean(axis=1)
    threshold = float(stats["core_score"].quantile(core_quantile))
    stats["role"] = np.where(stats["core_score"] >= threshold, "core", "periphery")
    return stats.sort_values("core_score", ascending=False).reset_index(drop=True)


def graph_to_edge_frame(graph: nx.Graph) -> pd.DataFrame:
    """將網路邊輸出為表格（便於存檔或繪圖）。"""
    rows: list[dict[str, float | str]] = []
    for left, right, attrs in graph.edges(data=True):
        rows.append(
            {
                "source": str(left),
                "target": str(right),
                "distance": float(attrs.get("weight", np.nan)),
                "similarity": float(attrs.get("similarity", np.nan)),
            }
        )
    return pd.DataFrame(rows)
