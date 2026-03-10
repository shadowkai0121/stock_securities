"""非線性距離導向的分群流程。"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import pandas as pd
from sklearn.cluster import AgglomerativeClustering

from evaluation.metrics import evaluate_clustering_quality, select_best_k
from .networks import compute_mutual_information_matrix, mi_to_distance


@dataclass
class ClusteringResult:
    labels: pd.Series
    best_k: int
    score_table: pd.DataFrame
    mi_matrix: pd.DataFrame
    distance_matrix: pd.DataFrame


class AdvancedClusterer:
    """以 MI 距離 + Agglomerative clustering 進行金融分群。"""

    def __init__(
        self,
        *,
        min_k: int = 2,
        max_k: int = 10,
        n_bins: int = 10,
        min_obs: int = 120,
        criterion: str = "silhouette",
    ) -> None:
        self.min_k = min_k
        self.max_k = max_k
        self.n_bins = n_bins
        self.min_obs = min_obs
        self.criterion = criterion

    @staticmethod
    def _fit_labels(distance_matrix: pd.DataFrame, n_clusters: int) -> pd.Series:
        model = AgglomerativeClustering(
            n_clusters=n_clusters,
            metric="precomputed",
            linkage="average",
        )
        labels = model.fit_predict(distance_matrix.to_numpy(dtype=float))
        return pd.Series(labels, index=distance_matrix.index, name="cluster")

    def fit(self, returns_df: pd.DataFrame) -> ClusteringResult:
        if returns_df.empty:
            raise ValueError("returns_df is empty.")
        if returns_df.shape[1] < 2:
            raise ValueError("At least 2 assets are required.")

        mi_matrix = compute_mutual_information_matrix(
            returns_df,
            n_bins=self.n_bins,
            min_obs=self.min_obs,
        )
        distance_matrix = mi_to_distance(mi_matrix)

        n_assets = distance_matrix.shape[0]
        upper_k = max(self.min_k, min(self.max_k, n_assets - 1))
        k_values = list(range(self.min_k, upper_k + 1))
        if not k_values:
            raise ValueError("No valid k values for clustering.")

        score_rows: list[dict[str, float | int]] = []
        label_map: dict[int, pd.Series] = {}
        feature_matrix = returns_df.T.fillna(0.0)

        for k in k_values:
            labels = self._fit_labels(distance_matrix, n_clusters=k)
            metrics = evaluate_clustering_quality(
                feature_matrix=feature_matrix,
                distance_matrix=distance_matrix,
                labels=labels,
            )
            score_rows.append({"k": int(k), **metrics})
            label_map[k] = labels

        score_table = pd.DataFrame(score_rows).sort_values("k").reset_index(drop=True)
        best_k = int(select_best_k(score_table, criterion=self.criterion))
        return ClusteringResult(
            labels=label_map[best_k],
            best_k=best_k,
            score_table=score_table,
            mi_matrix=mi_matrix,
            distance_matrix=distance_matrix,
        )
