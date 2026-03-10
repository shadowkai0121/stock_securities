"""Feature store with reproducible version-aware local caching."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Sequence

import pandas as pd

from features import feature_defs


class FeatureStore:
    """Compute and cache reusable research features."""

    def __init__(
        self,
        *,
        cache_dir: str | Path = "data/feature_cache",
        version: str = "v1",
    ) -> None:
        self.cache_dir = Path(cache_dir)
        self.version = version
        self.base_dir = self.cache_dir / self.version
        self.base_dir.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _hash_payload(payload: dict[str, Any]) -> str:
        raw = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
        return hashlib.sha256(raw).hexdigest()

    def build_cache_key(
        self,
        *,
        feature_set_name: str,
        universe_definition: dict[str, Any],
        feature_definition: dict[str, Any],
    ) -> str:
        payload = {
            "feature_set_name": feature_set_name,
            "version": self.version,
            "universe_definition": universe_definition,
            "feature_definition": feature_definition,
        }
        return self._hash_payload(payload)[:24]

    def _feature_path(self, key: str) -> Path:
        return self.base_dir / f"{key}.parquet"

    def _metadata_path(self, key: str) -> Path:
        return self.base_dir / f"{key}.metadata.json"

    def feature_path(self, key: str) -> Path:
        """Return parquet path for a cached feature key."""

        return self._feature_path(key)

    def load(self, key: str) -> pd.DataFrame | None:
        path = self._feature_path(key)
        if not path.exists():
            return None
        return pd.read_parquet(path)

    def save(self, *, key: str, frame: pd.DataFrame, metadata: dict[str, Any]) -> None:
        frame.to_parquet(self._feature_path(key), index=False)
        self._metadata_path(key).write_text(
            json.dumps(metadata, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def metadata(self, key: str) -> dict[str, Any] | None:
        path = self._metadata_path(key)
        if not path.exists():
            return None
        return json.loads(path.read_text(encoding="utf-8"))

    def build_features(
        self,
        *,
        price_df: pd.DataFrame,
        margin_df: pd.DataFrame | None = None,
        broker_flow_df: pd.DataFrame | None = None,
        holding_df: pd.DataFrame | None = None,
        ma_windows: Sequence[int] = (5, 20, 60),
        vol_windows: Sequence[int] = (20, 60),
    ) -> pd.DataFrame:
        """Build strategy-agnostic feature panel."""

        panel = feature_defs.simple_returns(price_df)
        panel = panel.merge(feature_defs.log_returns(price_df), on=["date", "stock_id"], how="outer")
        panel = panel.merge(feature_defs.turnover_proxy(price_df), on=["date", "stock_id"], how="outer")

        for window in ma_windows:
            panel = panel.merge(
                feature_defs.moving_average(price_df, window=int(window)),
                on=["date", "stock_id"],
                how="outer",
            )
        for window in vol_windows:
            panel = panel.merge(
                feature_defs.rolling_volatility(price_df, window=int(window)),
                on=["date", "stock_id"],
                how="outer",
            )

        if margin_df is not None and not margin_df.empty:
            panel = panel.merge(
                feature_defs.margin_ratios(margin_df),
                on=["date", "stock_id"],
                how="left",
            )
        if broker_flow_df is not None and not broker_flow_df.empty:
            panel = panel.merge(
                feature_defs.broker_features(broker_flow_df),
                on=["date", "stock_id"],
                how="left",
            )
        if holding_df is not None and not holding_df.empty:
            panel = panel.merge(
                feature_defs.holding_share_features(holding_df),
                on=["date", "stock_id"],
                how="left",
            )

        panel["date"] = pd.to_datetime(panel["date"], errors="coerce")
        panel = panel[panel["date"].notna()].copy()
        panel["date"] = panel["date"].dt.strftime("%Y-%m-%d")
        panel = panel.sort_values(["date", "stock_id"]).reset_index(drop=True)
        return panel

    def get_or_create(
        self,
        *,
        key: str,
        builder: callable,
        metadata: dict[str, Any],
    ) -> pd.DataFrame:
        cached = self.load(key)
        if cached is not None:
            return cached
        frame = builder()
        self.save(key=key, frame=frame, metadata=metadata)
        return frame
