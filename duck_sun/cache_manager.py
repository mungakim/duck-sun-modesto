"""
Unified Cache Manager for Duck Sun Modesto

Provides Last Known Good (LKG) caching with tiered staleness detection.
Ensures the PDF NEVER shows "--" - always returns SOME data.

Tiers:
- FRESH: < 10 minutes (real-time API)
- ACCEPTABLE: < 6 hours (cached, no warning)
- STALE_WARN: < 24 hours (cached, warn user)
- STALE_ERROR: > 24 hours (cached, error log)
- DEFAULT: No cache available (use hardcoded defaults)

Features:
- Persistent LKG storage per provider
- Analytics tracking -> outputs/lessons_learned.json
- Default values ensure PDF always has data
"""

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class CacheTier(Enum):
    """Data freshness tiers."""
    FRESH = "FRESH"            # < 10 minutes
    ACCEPTABLE = "ACCEPTABLE"  # < 6 hours
    STALE_WARN = "STALE_WARN"  # < 24 hours
    STALE_ERROR = "STALE_ERROR"  # > 24 hours
    DEFAULT = "DEFAULT"        # No cache, using defaults


@dataclass
class CacheEntry:
    """Cached data with metadata."""
    provider: str
    timestamp: datetime
    data: Any
    api_success: bool = True

    @property
    def age_minutes(self) -> float:
        return (datetime.now() - self.timestamp).total_seconds() / 60

    @property
    def age_hours(self) -> float:
        return self.age_minutes / 60

    @property
    def tier(self) -> CacheTier:
        hours = self.age_hours
        if hours < 0.167:  # 10 minutes
            return CacheTier.FRESH
        elif hours < 6:
            return CacheTier.ACCEPTABLE
        elif hours < 24:
            return CacheTier.STALE_WARN
        else:
            return CacheTier.STALE_ERROR


@dataclass
class FetchResult:
    """Result of a provider fetch with fallback chain info."""
    provider: str
    data: Any
    tier: CacheTier
    timestamp: datetime
    source: str  # "API", "CACHE", "DEFAULT"
    error_message: Optional[str] = None

    @property
    def is_degraded(self) -> bool:
        return self.tier in (CacheTier.STALE_WARN, CacheTier.STALE_ERROR, CacheTier.DEFAULT)

    @property
    def status_label(self) -> str:
        """Human-readable status for PDF annotation."""
        if self.tier == CacheTier.FRESH:
            return "LIVE"
        elif self.tier == CacheTier.ACCEPTABLE:
            return "CACHED"
        elif self.tier == CacheTier.STALE_WARN:
            hours = int((datetime.now() - self.timestamp).total_seconds() / 3600)
            return f"STALE ({hours}h)"
        elif self.tier == CacheTier.STALE_ERROR:
            hours = int((datetime.now() - self.timestamp).total_seconds() / 3600)
            return f"OLD ({hours}h)"
        else:
            return "DEFAULT"


class CacheManager:
    """
    Unified cache manager for all weather providers.

    Features:
    - Persistent Last Known Good (LKG) storage
    - Tiered staleness detection
    - Analytics tracking
    - Default values ensure PDF never shows "--"
    """

    CACHE_DIR = Path("outputs/cache")
    ANALYTICS_FILE = Path("outputs/lessons_learned.json")

    # Default values when ALL else fails - ensures PDF never shows "--"
    # These are reasonable Modesto winter values
    DEFAULT_VALUES: Dict[str, Any] = {
        "open_meteo": {
            "daily_summary": [],
            "daily_forecast": [],
            "hourly": [],
            "generated_at": "DEFAULT"
        },
        "hrrr": {
            "hourly": [],
            "daily_precip_prob": {},
            "status": "DEFAULT"
        },
        "nws": [],  # Empty list, handled gracefully by PDF
        "met_no": [],  # Empty list, handled gracefully by PDF
        "accuweather": [
            {"date": datetime.now().strftime("%Y-%m-%d"), "high_f": 55, "low_f": 40, "condition": "Default"},
            {"date": (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d"), "high_f": 56, "low_f": 41, "condition": "Default"},
            {"date": (datetime.now() + timedelta(days=2)).strftime("%Y-%m-%d"), "high_f": 57, "low_f": 42, "condition": "Default"},
            {"date": (datetime.now() + timedelta(days=3)).strftime("%Y-%m-%d"), "high_f": 58, "low_f": 43, "condition": "Default"},
            {"date": (datetime.now() + timedelta(days=4)).strftime("%Y-%m-%d"), "high_f": 59, "low_f": 44, "condition": "Default"},
        ],
        "weathercom": [
            {"date": datetime.now().strftime("%Y-%m-%d"), "high_f": 55, "low_f": 40, "condition": "Default"},
            {"date": (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d"), "high_f": 56, "low_f": 41, "condition": "Default"},
            {"date": (datetime.now() + timedelta(days=2)).strftime("%Y-%m-%d"), "high_f": 57, "low_f": 42, "condition": "Default"},
        ],
        "mid_org": {
            "today": {"high": "55", "low": "40", "condition": "Default"},
            "yesterday": {"high": "54", "low": "39", "condition": "Default"}
        },
        "metar": None,  # Ground truth - None is acceptable
        "smoke": None,  # Optional - None is acceptable
    }

    MAX_HISTORY_DAYS = 30

    def __init__(self):
        """Initialize cache manager, ensuring directories exist."""
        self.CACHE_DIR.mkdir(parents=True, exist_ok=True)
        self._analytics: Dict[str, Any] = self._load_analytics()

    def _cache_path(self, provider: str) -> Path:
        """Get cache file path for a provider."""
        return self.CACHE_DIR / f"{provider}_lkg.json"

    def _load_analytics(self) -> Dict[str, Any]:
        """Load analytics from lessons_learned.json."""
        if self.ANALYTICS_FILE.exists():
            try:
                with open(self.ANALYTICS_FILE, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                logger.debug(f"[CacheManager] Loaded analytics from {self.ANALYTICS_FILE}")
                return data
            except Exception as e:
                logger.warning(f"[CacheManager] Failed to load analytics: {e}")

        return {
            "version": "1.0",
            "last_updated": None,
            "total_runs": 0,
            "providers": {}
        }

    def _save_analytics(self) -> None:
        """Persist analytics to lessons_learned.json."""
        try:
            self._analytics["last_updated"] = datetime.now().isoformat()
            self.ANALYTICS_FILE.parent.mkdir(exist_ok=True)

            with open(self.ANALYTICS_FILE, 'w', encoding='utf-8') as f:
                json.dump(self._analytics, f, indent=2, default=str)

        except Exception as e:
            logger.error(f"[CacheManager] Failed to save analytics: {e}")

    def _ensure_provider_stats(self, provider: str) -> Dict[str, Any]:
        """Ensure provider entry exists in analytics."""
        if provider not in self._analytics["providers"]:
            self._analytics["providers"][provider] = {
                "total_fetches": 0,
                "api_successes": 0,
                "cache_hits": 0,
                "default_fallbacks": 0,
                "error_types": {},
                "staleness_distribution": {
                    "FRESH": 0,
                    "ACCEPTABLE": 0,
                    "STALE_WARN": 0,
                    "STALE_ERROR": 0,
                    "DEFAULT": 0
                }
            }
        return self._analytics["providers"][provider]

    def save_lkg(self, provider: str, data: Any, api_success: bool = True) -> None:
        """
        Save Last Known Good data for a provider.

        Args:
            provider: Provider name
            data: The data to cache
            api_success: Whether this came from a successful API call
        """
        cache_entry = {
            "provider": provider,
            "timestamp": datetime.now().isoformat(),
            "api_success": api_success,
            "data": data
        }

        try:
            with open(self._cache_path(provider), 'w', encoding='utf-8') as f:
                json.dump(cache_entry, f, indent=2, default=str)
            logger.debug(f"[CacheManager] LKG saved for {provider}")
        except Exception as e:
            logger.error(f"[CacheManager] Failed to save LKG for {provider}: {e}")

    def load_lkg(self, provider: str) -> Optional[CacheEntry]:
        """
        Load Last Known Good data for a provider.

        Returns:
            CacheEntry if cache exists, None otherwise
        """
        cache_path = self._cache_path(provider)
        if not cache_path.exists():
            return None

        try:
            with open(cache_path, 'r', encoding='utf-8') as f:
                raw = json.load(f)

            return CacheEntry(
                provider=raw["provider"],
                timestamp=datetime.fromisoformat(raw["timestamp"]),
                data=raw["data"],
                api_success=raw.get("api_success", True)
            )
        except Exception as e:
            logger.warning(f"[CacheManager] Failed to load LKG for {provider}: {e}")
            return None

    def get_with_fallback(
        self,
        provider: str,
        fresh_data: Optional[Any],
        api_error: Optional[str] = None
    ) -> FetchResult:
        """
        Get data using the tiered fallback chain.

        Priority:
        1. Fresh API data (if provided and not None)
        2. Cached data (with tier classification)
        3. Default values (last resort)

        NEVER returns None for data - always has SOMETHING.

        Args:
            provider: Provider name
            fresh_data: Data from API call (None if failed)
            api_error: Error message if API failed

        Returns:
            FetchResult with data guaranteed
        """
        now = datetime.now()
        stats = self._ensure_provider_stats(provider)
        stats["total_fetches"] += 1

        # Tier 1: Fresh API data
        if fresh_data is not None:
            self.save_lkg(provider, fresh_data, api_success=True)
            stats["api_successes"] += 1
            stats["staleness_distribution"]["FRESH"] += 1
            self._save_analytics()

            logger.info(f"[CacheManager] {provider}: FRESH data from API")

            return FetchResult(
                provider=provider,
                data=fresh_data,
                tier=CacheTier.FRESH,
                timestamp=now,
                source="API"
            )

        # Tiers 2-4: Cached data
        lkg = self.load_lkg(provider)
        if lkg is not None:
            tier = lkg.tier
            stats["cache_hits"] += 1
            stats["staleness_distribution"][tier.value] += 1
            self._save_analytics()

            if tier == CacheTier.ACCEPTABLE:
                logger.info(f"[CacheManager] {provider}: Using cached data ({lkg.age_hours:.1f}h old)")
            elif tier == CacheTier.STALE_WARN:
                logger.warning(f"[CacheManager] {provider}: Using STALE data ({lkg.age_hours:.1f}h old)")
            elif tier == CacheTier.STALE_ERROR:
                logger.error(f"[CacheManager] {provider}: Using VERY STALE data ({lkg.age_hours:.1f}h old)!")

            return FetchResult(
                provider=provider,
                data=lkg.data,
                tier=tier,
                timestamp=lkg.timestamp,
                source="CACHE",
                error_message=api_error
            )

        # Tier 5: Default values (last resort)
        logger.error(f"[CacheManager] {provider}: No cache! Using DEFAULT values")
        stats["default_fallbacks"] += 1
        stats["staleness_distribution"]["DEFAULT"] += 1

        # Record error type if provided
        if api_error:
            error_key = api_error.split(":")[0] if ":" in api_error else "Unknown"
            stats["error_types"][error_key] = stats["error_types"].get(error_key, 0) + 1

        self._save_analytics()

        return FetchResult(
            provider=provider,
            data=self.DEFAULT_VALUES.get(provider, {}),
            tier=CacheTier.DEFAULT,
            timestamp=now,
            source="DEFAULT",
            error_message=api_error or "No cache available"
        )

    def get_lessons_learned(self) -> Dict[str, Any]:
        """
        Get analytics summary for reporting.

        Returns:
            Summary with provider stats and reliability scores
        """
        summary = {
            "generated_at": datetime.now().isoformat(),
            "total_runs": self._analytics.get("total_runs", 0),
            "provider_stats": {}
        }

        for provider, stats in self._analytics.get("providers", {}).items():
            total = stats.get("total_fetches", 0)
            if total == 0:
                continue

            api_rate = stats.get("api_successes", 0) / total * 100
            cache_rate = stats.get("cache_hits", 0) / total * 100
            default_rate = stats.get("default_fallbacks", 0) / total * 100

            # Reliability score: API success weighted heavily, defaults penalized
            reliability = (api_rate * 0.6) + ((100 - default_rate) * 0.4)

            summary["provider_stats"][provider] = {
                "total_fetches": total,
                "api_success_rate": round(api_rate, 1),
                "cache_hit_rate": round(cache_rate, 1),
                "default_rate": round(default_rate, 1),
                "reliability_score": round(reliability, 1),
                "error_types": stats.get("error_types", {}),
                "staleness_distribution": stats.get("staleness_distribution", {})
            }

        return summary

    def increment_run_count(self) -> None:
        """Increment the total run counter."""
        self._analytics["total_runs"] = self._analytics.get("total_runs", 0) + 1
        self._save_analytics()

    def get_degraded_providers(self, results: Dict[str, 'FetchResult']) -> List[str]:
        """
        Get list of providers with degraded data quality.

        Args:
            results: Dict of provider name -> FetchResult

        Returns:
            List of provider names with STALE_WARN, STALE_ERROR, or DEFAULT tier
        """
        return [
            name for name, result in results.items()
            if result.is_degraded
        ]
