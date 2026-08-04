"""
Weighted Ensemble Engine for Duck Sun Modesto

Computes robust temperature consensus using weighted statistics
and detects high-variance conditions between weather sources.

Key Features:
1. Weighted median calculation (Google Weather weighted highest)
2. Outlier detection (flags sources > 2 stdev from median)
3. Variance classification (LOW/MODERATE/CRITICAL)
4. Confidence scoring based on source agreement

WEIGHTS (Calibrated Jan 2026):
- Google: 6.0 (MetNet-3 neural model - satellite/radar fusion)
- AccuWeather: 4.0 (Commercial provider)
- Weather.com: 4.0 (The Weather Channel - scraped)
- WUnderground: 4.0 (Weather Underground/IBM - scraped)
- NOAA: 3.0 (Government source)
- MID.org: 2.0 (Local microclimate)
- Open-Meteo: 1.0 (Fallback)

VARIANCE THRESHOLDS (in Fahrenheit):
- LOW: spread < 5°F (normal operation)
- MODERATE: spread 5-10°F (yellow warning)
- CRITICAL: spread > 10°F (red warning, detailed breakdown)
"""

import logging
import numpy as np
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Any

logger = logging.getLogger(__name__)


@dataclass
class ConsensusResult:
    """Result from weighted ensemble consensus calculation."""
    consensus_value: float
    variance_level: str  # "LOW", "MODERATE", "CRITICAL"
    spread_f: float  # Max - Min spread in Fahrenheit
    outliers: List[Tuple[str, float, float]]  # (source, value, delta_from_consensus)
    confidence: float  # 0-1 score based on agreement
    source_contributions: Dict[str, float]  # Source -> effective weight used
    diagnostics: Dict[str, Any]  # Full breakdown for logging


class WeightedEnsembleEngine:
    """
    Weighted ensemble engine for temperature consensus.

    Uses weighted median to compute consensus while respecting
    source hierarchy (Google > AccuWeather > NOAA > Open-Meteo).

    Outliers are flagged but NOT excluded from consensus - this
    is a "warn only" system that never blocks operations.
    """

    # Source weights (calibrated Jan 2026)
    SOURCE_WEIGHTS = {
        "Google": 6.0,        # MetNet-3 neural model - satellite/radar fusion
        "AccuWeather": 4.0,   # Commercial provider
        "Weather.com": 4.0,   # The Weather Channel (scraped)
        "WUnderground": 4.0,  # Weather Underground/IBM (scraped)
        "NOAA": 3.0,          # Government source
        "MID.org": 2.0,       # Local microclimate
        "Open-Meteo": 1.0,    # Fallback
    }

    # Variance thresholds (Fahrenheit)
    VARIANCE_LOW = 5.0
    VARIANCE_MODERATE = 5.0
    VARIANCE_CRITICAL = 10.0

    # Outlier detection threshold (standard deviations)
    OUTLIER_STDEV_THRESHOLD = 2.0

    def __init__(self):
        logger.info("[WeightedEnsembleEngine] Initializing with weighted consensus...")
        logger.info(f"[WeightedEnsembleEngine] Weights: {self.SOURCE_WEIGHTS}")

    def compute_consensus(
        self,
        sources: Dict[str, Optional[float]],
        unit: str = "C"
    ) -> ConsensusResult:
        """
        Compute weighted median consensus with outlier detection.

        Google is never demoted. It holds weight 6.0 in every hour regardless of
        how far it sits from the peer median, per the Jul 2026 calibration
        decision: Google (MetNet-3) has consistently been the most accurate
        source, so a disagreement with its peers is evidence against the peers,
        not against Google.

        Deviation is still measured and reported in diagnostics for
        observability, but it no longer changes any weight.

        Args:
            sources: Dict mapping source name to temperature value (or None)
            unit: "C" for Celsius, "F" for Fahrenheit

        Returns:
            ConsensusResult with consensus value and diagnostics
        """
        # Filter out None values
        valid_sources = {k: v for k, v in sources.items() if v is not None}

        if not valid_sources:
            logger.warning("[WeightedEnsembleEngine] No valid sources provided")
            return ConsensusResult(
                consensus_value=0.0,
                variance_level="CRITICAL",
                spread_f=0.0,
                outliers=[],
                confidence=0.0,
                source_contributions={},
                diagnostics={"error": "No valid sources"}
            )

        # === GOOGLE DEVIATION TRACKING (observability only - NEVER demotes) ===
        # Google keeps weight 6.0 unconditionally. We still measure how far it
        # sits from the peer median so the divergence is visible in diagnostics,
        # but that measurement no longer touches any weight.
        peers = [v for k, v in valid_sources.items() if k != "Google" and v is not None]
        peer_median = np.median(peers) if peers else None

        current_weights = self.SOURCE_WEIGHTS.copy()
        google_veto_triggered = False       # Retained for diagnostics schema; always False
        google_veto_severity = None

        google_val = valid_sources.get("Google")
        google_peer_delta_f = None
        if google_val is not None and peer_median is not None and len(peers) >= 2:
            google_peer_delta_f = abs(google_val - peer_median) * 1.8  # C delta -> F
            if google_peer_delta_f > 10.0:
                logger.debug(
                    f"[WeightedEnsembleEngine] Google {google_peer_delta_f:.1f}F from peer "
                    f"median - keeping full weight 6.0 (peers are the suspect ones)"
                )

        # Convert to arrays for calculation
        source_names = list(valid_sources.keys())
        values = np.array([valid_sources[s] for s in source_names])
        weights = np.array([current_weights.get(s, 1.0) for s in source_names])

        # Calculate unweighted median for outlier detection
        unweighted_median = np.median(values)

        # Calculate standard deviation
        stdev = np.std(values) if len(values) > 1 else 0.0

        # Detect outliers (> 2 stdev from unweighted median)
        outliers = []
        for name, value in valid_sources.items():
            delta = abs(value - unweighted_median)
            if stdev > 0 and delta > self.OUTLIER_STDEV_THRESHOLD * stdev:
                # Convert to F for reporting
                delta_f = delta * 9 / 5 if unit == "C" else delta
                value_f = value * 9 / 5 + 32 if unit == "C" else value
                outliers.append((name, value_f, delta_f))
                logger.warning(f"[WeightedEnsembleEngine] OUTLIER: {name} = {value_f:.1f}°F "
                             f"(delta: {delta_f:.1f}°F from median)")

        # Calculate WEIGHTED median
        consensus_value = self._weighted_median(values, weights)

        # Calculate spread (in original unit, then convert to F for thresholds)
        spread = float(np.max(values) - np.min(values))
        spread_f = spread * 9 / 5 if unit == "C" else spread

        # Determine variance level
        if spread_f < self.VARIANCE_LOW:
            variance_level = "LOW"
        elif spread_f < self.VARIANCE_CRITICAL:
            variance_level = "MODERATE"
        else:
            variance_level = "CRITICAL"

        # Calculate confidence (higher agreement = higher confidence)
        confidence = self._calculate_confidence(values, weights, consensus_value)

        # Build source contributions (normalized weights - use current_weights which may be modified)
        total_weight = sum(weights)
        source_contributions = {
            name: current_weights.get(name, 1.0) / total_weight
            for name in source_names
        }

        # Build diagnostics
        diagnostics = {
            "sources_used": len(valid_sources),
            "sources_available": list(valid_sources.keys()),
            "unweighted_median": unweighted_median,
            "weighted_median": consensus_value,
            "stdev": stdev,
            "spread": spread,
            "spread_f": spread_f,
            "unit": unit,
            "outlier_count": len(outliers),
            "raw_values": dict(valid_sources),
            "google_veto_triggered": google_veto_triggered,   # Always False - veto removed Jul 2026
            "google_veto_severity": google_veto_severity,     # Always None - veto removed Jul 2026
            "google_peer_delta_f": google_peer_delta_f,       # Observability only, never acted on
            "peer_median": peer_median,
            "effective_weights": {name: current_weights.get(name, 1.0) for name in source_names}
        }

        # Log summary
        if variance_level == "CRITICAL":
            logger.warning(f"[WeightedEnsembleEngine] CRITICAL VARIANCE: "
                         f"spread={spread_f:.1f}°F across {len(valid_sources)} sources")
        elif variance_level == "MODERATE":
            logger.info(f"[WeightedEnsembleEngine] Moderate variance: "
                       f"spread={spread_f:.1f}°F across {len(valid_sources)} sources")
        else:
            logger.debug(f"[WeightedEnsembleEngine] Low variance: "
                        f"spread={spread_f:.1f}°F, consensus={consensus_value:.1f}")

        return ConsensusResult(
            consensus_value=consensus_value,
            variance_level=variance_level,
            spread_f=spread_f,
            outliers=outliers,
            confidence=confidence,
            source_contributions=source_contributions,
            diagnostics=diagnostics
        )

    def _weighted_median(self, values: np.ndarray, weights: np.ndarray) -> float:
        """
        Calculate weighted median.

        The weighted median is the value where cumulative weight reaches 50%.
        """
        if len(values) == 0:
            return 0.0

        if len(values) == 1:
            return float(values[0])

        # Sort by values
        sorted_indices = np.argsort(values)
        sorted_values = values[sorted_indices]
        sorted_weights = weights[sorted_indices]

        # Normalize weights
        cumulative_weight = np.cumsum(sorted_weights)
        total_weight = cumulative_weight[-1]

        # Find the weighted median (where cumulative reaches 50%)
        median_idx = np.searchsorted(cumulative_weight, total_weight / 2)

        # Handle edge cases
        if median_idx >= len(sorted_values):
            median_idx = len(sorted_values) - 1

        return float(sorted_values[median_idx])

    def _calculate_confidence(
        self,
        values: np.ndarray,
        weights: np.ndarray,
        consensus: float
    ) -> float:
        """
        Calculate confidence score based on weighted agreement.

        Higher agreement (lower weighted variance) = higher confidence.
        Returns value between 0 and 1.
        """
        if len(values) <= 1:
            return 1.0  # Single source = full confidence in that source

        # Calculate weighted variance from consensus
        deviations = np.abs(values - consensus)
        weighted_deviation = np.average(deviations, weights=weights)

        # Convert to confidence (inverse relationship)
        # Use a sigmoid-like transformation
        # Higher deviation = lower confidence
        # Scale factor: 5°C deviation = ~0.5 confidence
        scale = 5.0  # Celsius scale factor
        confidence = 1.0 / (1.0 + weighted_deviation / scale)

        return round(confidence, 3)

    def compute_daily_consensus(
        self,
        source_highs: Dict[str, Optional[float]],
        source_lows: Dict[str, Optional[float]],
        unit: str = "C"
    ) -> Dict[str, ConsensusResult]:
        """
        Compute consensus for both high and low temperatures.

        Args:
            source_highs: Dict of source -> high temperature
            source_lows: Dict of source -> low temperature
            unit: Temperature unit

        Returns:
            Dict with 'high' and 'low' ConsensusResult objects
        """
        return {
            "high": self.compute_consensus(source_highs, unit),
            "low": self.compute_consensus(source_lows, unit)
        }

    def get_variance_report(
        self,
        results: List[ConsensusResult]
    ) -> Dict[str, Any]:
        """
        Generate summary report across multiple consensus calculations.

        Args:
            results: List of ConsensusResult objects

        Returns:
            Summary report with counts and statistics
        """
        if not results:
            return {"total": 0}

        levels = {"LOW": 0, "MODERATE": 0, "CRITICAL": 0}
        total_outliers = 0
        avg_confidence = 0.0
        max_spread = 0.0

        for r in results:
            levels[r.variance_level] = levels.get(r.variance_level, 0) + 1
            total_outliers += len(r.outliers)
            avg_confidence += r.confidence
            max_spread = max(max_spread, r.spread_f)

        avg_confidence /= len(results)

        return {
            "total": len(results),
            "variance_counts": levels,
            "total_outliers": total_outliers,
            "avg_confidence": round(avg_confidence, 3),
            "max_spread_f": round(max_spread, 1),
            "has_critical": levels.get("CRITICAL", 0) > 0
        }


# Convenience function for simple consensus
def quick_consensus(
    google: Optional[float] = None,
    noaa: Optional[float] = None,
    accuweather: Optional[float] = None,
    weather_com: Optional[float] = None,
    wunderground: Optional[float] = None,
    mid_org: Optional[float] = None,
    open_meteo: Optional[float] = None,
    unit: str = "C"
) -> ConsensusResult:
    """
    Quick consensus calculation with named parameters.

    Example:
        result = quick_consensus(google=7.0, noaa=7.2, accuweather=7.1)
        print(f"Consensus: {result.consensus_value}°C")
    """
    engine = WeightedEnsembleEngine()
    sources = {
        "Google": google,
        "NOAA": noaa,
        "AccuWeather": accuweather,
        "Weather.com": weather_com,
        "WUnderground": wunderground,
        "MID.org": mid_org,
        "Open-Meteo": open_meteo
    }
    return engine.compute_consensus(sources, unit)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)

    print("=" * 60)
    print("  WEIGHTED ENSEMBLE ENGINE TEST")
    print("=" * 60)

    engine = WeightedEnsembleEngine()

    # Test 1: Normal agreement (LOW variance)
    print("\n[TEST 1] Normal agreement (all sources close)")
    sources_1 = {
        "NOAA": 7.2,
        "AccuWeather": 7.5,
        "Open-Meteo": 7.1
    }
    result_1 = engine.compute_consensus(sources_1)
    print(f"  Consensus: {result_1.consensus_value:.1f}°C")
    print(f"  Variance: {result_1.variance_level} (spread: {result_1.spread_f:.1f}°F)")
    print(f"  Confidence: {result_1.confidence:.2f}")

    # Test 2: Moderate disagreement
    print("\n[TEST 2] Moderate disagreement")
    sources_2 = {
        "NOAA": 7.0,
        "AccuWeather": 8.5,
        "Open-Meteo": 7.5
    }
    result_2 = engine.compute_consensus(sources_2)
    print(f"  Consensus: {result_2.consensus_value:.1f}°C")
    print(f"  Variance: {result_2.variance_level} (spread: {result_2.spread_f:.1f}°F)")
    print(f"  Outliers: {result_2.outliers}")

    # Test 3: Critical variance (one source way off)
    print("\n[TEST 3] Critical variance (NOAA cold bias example)")
    sources_3 = {
        "NOAA": 2.0,      # Cold bias: -7°F
        "AccuWeather": 7.0,
        "Open-Meteo": 7.5
    }
    result_3 = engine.compute_consensus(sources_3)
    print(f"  Consensus: {result_3.consensus_value:.1f}°C")
    print(f"  Variance: {result_3.variance_level} (spread: {result_3.spread_f:.1f}°F)")
    print(f"  Outliers: {[f'{o[0]}={o[1]:.0f}°F' for o in result_3.outliers]}")
    print(f"  Confidence: {result_3.confidence:.2f}")

    # Test 4: Few sources (graceful degradation)
    print("\n[TEST 4] Few sources available")
    sources_4 = {
        "NOAA": 7.0,
        "Open-Meteo": 7.5
    }
    result_4 = engine.compute_consensus(sources_4)
    print(f"  Consensus: {result_4.consensus_value:.1f}°C")
    print(f"  Variance: {result_4.variance_level}")
    print(f"  Sources used: {result_4.diagnostics['sources_used']}")

    # Summary report
    print("\n[VARIANCE REPORT]")
    report = engine.get_variance_report([result_1, result_2, result_3, result_4])
    print(f"  Total calculations: {report['total']}")
    print(f"  Variance counts: {report['variance_counts']}")
    print(f"  Total outliers flagged: {report['total_outliers']}")
    print(f"  Average confidence: {report['avg_confidence']:.2f}")

    print("\n" + "=" * 60)
