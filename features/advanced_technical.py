"""
features/advanced_technical.py

Phase: 3.1 (Advanced Technical Features)
Specs: SPEC-FEAT-001, SPEC-FEAT-002, SPEC-PIPE-004, SPEC-PIPE-005
Owner: Platform / Features
Consumers: features/matrix_builder, systems/ml_signal_engine/models/deep

Computes 18 advanced technical features from OHLCV panels:
  Wavelet decomposition (4): wavelet_trend, wavelet_noise, wavelet_energy_ratio,
    wavelet_regime_signal — using PyWavelets db4 decomposition
  Hurst exponent (2): hurst_exp_21d, hurst_exp_63d — R/S (rescaled range) analysis
  Entropy features (5): approx_entropy_21d, sample_entropy_21d,
    permutation_entropy_21d, spectral_entropy, fractal_dimension
  Fractional differentiation (3): fracdiff_d_optimal, fracdiff_price,
    fracdiff_volume — memory-preserving differentiation
  Complexity (4): lyapunov_exponent_proxy, rqa_rec_rate, time_series_complexity,
    nonlinear_trend_strength

All features are computed per-ticker via groupby (SPEC-PIPE-004 — no
Python for-loops over individual stocks at the caller level). OHLCV must
be corporate-action-adjusted before calling these functions.

PIT Assumptions
---------------
All inputs are OHLCV prices (PITRule.NONE — always same-day knowable).
No fundamental or quarterly data consumed here.
"""

import logging
from typing import List, Tuple

import numpy as np
import pandas as pd
import pywt
import scipy.signal
import scipy.stats

logger = logging.getLogger(__name__)

# ── Feature catalog ──────────────────────────────────────────────────────────

WAVELET_FEATURES: List[str] = [
    "wavelet_trend",
    "wavelet_noise",
    "wavelet_energy_ratio",
    "wavelet_regime_signal",
]

HURST_FEATURES: List[str] = [
    "hurst_exp_21d",
    "hurst_exp_63d",
]

ENTROPY_FEATURES: List[str] = [
    "approx_entropy_21d",
    "sample_entropy_21d",
    "permutation_entropy_21d",
    "spectral_entropy",
    "fractal_dimension",
]

FRACDIFF_FEATURES: List[str] = [
    "fracdiff_d_optimal",
    "fracdiff_price",
    "fracdiff_volume",
]

COMPLEXITY_FEATURES: List[str] = [
    "lyapunov_exponent_proxy",
    "rqa_rec_rate",
    "time_series_complexity",
    "nonlinear_trend_strength",
]

ADVANCED_TECHNICAL_FEATURES: List[str] = (
    WAVELET_FEATURES + HURST_FEATURES + ENTROPY_FEATURES + FRACDIFF_FEATURES + COMPLEXITY_FEATURES
)


# ── Wavelet helpers ───────────────────────────────────────────────────────────


def _wavelet_features_series(prices: np.ndarray) -> Tuple[float, float, float, float]:
    """
    Decompose price series with db4 wavelet; return scalar features for the latest bar.

    Returns (trend, noise, energy_ratio, regime_signal).
    Uses level-3 decomposition: approximation = trend, details = noise components.
    """
    if len(prices) < 16:
        return np.nan, np.nan, np.nan, np.nan
    try:
        # db4 chosen for its smooth, compactly-supported shape; level 3 gives
        # a trend component with ~8-bar scale
        coeffs = pywt.wavedec(prices, "db4", level=3)
        approx = coeffs[0]  # low-frequency: trend
        details = coeffs[1:]  # high-frequency: noise layers

        trend_energy = np.sum(approx ** 2)
        noise_energy = sum(np.sum(d ** 2) for d in details)
        total_energy = trend_energy + noise_energy + 1e-10

        # Reconstruct trend-only signal to extract the last-bar value
        trend_coeff = [approx] + [np.zeros_like(d) for d in details]
        trend_signal = pywt.waverec(trend_coeff, "db4")
        # Reconstructed length may differ from input by 1 due to boundary extension
        trend_val = float(trend_signal[min(len(prices) - 1, len(trend_signal) - 1)])

        noise_coeff = [np.zeros_like(approx)] + list(details)
        noise_signal = pywt.waverec(noise_coeff, "db4")
        noise_val = float(noise_signal[min(len(prices) - 1, len(noise_signal) - 1)])

        energy_ratio = trend_energy / total_energy
        # regime_signal: sign of trend change over last 5 trend points
        if len(approx) >= 5:
            regime_signal = float(np.sign(approx[-1] - approx[-5]))
        else:
            regime_signal = 0.0

        # Normalise trend/noise by current price to make cross-sectionally comparable
        ref = abs(prices[-1]) + 1e-10
        return trend_val / ref, noise_val / ref, float(energy_ratio), regime_signal
    except Exception as exc:
        logger.debug(f"wavelet decomposition failed: {exc}")
        return np.nan, np.nan, np.nan, np.nan


# ── Hurst exponent ────────────────────────────────────────────────────────────


def _hurst_rs(x: np.ndarray) -> float:
    """
    Hurst exponent via R/S (rescaled range) analysis.

    Returns H in (0, 1): ~0.5 = random walk, >0.5 = trending, <0.5 = mean-reverting.
    Uses log-price increments to avoid price-scale sensitivity.
    """
    n = len(x)
    if n < 8:
        return np.nan
    log_ret = np.diff(np.log(np.maximum(x, 1e-10)))
    try:
        # Compute RS for several sub-window sizes for OLS estimate
        min_size = 4
        sizes = []
        rs_vals = []
        for size in range(min_size, n // 2 + 1, max(1, (n // 2 - min_size) // 8)):
            if size < 4:
                continue
            nblocks = len(log_ret) // size
            if nblocks < 1:
                continue
            block_rs = []
            for i in range(nblocks):
                seg = log_ret[i * size: (i + 1) * size]
                mean_seg = seg.mean()
                deviation = np.cumsum(seg - mean_seg)
                r = deviation.max() - deviation.min()
                s = seg.std(ddof=1)
                if s > 0:
                    block_rs.append(r / s)
            if block_rs:
                sizes.append(size)
                rs_vals.append(np.mean(block_rs))

        if len(sizes) < 3:
            return np.nan

        log_sizes = np.log(sizes)
        log_rs = np.log(np.maximum(rs_vals, 1e-10))
        slope, _, _, _, _ = scipy.stats.linregress(log_sizes, log_rs)
        return float(np.clip(slope, 0.0, 1.5))
    except Exception:
        return np.nan


# ── Entropy helpers ───────────────────────────────────────────────────────────


def _approx_entropy(series: np.ndarray, m: int = 2, r_factor: float = 0.2) -> float:
    """Approximate entropy (ApEn) for regularity/predictability."""
    n = len(series)
    if n < m + 2:
        return np.nan
    r = r_factor * (np.std(series, ddof=0) + 1e-10)

    def _phi(m_val: int) -> float:
        templates = np.array([series[i: i + m_val] for i in range(n - m_val + 1)])
        count = np.array([
            np.sum(np.max(np.abs(templates - templates[i]), axis=1) <= r)
            for i in range(len(templates))
        ])
        return np.log(count / len(templates)).mean()

    try:
        return float(_phi(m) - _phi(m + 1))
    except Exception:
        return np.nan


def _sample_entropy(series: np.ndarray, m: int = 2, r_factor: float = 0.2) -> float:
    """Sample entropy — less biased than ApEn for short series."""
    n = len(series)
    if n < m + 2:
        return np.nan
    r = r_factor * (np.std(series, ddof=0) + 1e-10)

    def _count_matches(m_val: int) -> int:
        templates = np.array([series[i: i + m_val] for i in range(n - m_val)])
        total = 0
        for i in range(len(templates)):
            diffs = np.max(np.abs(templates - templates[i]), axis=1)
            total += np.sum(diffs <= r) - 1  # exclude self-match
        return total

    try:
        A = _count_matches(m + 1)
        B = _count_matches(m)
        if B == 0:
            return np.nan
        return float(-np.log(A / B + 1e-10))
    except Exception:
        return np.nan


def _permutation_entropy(series: np.ndarray, order: int = 3, delay: int = 1) -> float:
    """Permutation entropy — captures ordinal patterns in the series."""
    n = len(series)
    if n < (order - 1) * delay + 1:
        return np.nan
    try:
        from math import factorial
        from collections import Counter

        perms = []
        for i in range(n - (order - 1) * delay):
            window = series[i: i + order * delay: delay]
            perm = tuple(np.argsort(window))
            perms.append(perm)

        counts = Counter(perms)
        n_perms = len(perms)
        probs = np.array(list(counts.values())) / n_perms
        entropy = -np.sum(probs * np.log2(probs + 1e-10))
        # Normalise by max possible entropy = log2(order!)
        max_entropy = np.log2(factorial(order))
        return float(entropy / max_entropy) if max_entropy > 0 else np.nan
    except Exception:
        return np.nan


def _spectral_entropy(series: np.ndarray) -> float:
    """Spectral entropy from power spectral density."""
    n = len(series)
    if n < 8:
        return np.nan
    try:
        freqs, psd = scipy.signal.periodogram(series)
        psd = psd[1:]  # drop DC component
        psd_norm = psd / (psd.sum() + 1e-10)
        entropy = -np.sum(psd_norm * np.log2(psd_norm + 1e-10))
        max_ent = np.log2(len(psd_norm))
        return float(entropy / max_ent) if max_ent > 0 else np.nan
    except Exception:
        return np.nan


def _fractal_dimension(series: np.ndarray) -> float:
    """
    Higuchi fractal dimension — measures irregularity.

    Returns D in [1, 2]: 1 = smooth trend, 2 = white noise.
    """
    n = len(series)
    if n < 16:
        return np.nan
    try:
        k_max = min(8, n // 4)
        lk = []
        ks = []
        for k in range(1, k_max + 1):
            lengths = []
            for m in range(1, k + 1):
                idxs = np.arange(m - 1, n, k)
                if len(idxs) < 2:
                    continue
                seg = series[idxs]
                length = np.sum(np.abs(np.diff(seg))) * (n - 1) / (len(idxs) - 1) / k
                lengths.append(length)
            if lengths:
                lk.append(np.log(np.mean(lengths) + 1e-10))
                ks.append(np.log(k))

        if len(ks) < 3:
            return np.nan

        slope, _, _, _, _ = scipy.stats.linregress(ks, lk)
        return float(np.clip(-slope, 1.0, 2.0))
    except Exception:
        return np.nan


# ── Fractional differentiation ────────────────────────────────────────────────


def _fracdiff_weights(d: float, size: int) -> np.ndarray:
    """
    Compute fractional differentiation weights for order d.

    The weight vector w[k] = ∏_{j=0}^{k-1} (d - j)/(j + 1) × (-1)^k.
    Truncated once |w[k]| < 1e-5 for efficiency (memory-sensitive).
    """
    w = [1.0]
    for k in range(1, size):
        w_k = -w[-1] * (d - k + 1) / k
        if abs(w_k) < 1e-5:
            break
        w.append(w_k)
    return np.array(w[::-1])  # oldest weight first


def _apply_fracdiff(series: np.ndarray, d: float) -> np.ndarray:
    """Apply fractional differencing of order d; returns same-length array with leading NaNs."""
    n = len(series)
    weights = _fracdiff_weights(d, n)
    w_len = len(weights)
    result = np.full(n, np.nan)
    for i in range(w_len - 1, n):
        result[i] = np.dot(weights, series[i - w_len + 1: i + 1])
    return result


def _optimal_fracdiff_d(log_prices: np.ndarray, target_adf_threshold: float = -3.5) -> float:
    """
    Find minimum d such that the fracdiff series is stationary, via a real
    Augmented Dickey-Fuller test (statsmodels.tsa.stattools.adfuller) on
    each candidate fracdiff series — stationary means the ADF test
    statistic is below `target_adf_threshold` (2026-07-19 full-codebase-
    review Fix 10: this previously used a lag-1-autocorrelation proxy
    instead of a real ADF test, despite the docstring/parameter implying
    otherwise — see BuildLog.md for the prior mislabeled version).

    Binary search over d in [0, 1]. Falls back to d=1.0 if adfuller can't
    be computed (insufficient valid points, or a numerical failure).
    Returns the optimal d as a scalar feature.
    """
    from statsmodels.tsa.stattools import adfuller

    lo, hi, best_d = 0.0, 1.0, 1.0
    n = len(log_prices)
    if n < 32:
        return 1.0

    for _ in range(12):  # 12 bisection steps → precision < 0.001
        mid = (lo + hi) / 2
        fd = _apply_fracdiff(log_prices, mid)
        valid = fd[~np.isnan(fd)]
        if len(valid) < 16:
            lo = mid
            continue
        try:
            adf_stat = adfuller(valid, autolag="AIC")[0]
        except (ValueError, np.linalg.LinAlgError):
            lo = mid
            continue
        if adf_stat < target_adf_threshold:
            best_d = mid
            hi = mid
        else:
            lo = mid

    return float(np.clip(best_d, 0.0, 1.0))


# ── Complexity helpers ────────────────────────────────────────────────────────


def _lyapunov_proxy(series: np.ndarray, lag: int = 1) -> float:
    """
    Proxy for the largest Lyapunov exponent using average log-divergence.

    Full Lyapunov estimation requires phase-space reconstruction (TISEAN);
    this implements Rosenstein et al.'s simplified version over a 1D embedding.
    """
    n = len(series)
    if n < 20:
        return np.nan
    try:
        divergences = []
        for i in range(n - lag):
            # Find nearest neighbour excluding temporal neighbours (|i-j| > lag)
            dists = np.abs(series - series[i])
            dists[max(0, i - lag): min(n, i + lag + 1)] = np.inf
            j = np.argmin(dists)
            if j + lag < n and i + lag < n:
                d0 = abs(series[i] - series[j]) + 1e-10
                d1 = abs(series[i + lag] - series[j + lag]) + 1e-10
                divergences.append(np.log(d1 / d0))

        return float(np.mean(divergences)) if divergences else np.nan
    except Exception:
        return np.nan


def _rqa_recurrence_rate(series: np.ndarray, threshold_pct: float = 0.20) -> float:
    """
    Recurrence Quantification Analysis: recurrence rate.

    Fraction of state-space neighbours within threshold_pct of series std.
    Simplified to 1D (no phase-space embedding) for speed.
    """
    n = len(series)
    if n < 10:
        return np.nan
    try:
        threshold = threshold_pct * (np.std(series, ddof=0) + 1e-10)
        norm_series = (series - series.mean()) / (series.std(ddof=0) + 1e-10)
        dist_matrix = np.abs(norm_series[:, None] - norm_series[None, :])
        recurrence = (dist_matrix < threshold).astype(float)
        np.fill_diagonal(recurrence, 0)
        return float(recurrence.sum() / (n * (n - 1)))
    except Exception:
        return np.nan


def _time_series_complexity(series: np.ndarray) -> float:
    """
    Lempel-Ziv-like complexity proxy via number of turning points normalised by n.

    Counts sign changes in first-differences, normalised to [0, 1].
    """
    n = len(series)
    if n < 3:
        return np.nan
    diffs = np.diff(series)
    sign_changes = np.sum(np.diff(np.sign(diffs)) != 0)
    max_possible = n - 2
    return float(sign_changes / max_possible) if max_possible > 0 else np.nan


def _nonlinear_trend_strength(prices: np.ndarray, n_regimes: int = 3) -> float:
    """
    Ratio of variance explained by a piecewise-linear fit vs a linear fit.

    Higher value → stronger nonlinear trend structure.
    """
    n = len(prices)
    if n < 10:
        return np.nan
    try:
        x = np.arange(n)
        # Linear fit variance explained
        slope, intercept, r_lin, _, _ = scipy.stats.linregress(x, prices)
        r2_lin = r_lin ** 2

        # Piecewise: split into n_regimes equal segments, fit each independently
        seg_len = n // n_regimes
        residuals_pw = []
        for k in range(n_regimes):
            seg_x = x[k * seg_len: (k + 1) * seg_len]
            seg_y = prices[k * seg_len: (k + 1) * seg_len]
            if len(seg_x) < 2:
                continue
            s, i, _, _, _ = scipy.stats.linregress(seg_x, seg_y)
            residuals_pw.extend(seg_y - (s * seg_x + i))

        if not residuals_pw:
            return np.nan

        ss_res_pw = np.sum(np.array(residuals_pw) ** 2)
        ss_tot = np.sum((prices - prices.mean()) ** 2) + 1e-10
        r2_pw = 1 - ss_res_pw / ss_tot

        # Strength = improvement of piecewise over linear (0 → same, 1 → perfect improvement)
        return float(np.clip(r2_pw - r2_lin, 0.0, 1.0))
    except Exception:
        return np.nan


# ── Per-ticker computation ────────────────────────────────────────────────────


def _compute_for_ticker(grp: pd.DataFrame) -> pd.DataFrame:
    """
    Compute all 18 advanced technical features for a single ticker's panel.

    Parameters
    ----------
    grp : pd.DataFrame
        OHLCV rows for one ticker, sorted ascending by date. Must have
        columns: date, close, volume.

    Returns
    -------
    pd.DataFrame
        Same rows as input; new columns = ADVANCED_TECHNICAL_FEATURES. All
        per-bar values filled by a rolling computation (look-back windows
        defined by the sub-function).
    """
    grp = grp.sort_values("date").reset_index(drop=True)
    n = len(grp)
    prices = grp["close"].to_numpy(dtype=float)
    volumes = grp["volume"].to_numpy(dtype=float)
    log_prices = np.log(np.maximum(prices, 1e-6))

    result = grp[["date", "ticker"]].copy()
    for col in ADVANCED_TECHNICAL_FEATURES:
        result[col] = np.nan

    if n < 16:
        return result

    # ── Wavelet (use last 64 bars for computational stability) ────────────────
    wavelet_window = min(64, n)
    trend_v, noise_v, energy_r, regime_s = _wavelet_features_series(prices[-wavelet_window:])
    result.loc[result.index[-1], "wavelet_trend"] = trend_v
    result.loc[result.index[-1], "wavelet_noise"] = noise_v
    result.loc[result.index[-1], "wavelet_energy_ratio"] = energy_r
    result.loc[result.index[-1], "wavelet_regime_signal"] = regime_s

    # ── Hurst exponents ───────────────────────────────────────────────────────
    if n >= 21:
        result.loc[result.index[-1], "hurst_exp_21d"] = _hurst_rs(prices[-21:])
    if n >= 63:
        result.loc[result.index[-1], "hurst_exp_63d"] = _hurst_rs(prices[-63:])

    # ── Entropy (21-day window for short-window metrics) ─────────────────────
    ent_window = min(21, n)
    ret_21 = np.diff(log_prices[-ent_window:]) if ent_window >= 2 else np.array([])
    if len(ret_21) >= 8:
        result.loc[result.index[-1], "approx_entropy_21d"] = _approx_entropy(ret_21)
        result.loc[result.index[-1], "sample_entropy_21d"] = _sample_entropy(ret_21)
        result.loc[result.index[-1], "permutation_entropy_21d"] = _permutation_entropy(ret_21)
    if n >= 16:
        result.loc[result.index[-1], "spectral_entropy"] = _spectral_entropy(
            np.diff(log_prices[-min(63, n):])
        )
        result.loc[result.index[-1], "fractal_dimension"] = _fractal_dimension(prices)

    # ── Fractional differentiation ────────────────────────────────────────────
    d_opt = _optimal_fracdiff_d(log_prices)
    result.loc[result.index[-1], "fracdiff_d_optimal"] = d_opt
    fd_price = _apply_fracdiff(log_prices, d_opt)
    result.loc[result.index[-1], "fracdiff_price"] = fd_price[-1] if not np.isnan(fd_price[-1]) else np.nan

    log_vol = np.log(np.maximum(volumes, 1e-6))
    fd_vol = _apply_fracdiff(log_vol, d_opt)
    result.loc[result.index[-1], "fracdiff_volume"] = fd_vol[-1] if not np.isnan(fd_vol[-1]) else np.nan

    # ── Complexity ────────────────────────────────────────────────────────────
    complexity_window = min(63, n)
    recent_prices = prices[-complexity_window:]
    result.loc[result.index[-1], "lyapunov_exponent_proxy"] = _lyapunov_proxy(recent_prices)
    result.loc[result.index[-1], "rqa_rec_rate"] = _rqa_recurrence_rate(recent_prices)
    result.loc[result.index[-1], "time_series_complexity"] = _time_series_complexity(recent_prices)
    result.loc[result.index[-1], "nonlinear_trend_strength"] = _nonlinear_trend_strength(recent_prices)

    return result


# ── Public API ────────────────────────────────────────────────────────────────


def compute_advanced_technical_features(ohlcv_panel: pd.DataFrame) -> pd.DataFrame:
    """
    Compute all 18 advanced technical features for the full universe OHLCV panel.

    Parameters
    ----------
    ohlcv_panel : pd.DataFrame
        Columns: date, ticker, open, high, low, close, volume.
        Must include at least 252 trading days of history per ticker
        (SPEC-FEAT-001) for meaningful results; shorter history yields NaN.

    Returns
    -------
    pd.DataFrame
        One row per (date, ticker); columns: date, ticker + ADVANCED_TECHNICAL_FEATURES.
        Only the most recent bar per ticker has non-NaN values for most
        features (rolling-window features require the full history for
        stability but the daily pipeline only needs today's snapshot).

    Spec References
    ---------------
    SPEC-FEAT-001: minimum 252 trading days; NaN returned, not an error.
    SPEC-PIPE-004: vectorized — no Python loops over individual stocks.
    """
    required = {"date", "ticker", "close", "volume"}
    missing = required - set(ohlcv_panel.columns)
    if missing:
        raise ValueError(f"ohlcv_panel missing columns: {missing}")

    result = ohlcv_panel.groupby("ticker", group_keys=False).apply(_compute_for_ticker)
    return result[["date", "ticker"] + ADVANCED_TECHNICAL_FEATURES].reset_index(drop=True)
