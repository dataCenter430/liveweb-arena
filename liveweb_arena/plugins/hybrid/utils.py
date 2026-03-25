"""Shared utilities for hybrid plugin templates.

Design principle: NO FALLBACK BEHAVIOR
- Cache mode: Page and API data are from the same snapshot, use API data (deterministic)
- Live mode: Page visit triggers API fetch simultaneously, use API data (deterministic)
- If data is not found, fail immediately with clear error - don't silently fallback
"""

import asyncio
import time
from typing import Any, Callable, Optional, TypeVar

from liveweb_arena.plugins.coingecko.api_client import CoinGeckoClient
from liveweb_arena.plugins.stooq.api_client import StooqClient, StooqRateLimitError
from liveweb_arena.utils.logger import log, progress, progress_done, is_verbose

T = TypeVar('T')


def _filter_stooq_keys(keys: list) -> list:
    """Filter keys to show only Stooq symbols (contain '.' like 'aapl.us')."""
    return [k for k in keys if isinstance(k, str) and '.' in k]


def _filter_coingecko_keys(keys: list) -> list:
    """Filter keys to show only CoinGecko coin IDs (no '.' and not weather locations)."""
    return [k for k in keys if isinstance(k, str) and '.' not in k and k != 'taostats']


async def retry_with_backoff(
    func: Callable[[], T],
    max_retries: int = 10,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    operation_name: str = "operation",
) -> T:
    """Retry an async operation with exponential backoff."""
    last_exception = None
    start_time = time.time()

    for attempt in range(max_retries):
        try:
            if is_verbose():
                elapsed = time.time() - start_time
                progress("GT", elapsed, 120, f"[{attempt+1}/{max_retries}] {operation_name}")
            result = await func()
            if result is not None:
                if is_verbose():
                    progress_done("GT", f"{operation_name} done in {time.time()-start_time:.1f}s")
                return result
            raise ValueError(f"{operation_name} returned None")
        except StooqRateLimitError:
            raise
        except Exception as e:
            last_exception = e
            if attempt < max_retries - 1:
                delay = min(base_delay * (2 ** attempt), max_delay)
                log("GT", f"{operation_name} failed ({attempt + 1}/{max_retries}): {e}", force=True)
                await asyncio.sleep(delay)
            else:
                log("GT", f"{operation_name} failed after {max_retries} attempts: {e}", force=True)

    raise RuntimeError(f"{operation_name} failed after {max_retries} retries: {last_exception}")


async def get_crypto_24h_change(coin_id: str) -> float:
    """
    Get 24h percentage change from CoinGecko.

    Data source (deterministic, no fallback):
    - Cache mode: Uses collected API data (same snapshot as page display)
    - Live mode: Uses collected API data or fetches from API

    Args:
        coin_id: CoinGecko coin identifier

    Returns:
        24h percentage change

    Raises:
        RuntimeError: If data not found - evaluation should stop
    """
    from liveweb_arena.core.gt_collector import get_current_gt_collector

    # Try collected API data (works for both cache and live mode)
    gt_collector = get_current_gt_collector()
    if gt_collector is not None:
        api_data = gt_collector.get_collected_api_data()
        if coin_id in api_data:
            coin_data = api_data[coin_id]
            change = coin_data.get("price_change_percentage_24h")
            if change is not None:
                log("GT", f"Collected: {coin_id} 24h={change:+.2f}%")
                return change
            # Page was visited but data extraction failed - system error
            raise ValueError(
                f"GT extraction failed: CoinGecko page for '{coin_id}' was visited but "
                f"'price_change_percentage_24h' is missing. Data keys: {list(coin_data.keys())}"
            )

        # Data not found in collected pool - always fail when gt_collector exists
        coingecko_keys = _filter_coingecko_keys(list(api_data.keys()))
        raise RuntimeError(
            f"Agent did not visit CoinGecko page for '{coin_id}'. "
            f"Required URL: https://www.coingecko.com/en/coins/{coin_id} | "
            f"Visited CoinGecko: {coingecko_keys[:5] if coingecko_keys else '(none)'}"
        )

    # Live mode: no gt_collector (standalone usage only), fetch directly from API
    log("GT", f"Live fetch: {coin_id}")

    async def fetch():
        data = await CoinGeckoClient.get_coin_market_data(coin_id)
        if data and len(data) > 0:
            change = data[0].get("price_change_percentage_24h")
            if change is not None:
                return change
        return None

    return await retry_with_backoff(
        fetch,
        max_retries=10,
        base_delay=1.0,
        operation_name=f"CoinGecko fetch {coin_id}",
    )


async def get_stooq_price(symbol: str) -> float:
    """
    Get current price from Stooq.

    Data source (deterministic, no fallback):
    - Cache mode: Uses collected API data
    - Live mode: Uses collected API data or fetches from API

    Args:
        symbol: Stooq symbol

    Returns:
        Current price

    Raises:
        RuntimeError: If data not found - evaluation should stop
    """
    from liveweb_arena.core.gt_collector import get_current_gt_collector

    gt_collector = get_current_gt_collector()
    if gt_collector is not None:
        api_data = gt_collector.get_collected_api_data()
        # Try original, lowercase, and uppercase (API returns uppercase symbols)
        asset_data = None
        for sym in [symbol, symbol.lower(), symbol.upper()]:
            if sym in api_data:
                asset_data = api_data[sym]
                break
        if asset_data is not None:
            price = asset_data.get("close")
            if price is not None:
                log("GT", f"Collected: {symbol} price={price}")
                return price
            # Page was visited but data extraction failed - system error
            raise ValueError(
                f"GT extraction failed: Stooq page for '{symbol}' was visited but 'close' price is missing. "
                f"Data keys: {list(asset_data.keys())}"
            )

        stooq_keys = _filter_stooq_keys(list(api_data.keys()))
        raise RuntimeError(
            f"Agent did not visit Stooq page for '{symbol}'. "
            f"Required URL: https://stooq.com/q/?s={symbol} | "
            f"Visited Stooq: {stooq_keys[:5] if stooq_keys else '(none)'}"
        )

    # Live mode: no gt_collector (standalone usage only), fetch directly from API
    log("GT", f"Live fetch: {symbol}")

    async def fetch():
        data = await StooqClient.get_price_data(symbol)
        if data:
            price = data.get("close")
            if price is not None:
                return price
        return None

    return await retry_with_backoff(
        fetch,
        max_retries=10,
        base_delay=1.0,
        operation_name=f"Stooq price {symbol}",
    )


async def get_stooq_24h_change(symbol: str) -> float:
    """
    Get daily percentage change from Stooq.

    Data source (deterministic, no fallback):
    - Cache mode: Uses collected API data
    - Live mode: Uses collected API data or fetches from API

    Args:
        symbol: Stooq symbol

    Returns:
        Daily percentage change

    Raises:
        RuntimeError: If data not found - evaluation should stop
    """
    from liveweb_arena.core.gt_collector import get_current_gt_collector

    gt_collector = get_current_gt_collector()
    if gt_collector is not None:
        api_data = gt_collector.get_collected_api_data()
        # Try both original and lowercase
        asset_data = None
        matched_sym = None
        for sym in [symbol, symbol.lower()]:
            if sym in api_data:
                asset_data = api_data[sym]
                matched_sym = sym
                break

        if asset_data is not None:
            change = asset_data.get("daily_change_pct")
            if change is not None:
                log("GT", f"Collected: {symbol} 24h={change:+.2f}%")
                return change
            # Page was visited but data extraction failed - system error
            raise ValueError(
                f"GT extraction failed: Stooq page for '{matched_sym}' was visited but "
                f"'daily_change_pct' is missing. Data keys: {list(asset_data.keys())}"
            )

        stooq_keys = _filter_stooq_keys(list(api_data.keys()))
        raise RuntimeError(
            f"Agent did not visit Stooq page for '{symbol}'. "
            f"Required URL: https://stooq.com/q/?s={symbol} | "
            f"Visited Stooq: {stooq_keys[:5] if stooq_keys else '(none)'}"
        )

    # Live mode: no gt_collector (standalone usage only), fetch directly from API
    log("GT", f"Live fetch: {symbol}")

    async def fetch():
        data = await StooqClient.get_price_data(symbol)
        if data:
            change = data.get("daily_change_pct")
            if change is not None:
                return change
        return None

    return await retry_with_backoff(
        fetch,
        max_retries=10,
        base_delay=1.0,
        operation_name=f"Stooq change {symbol}",
    )
