"""
Indicator pairlist filter
"""
import logging
from copy import deepcopy
from typing import Any, Dict, List, Optional

import arrow
from cachetools import TTLCache
from pandas import DataFrame

from freqtrade.constants import Config, ListPairsWithTimeframes
from freqtrade.exceptions import OperationalException
from freqtrade.plugins.pairlist.IPairList import IPairList
from freqtrade.resolvers import StrategyResolver


logger = logging.getLogger(__name__)


class IndicatorFilter(IPairList):

    def __init__(self, exchange, pairlistmanager,
                 config: Config, pairlistconfig: Dict[str, Any],
                 pairlist_pos: int) -> None:
        super().__init__(exchange, pairlistmanager, config, pairlistconfig, pairlist_pos)

        self._indicator_timeframe = self._pairlistconfig.get('indicator_timeframe', '1d')
        self._refresh_period = pairlistconfig.get('refresh_period', 1440)
        self._indicator_warmup_period = self._pairlistconfig.get('indicator_warmup_period', 200)

        self._def_candletype = self._config['candle_type_def']
        self._strat = StrategyResolver._load_strategy(
            config=self._config, strategy_name=self._config['strategy'])

        self._pair_cache: TTLCache = TTLCache(maxsize=1000, ttl=self._refresh_period)

        candle_limit = exchange.ohlcv_candle_limit(
            self._indicator_timeframe, self._config['candle_type_def'])

        if self._indicator_warmup_period < 1:
            raise OperationalException("IndicatorFilter requires indicator_warmup_period >= 1")
        if self._indicator_warmup_period > candle_limit:
            raise OperationalException("IndicatorFilter requires indicator_warmup_period to not "
                                       f"exceed exchange max request size ({candle_limit})")

    @property
    def needstickers(self) -> bool:
        """
        Boolean property defining if tickers are necessary.
        If no Pairlist requires tickers, an empty List is passed
        as tickers argument to filter_pairlist
        """
        return False

    def short_desc(self) -> str:
        """
        Short whitelist method description - used for startup-messages
        """
        return (f"{self.name} - Filtering pairs with condition.")

    def filter_pairlist(self, pairlist: List[str], tickers: Dict) -> List[str]:
        """
        Validate Conditions
        :param pairlist: pairlist to filter or sort
        :param tickers: Tickers (from exchange.get_tickers). May be cached.
        :return: new allowlist
        """
        needed_pairs: ListPairsWithTimeframes = [
            (p, self._indicator_timeframe, self._def_candletype) for p in pairlist
            if p not in self._pair_cache]

        since_ms = (arrow.utcnow()
                    .floor('day')
                    .shift(days=-self._indicator_warmup_period - 1)
                    .int_timestamp) * 1000
        # Get all candles
        candles = {}
        if needed_pairs:
            candles = self._exchange.refresh_latest_ohlcv(needed_pairs, since_ms=since_ms,
                                                          cache=False)

        if self._enabled:
            for p in deepcopy(pairlist):
                check_candles = candles[(p, self._indicator_timeframe, self._def_candletype)] if (
                    p, self._indicator_timeframe, self._def_candletype) in candles else None
                if not self._validate_pair_loc(p, check_candles):
                    pairlist.remove(p)
        return pairlist

    def _validate_pair_loc(self, pair: str, check_candles: Optional[DataFrame]) -> bool:
        """
        Validate Indicator Conditions
        :param pair: Pair that's currently validated
        :param check_candles: Downloaded candles
        :return: True if the pair can stay, false if it should be removed
        """
        # Check symbol in cache
        cached_res = self._pair_cache.get(pair, None)
        if cached_res is not None:
            return cached_res

        result = True

        if check_candles is not None and not check_candles.empty:
            check_candles = self._strat.populate_filter_indicators(
                check_candles, self._indicator_timeframe)
            last_candle = check_candles.iloc[-1].squeeze()

            if not last_candle['filter']:
                self.log_once(
                    f"Removed {pair} from whitelist"
                    f" due to not passing condition ({self._indicator_timeframe})", logger.info)
                result = False
            self._pair_cache[pair] = result
        else:
            self.log_once(f"Removed {pair} from whitelist, no candles found.", logger.info)
        return result
