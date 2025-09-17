
from __future__ import annotations

import asyncio
import argparse
import json
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import aiohttp

API_URL = "https://api.privatbank.ua/p24api/exchange_rates?json&date={date}"  
SUPPORTED_CURRENCIES = {"USD", "EUR"}
MAX_DAYS = 10



@dataclass(frozen=True)
class Rate:
    currency: str
    sale: float
    purchase: float


@dataclass(frozen=True)
class DayRates:
    date: str  
    rates: Dict[str, Rate]  




class RateFetcher(ABC):
    """Абстракція джерела курсів (можна підміняти в тестах/інших реалізаціях)."""
    @abstractmethod
    async def fetch_day(self, date_str: str) -> DayRates:
        ...



class PrivatBankRateFetcher(RateFetcher):
    """Реалізація роботи з публічним API ПриватБанку (історичні готівкові курси)."""

    def __init__(
        self,
        session: aiohttp.ClientSession,
        timeout_s: float = 10.0,
        max_retries: int = 3,
        backoff_base: float = 0.6,
    ):
        self._session = session
        self._timeout = aiohttp.ClientTimeout(total=timeout_s)
        self._max_retries = max_retries
        self._backoff_base = backoff_base

    async def fetch_day(self, date_str: str) -> DayRates:
        url = API_URL.format(date=date_str)

        last_error: Optional[Exception] = None
        for attempt in range(1, self._max_retries + 1):
            try:
                async with self._session.get(url, timeout=self._timeout) as resp:
                    if resp.status != 200:
                        raise RuntimeError(f"HTTP {resp.status}")
                    data = await resp.json(content_type=None)
                    rates = self._parse_payload(date_str, data)
                    return DayRates(date=date_str, rates=rates)
            except (aiohttp.ClientError, asyncio.TimeoutError, json.JSONDecodeError, RuntimeError) as e:
                last_error = e
                if attempt == self._max_retries:
                    break
                await asyncio.sleep(self._backoff_base * (2 ** (attempt - 1)))

        return DayRates(date=date_str, rates={})

    @staticmethod
    def _parse_payload(date_str: str, payload: Dict[str, Any]) -> Dict[str, Rate]:
        """
        В payload очікуємо ключ 'exchangeRate' — список об'єктів із полями:
        - currency, saleRate, purchaseRate (готівкові Привату)
        - saleRateNB, purchaseRateNB (НБУ) – на випадок, якщо готівкові відсутні
        """
        result: Dict[str, Rate] = {}
        items = payload.get("exchangeRate", [])

        for item in items:
            cur = item.get("currency")
            if cur not in SUPPORTED_CURRENCIES:
                continue

            sale = item.get("saleRate")
            purchase = item.get("purchaseRate")

            if sale is None or purchase is None:
                sale = item.get("saleRateNB")
                purchase = item.get("purchaseRateNB")

            if sale is None or purchase is None:
                continue

            try:
                sale_f = float(sale)
                purchase_f = float(purchase)
            except (TypeError, ValueError):
                continue

            result[cur] = Rate(currency=cur, sale=sale_f, purchase=purchase_f)

        return result


class RateService:
    """Оркестратор: збирає дати, паралельно тягне, сортує та форматує."""

    def __init__(self, fetcher: RateFetcher, concurrency: int = 5):
        self._fetcher = fetcher
        self._sem = asyncio.Semaphore(concurrency)

    async def get_last_days(self, days: int) -> List[DayRates]:
        if days < 1 or days > MAX_DAYS:
            raise ValueError(f"Кількість днів має бути в діапазоні 1..{MAX_DAYS}")

        dates = [self._format_date(datetime.now() - timedelta(days=offset))
                 for offset in range(0, days)]
        dates_sorted = sorted(dates, key=lambda d: datetime.strptime(d, "%d.%m.%Y"), reverse=True)

        async def _task(date_str: str) -> DayRates:
            async with self._sem:
                return await self._fetcher.fetch_day(date_str)

        results = await asyncio.gather(*[_task(d) for d in dates_sorted], return_exceptions=False)
        return list(results)

    @staticmethod
    def _format_date(dt: datetime) -> str:
        return dt.strftime("%d.%m.%Y")


class Presenter:
    """Відповідає за представлення результату назовні (SRP)."""

    @staticmethod
    def to_example_structure(days: List[DayRates]) -> List[Dict[str, Dict[str, Dict[str, float]]]]:
        """
        Повертає список словників:
        [
          {
            'DD.MM.YYYY': {
              'EUR': {'sale': 0.0, 'purchase': 0.0},
              'USD': {'sale': 0.0, 'purchase': 0.0},
            }
          },
          ...
        ]
        Дати у списку вже в бажаному порядку (контролює RateService).
        """
        out: List[Dict[str, Dict[str, Dict[str, float]]]] = []
        for day in days:
            day_obj: Dict[str, Dict[str, Dict[str, float]]] = {}
            inner: Dict[str, Dict[str, float]] = {}
            for cur in ("EUR", "USD"):
                rate = day.rates.get(cur)
                if rate:
                    inner[cur] = {"sale": rate.sale, "purchase": rate.purchase}
            day_obj[day.date] = inner
            out.append(day_obj)
        return out

    @staticmethod
    def print_human_readable(data: List[Dict[str, Dict[str, Dict[str, float]]]]) -> None:
        print(json.dumps(data, ensure_ascii=False, indent=2))



def parse_args() -> int:
    parser = argparse.ArgumentParser(
        description=f"Повертає готівкові курси EUR та USD ПриватБанку за останні N днів (1..{MAX_DAYS})."
    )
    parser.add_argument(
        "days",
        type=int,
        help=f"Кількість днів (макс {MAX_DAYS}). 1 = сьогодні, 2 = сьогодні+вчора, ...",
    )
    args = parser.parse_args()
    if not (1 <= args.days <= MAX_DAYS):
        raise SystemExit(f"Помилка: дозволено 1..{MAX_DAYS} днів.")
    return args.days


async def main_async(days: int) -> None:
    connector = aiohttp.TCPConnector(limit=20, ssl=False)  
    async with aiohttp.ClientSession(connector=connector) as session:
        fetcher = PrivatBankRateFetcher(session=session)
        service = RateService(fetcher=fetcher, concurrency=5)

        day_rates = await service.get_last_days(days)
        data = Presenter.to_example_structure(day_rates)
        Presenter.print_human_readable(data)


def main() -> None:
    days = parse_args()
    try:
        asyncio.run(main_async(days))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
