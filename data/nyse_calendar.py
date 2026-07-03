import html
import re
import time
from dataclasses import dataclass
from datetime import datetime, date, time as dtime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import requests


@dataclass(frozen=True)
class NyseDayStatus:
    day: date
    is_open_day: bool
    reason: str
    is_early_close: bool = False
    early_close_note: str = ""


class NyseCalendarService:
    HOLIDAYS_URL = "https://www.nyse.com/markets/hours-calendars"

    def __init__(self, logger: Any, cache_ttl_seconds: float = 21600.0) -> None:
        self.logger = logger
        self._session = requests.Session()
        self._cache_ttl_seconds = cache_ttl_seconds
        self._cached_at = 0.0
        self._holiday_names_by_date: dict[date, str] = {}
        self._early_close_notes_by_date: dict[date, str] = {}

    def get_market_overview(self, days_ahead: int = 7) -> dict[str, Any]:
        holiday_names, early_close_notes = self._load_calendar_metadata()
        now_et = datetime.now(ZoneInfo("America/New_York"))
        today = now_et.date()

        today_status = self._status_for_day(today, holiday_names, early_close_notes)
        is_open_now = self._is_open_now(now_et, today_status.is_open_day)
        next_open_day = self._find_next_open_day_from_now(now_et, today_status, holiday_names, early_close_notes)
        next_early_close_day = self._find_next_early_close_day(today, holiday_names, early_close_notes)
        tomorrow = today + timedelta(days=1)
        tomorrow_status = self._status_for_day(tomorrow, holiday_names, early_close_notes)

        upcoming: list[NyseDayStatus] = []
        for offset in range(days_ahead):
            day = today + timedelta(days=offset)
            upcoming.append(self._status_for_day(day, holiday_names, early_close_notes))

        return {
            "now_et": now_et,
            "is_open_now": is_open_now,
            "today": today_status,
            "tomorrow": tomorrow_status,
            "next_open_day": next_open_day,
            "next_early_close_day": next_early_close_day,
            "upcoming": upcoming,
            "source": self.HOLIDAYS_URL,
        }

    def _load_calendar_metadata(self) -> tuple[dict[date, str], dict[date, str]]:
        if (time.time() - self._cached_at) <= self._cache_ttl_seconds and self._holiday_names_by_date:
            return self._holiday_names_by_date, self._early_close_notes_by_date

        response = self._session.get(self.HOLIDAYS_URL, timeout=20)
        response.raise_for_status()
        page = response.text

        table_match = re.search(r"<table[^>]*>(.*?)</table>", page, flags=re.IGNORECASE | re.DOTALL)
        if not table_match:
            raise ValueError("No se pudo encontrar la tabla de feriados de NYSE")

        table_html = table_match.group(1)

        year_headers = self._extract_year_headers(table_html)
        if not year_headers:
            raise ValueError("No se pudieron extraer los anos del calendario NYSE")

        holiday_names: dict[date, str] = {}

        row_matches = re.findall(r"<tr>(.*?)</tr>", table_html, flags=re.IGNORECASE | re.DOTALL)
        for row_html in row_matches:
            holiday_name_match = re.search(r"<th>(.*?)</th>", row_html, flags=re.IGNORECASE | re.DOTALL)
            if not holiday_name_match:
                continue

            holiday_name = self._clean_html_text(holiday_name_match.group(1))
            cell_values = re.findall(r"<td>(.*?)</td>", row_html, flags=re.IGNORECASE | re.DOTALL)
            if not cell_values:
                continue

            for index, raw_cell in enumerate(cell_values):
                if index >= len(year_headers):
                    continue
                year = year_headers[index]
                parsed_day = self._parse_holiday_day(raw_cell, year)
                if parsed_day is not None:
                    holiday_names[parsed_day] = holiday_name

        early_close_notes = self._extract_early_close_dates(page)

        if holiday_names:
            self._holiday_names_by_date = holiday_names
            self._early_close_notes_by_date = early_close_notes
            self._cached_at = time.time()
            return self._holiday_names_by_date, self._early_close_notes_by_date

        raise ValueError("No se pudieron parsear feriados de NYSE")

    @classmethod
    def _extract_early_close_dates(cls, page_html: str) -> dict[date, str]:
        cleaned = cls._clean_html_text(page_html)
        notes: dict[date, str] = {}
        lower_cleaned = cleaned.lower()
        marker = "close early at 1:00 p.m"
        index = 0
        while True:
            found = lower_cleaned.find(marker, index)
            if found == -1:
                break

            window = cleaned[found: found + 700]
            matches = re.findall(
                r"(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),\s+[A-Za-z]+\s+\d{1,2},\s+\d{4}",
                window,
            )
            if not matches:
                index = found + len(marker)
                continue

            for raw_date in matches:
                try:
                    parsed = datetime.strptime(raw_date, "%A, %B %d, %Y").date()
                except ValueError:
                    continue
                notes[parsed] = "Cierre temprano 1:00 p.m. ET"
            index = found + len(marker)
        return notes

    @staticmethod
    def _extract_year_headers(table_html: str) -> list[int]:
        headers = re.findall(r"<th>(\d{4})</th>", table_html, flags=re.IGNORECASE)
        return [int(value) for value in headers]

    @classmethod
    def _parse_holiday_day(cls, raw_cell: str, year: int) -> date | None:
        text = cls._clean_html_text(raw_cell)
        text = text.replace("\u2014", "-").strip()
        if not text or text.startswith("-"):
            return None

        text = re.sub(r"\*+", "", text)
        text = re.sub(r"\(.*?\)", "", text).strip()

        try:
            dt = datetime.strptime(f"{text} {year}", "%A, %B %d %Y")
            return dt.date()
        except ValueError:
            return None

    @staticmethod
    def _clean_html_text(value: str) -> str:
        text = re.sub(r"<[^>]+>", "", value)
        text = html.unescape(text)
        return re.sub(r"\s+", " ", text).strip()

    @staticmethod
    def _is_open_now(now_et: datetime, is_open_day: bool) -> bool:
        if not is_open_day:
            return False

        current = now_et.time()
        return dtime(hour=9, minute=30) <= current < dtime(hour=16, minute=0)

    def _status_for_day(
        self,
        day: date,
        holiday_names: dict[date, str],
        early_close_notes: dict[date, str],
    ) -> NyseDayStatus:
        if day in holiday_names:
            return NyseDayStatus(day=day, is_open_day=False, reason=f"Feriado NYSE: {holiday_names[day]}")
        if day.weekday() >= 5:
            return NyseDayStatus(day=day, is_open_day=False, reason="Fin de semana")
        if day in early_close_notes:
            return NyseDayStatus(
                day=day,
                is_open_day=True,
                reason="Dia habil (cierre temprano)",
                is_early_close=True,
                early_close_note=early_close_notes[day],
            )
        return NyseDayStatus(day=day, is_open_day=True, reason="Dia habil")

    def _find_next_open_day(
        self,
        start_day: date,
        holiday_names: dict[date, str],
        early_close_notes: dict[date, str],
    ) -> date:
        candidate = start_day
        for _ in range(14):
            status = self._status_for_day(candidate, holiday_names, early_close_notes)
            if status.is_open_day:
                return candidate
            candidate += timedelta(days=1)
        return candidate

    def _find_next_open_day_from_now(
        self,
        now_et: datetime,
        today_status: NyseDayStatus,
        holiday_names: dict[date, str],
        early_close_notes: dict[date, str],
    ) -> date:
        current_time = now_et.time()
        open_time = dtime(hour=9, minute=30)
        close_time = dtime(hour=16, minute=0)

        if today_status.is_open_day:
            if current_time < open_time:
                return now_et.date()
            if open_time <= current_time < close_time:
                return now_et.date()

        return self._find_next_open_day(now_et.date() + timedelta(days=1), holiday_names, early_close_notes)

    def _find_next_early_close_day(
        self,
        start_day: date,
        holiday_names: dict[date, str],
        early_close_notes: dict[date, str],
    ) -> date | None:
        candidate = start_day
        for _ in range(365):
            status = self._status_for_day(candidate, holiday_names, early_close_notes)
            if status.is_open_day and status.is_early_close:
                return candidate
            candidate += timedelta(days=1)
        return None
