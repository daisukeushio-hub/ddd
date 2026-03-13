#!/usr/bin/env python3
"""Sansan company scraper with page-level resume and dedupe support."""

from __future__ import annotations

import argparse
import csv
import getpass
import hashlib
import json
import logging
import os
import re
import sqlite3
import sys
import time
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


JST = timezone.utc

SALES_RANGES: List[Tuple[str, str]] = [
    ("10億", "30億"),
    ("30億", "50億"),
    ("50億", "100億"),
]

COLUMNS = [
    "企業名",
    "URL",
    "最新期業績売上高",
    "郵便番号",
    "住所",
    "電話番号",
    "代表者肩書",
    "代表者名",
    "主業",
    "産業分類（大分類）",
    "産業分類（中分類）",
    "産業分類（小分類）",
    "従業員数",
    "資本金",
    "最新決算期",
    "従業",
    "従業（大分類）",
    "従業（中分類）",
    "従業（小分類）",
    "取得日時",
    "検索条件ID",
    "取得ページ番号",
    "重複判定キー",
]


@dataclass
class Cursor:
    sales_index: int = 0
    industry_index: int = 0
    page: int = 1


class StateStore:
    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.backup = self.path.with_suffix(self.path.suffix + ".bak")

    def load(self) -> Optional[Dict[str, Any]]:
        for candidate in [self.path, self.backup]:
            if not candidate.exists():
                continue
            try:
                return json.loads(candidate.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                continue
        return None

    def save(self, state: Dict[str, Any]) -> None:
        tmp_path = self.path.with_suffix(self.path.suffix + ".tmp")
        state["updated_at"] = now_iso()
        with tmp_path.open("w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        if self.path.exists():
            self.backup.write_text(self.path.read_text(encoding="utf-8"), encoding="utf-8")
        os.replace(tmp_path, self.path)


class DedupeStore:
    def __init__(self, db_path: Path):
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(db_path))
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS seen_companies (
                dedupe_key TEXT PRIMARY KEY,
                first_seen_at TEXT NOT NULL,
                company_name TEXT,
                address TEXT,
                source_condition_id TEXT,
                source_page INTEGER
            )
            """
        )
        self.conn.commit()

    def seen(self, key: str) -> bool:
        row = self.conn.execute("SELECT 1 FROM seen_companies WHERE dedupe_key = ?", (key,)).fetchone()
        return row is not None

    def insert(self, key: str, company_name: str, address: str, condition_id: str, page: int) -> None:
        self.conn.execute(
            """
            INSERT OR IGNORE INTO seen_companies
            (dedupe_key, first_seen_at, company_name, address, source_condition_id, source_page)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (key, now_iso(), company_name, address, condition_id, page),
        )
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()


class CsvSink:
    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def append_rows(self, rows: List[Dict[str, Any]]) -> None:
        if not rows:
            return
        write_header = not self.path.exists()
        with self.path.open("a", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=COLUMNS)
            if write_header:
                writer.writeheader()
            writer.writerows(rows)


def now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def normalize_for_key(text: str) -> str:
    text = unicodedata.normalize("NFKC", (text or "").strip())
    text = text.replace("\n", " ").replace("\t", " ")
    text = re.sub(r"\s+", " ", text)
    return text


def make_dedupe_key(company_name: str, address: str) -> str:
    base = f"{normalize_for_key(company_name)}||{normalize_for_key(address)}"
    return hashlib.sha256(base.encode("utf-8")).hexdigest()


def load_industries(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        expected = {"大分類", "中分類", "小分類"}
        if not reader.fieldnames or not expected.issubset(set(reader.fieldnames)):
            raise ValueError("industries.csv のヘッダーは 大分類,中分類,小分類 が必要です")
        return [
            {
                "大分類": (row.get("大分類") or "").strip(),
                "中分類": (row.get("中分類") or "").strip(),
                "小分類": (row.get("小分類") or "").strip(),
            }
            for row in reader
        ]


def retry_call(func, retries: int, logger: logging.Logger, label: str):
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            return func()
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if attempt == retries:
                break
            sleep_sec = min(20, 1.5 * (2 ** (attempt - 1)))
            logger.warning("%s failed (attempt %s/%s): %s", label, attempt, retries, exc)
            time.sleep(sleep_sec)
    raise RuntimeError(f"{label} failed after {retries} retries: {last_error}")


def setup_logger(log_path: Path, verbose: bool) -> logging.Logger:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("sansan_scraper")
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    sh.setLevel(logging.DEBUG if verbose else logging.INFO)
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(fmt)
    fh.setLevel(logging.DEBUG)

    logger.addHandler(sh)
    logger.addHandler(fh)
    return logger


def build_condition_id(sales_from: str, sales_to: str, industry: Dict[str, str]) -> str:
    return f"{sales_from}-{sales_to}|{industry['大分類']}>{industry['中分類']}>{industry['小分類']}"


def select_custom_dropdown(driver, wait, By, EC, category_text: str, option_text: str) -> None:
    dropdown_xpath = f"//span[contains(@class, 'select2-selection')][.//span[text()='{category_text}']]"
    dropdown_display = wait.until(EC.element_to_be_clickable((By.XPATH, dropdown_xpath)))
    dropdown_display.click()

    search_box = wait.until(EC.presence_of_element_located((By.CLASS_NAME, "select2-search__field")))
    search_box.clear()
    search_box.send_keys(option_text)

    result_option_xpath = f"//li[contains(@class, 'select2-results__option') and contains(., '{option_text}')]"
    result_option = wait.until(EC.element_to_be_clickable((By.XPATH, result_option_xpath)))
    result_option.click()


def parse_page_rows(driver, By, logger: logging.Logger) -> List[Dict[str, str]]:
    rows = driver.find_elements(By.CLASS_NAME, "search-result-list-table-data-row")
    data: List[Dict[str, str]] = []

    def safe_text(row, class_name: str) -> str:
        try:
            return row.find_element(By.CLASS_NAME, class_name).text
        except Exception:
            return ""

    for row in rows:
        try:
            url = ""
            try:
                url = row.find_element(By.CLASS_NAME, "url").find_element(By.TAG_NAME, "a").get_attribute("href")
            except Exception:
                pass
            data.append(
                {
                    "企業名": safe_text(row, "company-name-label"),
                    "URL": url,
                    "最新期業績売上高": safe_text(row, "latest-sales-accounting-term-sales"),
                    "郵便番号": safe_text(row, "postal-code"),
                    "住所": safe_text(row, "location"),
                    "電話番号": safe_text(row, "phone-number"),
                    "代表者肩書": safe_text(row, "representative-title"),
                    "代表者名": safe_text(row, "representative-name"),
                    "主業": safe_text(row, "tdb-main-industrial-classification-section"),
                    "産業分類（大分類）": safe_text(row, "tdb-main-industrial-classification-division"),
                    "産業分類（中分類）": safe_text(row, "tdb-main-industrial-classification-group"),
                    "産業分類（小分類）": safe_text(row, "tdb-main-industrial-classification-class"),
                    "従業員数": safe_text(row, "employee-number"),
                    "資本金": safe_text(row, "legal-capital"),
                    "最新決算期": safe_text(row, "latest-sales-accounting-term"),
                    "従業": safe_text(row, "tdb-sub-industrial-classification-section"),
                    "従業（大分類）": safe_text(row, "tdb-sub-industrial-classification-division"),
                    "従業（中分類）": safe_text(row, "tdb-sub-industrial-classification-group"),
                    "従業（小分類）": safe_text(row, "tdb-sub-industrial-classification-class"),
                }
            )
        except Exception as e:  # noqa: BLE001
            logger.warning("row parse skipped: %s", e)
    return data


def run(args: argparse.Namespace) -> int:
    from selenium import webdriver
    from selenium.common.exceptions import TimeoutException
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.support.ui import Select, WebDriverWait

    today = datetime.now().strftime("%Y%m%d")
    output_path = Path(args.output_csv or f"output/sansan_企業リスト_{today}.csv")
    log_path = Path(args.log_file or f"logs/run_{today}.log")
    state_path = Path(args.state_file)
    db_path = Path(args.sqlite_file)

    logger = setup_logger(log_path, args.verbose)
    industries = load_industries(Path(args.industries_csv))
    logger.info("industries loaded: %s", len(industries))

    state_store = StateStore(state_path)
    dedupe_store = DedupeStore(db_path)
    sink = CsvSink(output_path)

    state = {
        "version": 1,
        "status": "running",
        "cursor": {"sales_index": 0, "industry_index": 0, "page": 1},
        "stats": {"rows_seen": 0, "rows_written": 0, "rows_duplicated": 0, "conditions_done": 0, "errors": 0},
        "updated_at": now_iso(),
        "last_error": None,
    }

    if args.resume:
        loaded = state_store.load()
        if loaded:
            state = loaded
            logger.info("resume enabled; restored state from %s", state_path)

    start_cursor = Cursor(
        sales_index=state["cursor"].get("sales_index", 0),
        industry_index=state["cursor"].get("industry_index", 0),
        page=state["cursor"].get("page", 1),
    )

    options = webdriver.ChromeOptions()
    options.add_experimental_option("excludeSwitches", ["enable-logging"])
    if args.headless:
        options.add_argument("--headless=new")

    driver = webdriver.Chrome(options=options)
    wait = WebDriverWait(driver, args.timeout_sec)

    try:
        driver.get("https://ap.sansan.com/v/SSLogin.aspx")
        email = input("メールアドレスを入力してください: ")
        password = getpass.getpass("パスワードを入力してください: ")
        wait.until(EC.presence_of_element_located((By.ID, "txtLoginEmail"))).send_keys(email)
        driver.find_element(By.ID, "txtPassword").send_keys(password)
        driver.find_element(By.ID, "btnLogin").click()
        logger.info("login submitted")

        condition_count = 0
        for si, (sales_from, sales_to) in enumerate(SALES_RANGES):
            if si < start_cursor.sales_index:
                continue
            for ii, industry in enumerate(industries):
                if si == start_cursor.sales_index and ii < start_cursor.industry_index:
                    continue
                if args.max_conditions and condition_count >= args.max_conditions:
                    logger.info("max_conditions reached: %s", args.max_conditions)
                    state["status"] = "stopped_by_limit"
                    state_store.save(state)
                    return 0

                condition_count += 1
                page = start_cursor.page if (si == start_cursor.sales_index and ii == start_cursor.industry_index) else 1
                condition_id = build_condition_id(sales_from, sales_to, industry)
                logger.info("condition start: %s page=%s", condition_id, page)

                def open_and_search():
                    driver.get("https://ap.sansan.com/v/companies/")
                    Select(driver.find_element(By.ID, "SearchInput_LatestSalesAccountingTermSalesFrom")).select_by_visible_text(sales_from)
                    Select(driver.find_element(By.ID, "SearchInput_LatestSalesAccountingTermSalesTo")).select_by_visible_text(sales_to)

                    select_custom_dropdown(driver, wait, By, EC, "大分類", industry["大分類"])
                    select_custom_dropdown(driver, wait, By, EC, "中分類", industry["中分類"])
                    select_custom_dropdown(driver, wait, By, EC, "小分類", industry["小分類"])

                    old_html = driver.find_element(By.TAG_NAME, "html")
                    driver.find_element(By.ID, "button-detail-search").click()
                    wait.until(EC.staleness_of(old_html))

                retry_call(open_and_search, args.retries, logger, "search setup")

                for p in range(1, page):
                    try:
                        old_html = driver.find_element(By.TAG_NAME, "html")
                        driver.find_element(By.CLASS_NAME, "btn-next-page").click()
                        wait.until(EC.staleness_of(old_html))
                    except Exception as e:  # noqa: BLE001
                        logger.error("resume fast-forward failed at page=%s: %s", p, e)
                        break

                while True:
                    state["cursor"] = {"sales_index": si, "industry_index": ii, "page": page}
                    try:
                        wait.until(EC.presence_of_element_located((By.CLASS_NAME, "search-result-list-table-data-row")))
                    except TimeoutException:
                        logger.info("no rows at condition=%s page=%s", condition_id, page)
                        break

                    raw_rows = retry_call(lambda: parse_page_rows(driver, By, logger), args.retries, logger, "parse page")
                    state["stats"]["rows_seen"] += len(raw_rows)

                    new_rows: List[Dict[str, Any]] = []
                    for row in raw_rows:
                        name = row.get("企業名", "")
                        address = row.get("住所", "")
                        key = make_dedupe_key(name, address)
                        if dedupe_store.seen(key):
                            state["stats"]["rows_duplicated"] += 1
                            continue
                        row["取得日時"] = now_iso()
                        row["検索条件ID"] = condition_id
                        row["取得ページ番号"] = page
                        row["重複判定キー"] = key
                        new_rows.append(row)

                    sink.append_rows(new_rows)
                    for row in new_rows:
                        dedupe_store.insert(
                            row["重複判定キー"], row["企業名"], row["住所"], row["検索条件ID"], int(row["取得ページ番号"])
                        )
                    state["stats"]["rows_written"] += len(new_rows)
                    logger.info(
                        "page done condition=%s page=%s seen=%s written=%s dup=%s",
                        condition_id,
                        page,
                        len(raw_rows),
                        len(new_rows),
                        len(raw_rows) - len(new_rows),
                    )
                    state_store.save(state)

                    try:
                        old_html = driver.find_element(By.TAG_NAME, "html")
                        next_btn = WebDriverWait(driver, args.short_timeout_sec).until(
                            EC.element_to_be_clickable((By.CLASS_NAME, "btn-next-page"))
                        )
                        driver.execute_script("arguments[0].click();", next_btn)
                        wait.until(EC.staleness_of(old_html))
                        page += 1
                    except TimeoutException:
                        break

                state["stats"]["conditions_done"] += 1
                state["cursor"] = {"sales_index": si, "industry_index": ii + 1, "page": 1}
                state_store.save(state)

        state["status"] = "completed"
        state_store.save(state)
        logger.info("completed rows_written=%s", state["stats"]["rows_written"])
        return 0

    except Exception as e:  # noqa: BLE001
        state["status"] = "failed"
        state["stats"]["errors"] += 1
        state["last_error"] = str(e)
        state_store.save(state)
        logger.exception("fatal error")
        return 1
    finally:
        dedupe_store.close()
        driver.quit()


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Sansan scraper with resume and dedupe")
    p.add_argument("--industries-csv", default="industries.csv")
    p.add_argument("--output-csv", default="")
    p.add_argument("--state-file", default="state/state.json")
    p.add_argument("--sqlite-file", default="state/dedupe.db")
    p.add_argument("--log-file", default="")
    p.add_argument("--resume", action="store_true")
    p.add_argument("--headless", action="store_true")
    p.add_argument("--max-conditions", type=int, default=0)
    p.add_argument("--timeout-sec", type=int, default=20)
    p.add_argument("--short-timeout-sec", type=int, default=3)
    p.add_argument("--retries", type=int, default=3)
    p.add_argument("--verbose", action="store_true")
    return p


if __name__ == "__main__":
    parser = build_parser()
    args = parser.parse_args()
    raise SystemExit(run(args))
