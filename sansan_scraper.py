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
from urllib.parse import urljoin
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


JST = timezone.utc

SALES_RANGES: List[Tuple[str, str]] = [
    ("5,000万", "1億"),
    ("1億", "3億"),
    ("3億", "5億"),
    ("5億", "10億"),
    ("10億", "30億"),
    ("30億", "50億"),
    ("50億", "100億"),
    ("100億", "300億"),
    ("300億", "500億"),
    ("500億", "1,000億"),
    ("1,000億", "3,000億"),
    ("3,000億", "5,000億"),
    ("5,000億", "1兆"),
]

SALES_RANGE_CHOICES = {
    "5000man-1oku": ("5,000万", "1億"),
    "1oku-3oku": ("1億", "3億"),
    "3oku-5oku": ("3億", "5億"),
    "5oku-10oku": ("5億", "10億"),
    "10-30": ("10億", "30億"),
    "30-50": ("30億", "50億"),
    "50-100": ("50億", "100億"),
    "100-300": ("100億", "300億"),
    "300-500": ("300億", "500億"),
    "500-1000": ("500億", "1,000億"),
    "1000-3000": ("1,000億", "3,000億"),
    "3000-5000": ("3,000億", "5,000億"),
    "5000-1cho": ("5,000億", "1兆"),
}

EMPLOYEE_RANGE_VALUES = ["5", "10", "30", "50", "100", "200", "300", "500", "1000", "3000", "5000", "10000"]
DEFAULT_EMPLOYEE_SPLITS: List[Tuple[Optional[str], Optional[str]]] = [
    (None, "100"),
    ("100", "300"),
    ("300", "1000"),
    ("1000", "3000"),
    ("3000", "10000"),
    ("10000", None),
]
PREFECTURES = [
    "北海道",
    "青森県",
    "岩手県",
    "宮城県",
    "秋田県",
    "山形県",
    "福島県",
    "茨城県",
    "栃木県",
    "群馬県",
    "埼玉県",
    "千葉県",
    "東京都",
    "神奈川県",
    "新潟県",
    "富山県",
    "石川県",
    "福井県",
    "山梨県",
    "長野県",
    "岐阜県",
    "静岡県",
    "愛知県",
    "三重県",
    "滋賀県",
    "京都府",
    "大阪府",
    "兵庫県",
    "奈良県",
    "和歌山県",
    "鳥取県",
    "島根県",
    "岡山県",
    "広島県",
    "山口県",
    "徳島県",
    "香川県",
    "愛媛県",
    "高知県",
    "福岡県",
    "佐賀県",
    "長崎県",
    "熊本県",
    "大分県",
    "宮崎県",
    "鹿児島県",
    "沖縄県",
]

INDUSTRY_OPTION_OVERRIDES = {
    ("select[data-is-major-group='True']", "廃棄物処理"): "R01",
    ("select[data-is-major-group='True']", "自動車整備"): "R02",
    ("select[data-is-major-group='True']", "機械等修理"): "R03",
    ("select[data-is-major-group='True']", "人材サービス"): "R04",
    ("select[data-is-major-group='True']", "警備"): "R05",
    ("select[data-is-major-group='True']", "業務請負、アウトソーシング"): "R06",
    ("select[data-is-major-group='True']", "物品賃貸"): "R07",
    ("select[data-is-major-group='True']", "郵便局"): "R08",
    ("select[data-is-major-group='True']", "協同組合"): "R09",
    ("select[data-is-major-group='True']", "その他の事業サービス"): "R98",
    ("select[data-is-major-group='True']", "その他のサービス"): "R99",
}

COLUMNS = [
    "会社名",
    "名刺所有枚数",
    "最終名刺交換日",
    "役員・管理職",
    "URL",
    "郵便番号",
    "住所",
    "電話番号",
    "代表者の役職名",
    "代表者の氏名",
    "主業：大分類",
    "主業：中分類",
    "従業：大分類",
    "従業：中分類",
    "従業員数",
    "資本金（円）",
    "売上高（円）",
    "決算年月",
    "創業年月",
    "設立年月",
    "株式公開区分",
    "法人番号",
    "会社キーワード",
    "会社メモ",
    "取得日時",
    "検索条件ID",
    "取得ページ番号",
    "重複判定キー",
]

INDUSTRY_CONDITION_ITEM_SELECTOR = (
    "#sansan-industrial-classification-conditions > div > ul > li.sansan-industrial-classification-condition-detail-item"
)
INDUSTRY_DIVISION_SELECTOR = (
    f"{INDUSTRY_CONDITION_ITEM_SELECTOR} > div.sansan-industrial-classification-input-area > div:nth-child(2) > span > select"
)
INDUSTRY_MAJOR_GROUP_SELECTOR = (
    f"{INDUSTRY_CONDITION_ITEM_SELECTOR} > div.sansan-industrial-classification-input-area > div:nth-child(3) > span > select"
)
INDUSTRY_CODE_INPUT_SELECTOR = f"{INDUSTRY_CONDITION_ITEM_SELECTOR} input[data-input-sicc]"


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
            writer = csv.DictWriter(f, fieldnames=COLUMNS, extrasaction="ignore")
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
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        expected = {"大分類", "中分類"}
        if not reader.fieldnames or not expected.issubset(set(reader.fieldnames)):
            raise ValueError("industries.csv のヘッダーは 大分類,中分類 が必要です")
        rows = []
        for row in reader:
            major = (row.get("大分類") or "").strip()
            middle = (row.get("中分類") or "").strip()
            minor = (row.get("小分類") or "").strip()
            if not major and not middle:
                continue
            rows.append({"大分類": major, "中分類": middle, "小分類": minor})
        return rows


def dump_debug_artifacts(driver, label: str, logger: logging.Logger) -> None:
    debug_dir = Path("debug")
    debug_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    html_path = debug_dir / f"{label}_{stamp}.html"
    png_path = debug_dir / f"{label}_{stamp}.png"
    try:
        html_path.write_text(driver.page_source, encoding="utf-8")
        logger.info("debug html saved: %s", html_path)
    except Exception as exc:  # noqa: BLE001
        logger.warning("failed to save debug html: %s", exc)
    try:
        driver.save_screenshot(str(png_path))
        logger.info("debug screenshot saved: %s", png_path)
    except Exception as exc:  # noqa: BLE001
        logger.warning("failed to save debug screenshot: %s", exc)


def find_first(driver, by: str, selectors: Iterable[str]):
    for selector in selectors:
        elements = driver.find_elements(by, selector)
        if elements:
            return elements[0]
    return None


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


def build_condition_id(
    sales_from: str,
    sales_to: str,
    industry: Dict[str, str],
    employee_from: Optional[str] = None,
    employee_to: Optional[str] = None,
    location: Optional[str] = None,
) -> str:
    parts = [industry["大分類"], industry["中分類"]]
    if industry.get("小分類"):
        parts.append(industry["小分類"])
    condition_id = f"{sales_from}-{sales_to}|{'>'.join(parts)}"
    if employee_from or employee_to:
        condition_id += f"|従業員数:{employee_from or '--'}-{employee_to or '--'}"
    if location:
        condition_id += f"|住所:{location}"
    return condition_id


def make_split_task(
    employee_from: Optional[str] = None,
    employee_to: Optional[str] = None,
    location: Optional[str] = None,
    page: int = 1,
    split_level: str = "base",
) -> Dict[str, Any]:
    return {
        "employee_from": employee_from,
        "employee_to": employee_to,
        "location": location,
        "page": page,
        "split_level": split_level,
    }


def build_split_tasks_for_employee() -> List[Dict[str, Any]]:
    return [
        make_split_task(employee_from=employee_from, employee_to=employee_to, split_level="employee")
        for employee_from, employee_to in DEFAULT_EMPLOYEE_SPLITS
    ]


def build_split_tasks_for_prefecture(task: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [
        make_split_task(
            employee_from=task.get("employee_from"),
            employee_to=task.get("employee_to"),
            location=prefecture,
            split_level="prefecture",
        )
        for prefecture in PREFECTURES
    ]


def set_cursor_state(state: Dict[str, Any], sales_index: int, industry_index: int, page: int) -> None:
    state["cursor"] = {"sales_index": sales_index, "industry_index": industry_index, "page": page}


def set_split_context(
    state: Dict[str, Any],
    sales_index: int,
    industry_index: int,
    pending_tasks: Optional[List[Dict[str, Any]]],
) -> None:
    state["split_context"] = (
        {"sales_index": sales_index, "industry_index": industry_index, "pending_tasks": pending_tasks}
        if pending_tasks
        else None
    )


def persist_current_task(
    state: Dict[str, Any],
    sales_index: int,
    industry_index: int,
    page: int,
    current_task: Dict[str, Any],
    remaining_tasks: List[Dict[str, Any]],
) -> None:
    current_task["page"] = page
    set_cursor_state(state, sales_index, industry_index, page)
    set_split_context(state, sales_index, industry_index, [current_task] + remaining_tasks)


def parse_total_count(driver, By) -> Optional[int]:
    try:
        element = driver.find_element(By.CSS_SELECTOR, "#company-index-total-count")
        text = (element.text or "").strip()
        if not text:
            return None
        digits = re.sub(r"[^\d]", "", text)
        return int(digits) if digits else None
    except Exception:
        return None


def industry_division(industry: Dict[str, str]) -> str:
    return next(iter(industry.values()), "")


def industry_major_group(industry: Dict[str, str]) -> str:
    values = list(industry.values())
    return values[1] if len(values) > 1 else ""


def selected_sales_indexes(args: argparse.Namespace) -> List[int]:
    if not args.sales_range:
        return list(range(len(SALES_RANGES)))
    target = SALES_RANGE_CHOICES[args.sales_range]
    return [i for i, pair in enumerate(SALES_RANGES) if pair == target]


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


def selected_option_text_from_element(element, By) -> str:
    try:
        selected = element.find_elements(By.CSS_SELECTOR, "option:checked")
        for option in selected:
            text = option.text.strip()
            if text:
                return text
        value = (element.get_attribute("value") or "").strip()
        if value:
            for option in element.find_elements(By.TAG_NAME, "option"):
                if (option.get_attribute("value") or "").strip() == value:
                    text = option.text.strip()
                    if text:
                        return text
    except Exception:  # noqa: BLE001
        return ""
    return ""


def sync_industry_condition_input(driver, element, value: str) -> None:
    try:
        driver.execute_script(
            """
            const select = arguments[0];
            const value = arguments[1];
            const area = select.closest('.sansan-industrial-classification-input-area');
            if (!area) return;
            const hidden = area.querySelector('input[data-input-sicc]');
            if (!hidden) return;
            if (select.getAttribute('data-is-major-group') === 'True') {
              hidden.value = value || '';
            } else if (select.getAttribute('data-is-division') === 'True') {
              hidden.value = '';
            }
            """,
            element,
            value,
        )
    except Exception:  # noqa: BLE001
        return


def select_option_by_text(driver, wait, By, css_selector: str, option_text: str):
    def target_elements():
        if css_selector == "select[data-is-division='True']":
            elements = driver.find_elements(By.CSS_SELECTOR, INDUSTRY_DIVISION_SELECTOR)
            if elements:
                return elements
        if css_selector == "select[data-is-major-group='True']":
            elements = driver.find_elements(By.CSS_SELECTOR, INDUSTRY_MAJOR_GROUP_SELECTOR)
            if elements:
                return elements
        try:
            root = driver.find_element(By.CSS_SELECTOR, "#sansan-industrial-classification-conditions")
            elements = root.find_elements(By.CSS_SELECTOR, css_selector)
            if elements:
                return elements
        except Exception:  # noqa: BLE001
            pass
        return driver.find_elements(By.CSS_SELECTOR, css_selector)

    def has_selected_option_text() -> bool:
        for element in target_elements():
            try:
                selected = element.find_elements(By.CSS_SELECTOR, "option:checked")
                for option in selected:
                    if option.text.strip() == option_text:
                        return True
            except Exception:  # noqa: BLE001
                continue
        return False

    def has_selected_value() -> bool:
        for element in target_elements():
            try:
                value = (element.get_attribute("value") or "").strip()
                if value:
                    return True
            except Exception:  # noqa: BLE001
                continue
        return False

    def apply_override():
        override_value = INDUSTRY_OPTION_OVERRIDES.get((css_selector, option_text))
        if not override_value and css_selector == INDUSTRY_DIVISION_SELECTOR:
            override_value = INDUSTRY_OPTION_OVERRIDES.get(("select[data-is-division='True']", option_text))
        if not override_value and css_selector == INDUSTRY_MAJOR_GROUP_SELECTOR:
            override_value = INDUSTRY_OPTION_OVERRIDES.get(("select[data-is-major-group='True']", option_text))
        if not override_value:
            return None
        for element in target_elements():
            try:
                driver.execute_script(
                    """
                    const select = arguments[0];
                    const value = arguments[1];
                    const text = arguments[2];
                    let option = Array.from(select.options).find((opt) => opt.value === value);
                    if (!option) {
                      option = new Option(text, value, true, true);
                      select.add(option);
                    } else {
                      option.selected = true;
                    }
                    select.value = value;
                    select.dispatchEvent(new Event('change', { bubbles: true }));
                    """,
                    element,
                    override_value,
                    option_text,
                )
                sync_industry_condition_input(driver, element, override_value)
                return element
            except Exception:  # noqa: BLE001
                continue
        return None

    def select_via_ajax():
        script = r"""
const cssSelector = arguments[0];
const optionText = arguments[1];
const done = arguments[arguments.length - 1];

const root = document.querySelector('#sansan-industrial-classification-conditions') || document;
const select = root.querySelector(cssSelector);
if (!select) {
  done({ok: false, reason: "select not found"});
  return;
}

const dataUrl = select.getAttribute("data-url");
if (!dataUrl) {
  done({ok: false, reason: "data-url not found"});
  return;
}

const targetText = optionText.trim();
const inputArea = select.closest('.sansan-industrial-classification-input-area');
const divisionSelect = inputArea ? inputArea.querySelector("select[data-is-division='True']") : null;
const ancestorValue = (divisionSelect && divisionSelect.value) || "";

const queryBases = [
  { q: targetText, term: targetText },
  { q: targetText },
  { term: targetText },
  { text: targetText },
  { keyword: targetText }
];

function collectCandidates(payload, bucket) {
  if (!payload) return;
  if (Array.isArray(payload)) {
    for (const item of payload) collectCandidates(item, bucket);
    return;
  }
  if (typeof payload !== "object") return;

  const text = payload.text ?? payload.label ?? payload.name ?? payload.title ?? "";
  const value = payload.id ?? payload.value ?? payload.key ?? payload.code ?? "";
  if (text || value) {
    bucket.push({ text: String(text).trim(), value: String(value).trim() });
  }
  for (const value of Object.values(payload)) {
    if (value && typeof value === "object") collectCandidates(value, bucket);
  }
}

function applyOption(value, text) {
  let option = Array.from(select.options).find((opt) => opt.value === value);
  if (!option) {
    option = new Option(text, value, true, true);
    select.add(option);
  } else {
    option.selected = true;
  }
  select.value = value;
  select.dispatchEvent(new Event("change", { bubbles: true }));
  done({ok: true, value, text});
}

(async () => {
  for (const baseQuery of queryBases) {
    const url = new URL(dataUrl, location.origin);
    const query = { ...baseQuery };
    if (ancestorValue && select.getAttribute('data-is-major-group') === 'True') {
      query.ancestor = ancestorValue;
      query.ancestorCode = ancestorValue;
      query.ancestorId = ancestorValue;
      query.parent = ancestorValue;
      query.parentValue = ancestorValue;
      query.code = ancestorValue;
    }
    for (const [key, value] of Object.entries(query)) {
      url.searchParams.set(key, value);
    }
    try {
      const response = await fetch(url.toString(), {
        credentials: "same-origin",
        headers: { "X-Requested-With": "XMLHttpRequest" }
      });
      if (!response.ok) continue;
      const payload = await response.json();
      const candidates = [];
      collectCandidates(payload, candidates);
      const match = candidates.find((item) => item.text === targetText) ||
        candidates.find((item) => item.text.includes(targetText));
      if (match && match.value) {
        applyOption(match.value, match.text || targetText);
        return;
      }
    } catch (error) {
      // try next query shape
    }
  }
  done({ok: false, reason: "ajax option not found", targetText});
})();
"""
        result = driver.execute_async_script(script, css_selector, option_text)
        if result and result.get("ok"):
            for element in target_elements():
                if selected_option_text_from_element(element, By) == option_text:
                    return element
        return None

    def select_via_modal():
        script = r"""
const cssSelector = arguments[0];
const optionText = arguments[1];
const done = arguments[arguments.length - 1];

const root = document.querySelector('#sansan-industrial-classification-conditions') || document;
const select = root.querySelector(cssSelector);
const targetLink = root.querySelector('.target-list');
if (!select || !targetLink) {
  done({ok: false, reason: 'modal source not found'});
  return;
}

const remoteUrl = targetLink.getAttribute('data-remote');
if (!remoteUrl) {
  done({ok: false, reason: 'modal remote url not found'});
  return;
}

function findCodeAround(node) {
  let current = node;
  for (let i = 0; i < 5 && current; i += 1) {
    const input = current.querySelector?.('input[type="checkbox"], input[type="radio"], input[type="hidden"]');
    if (input) {
      return input.value || input.getAttribute('data-value') || input.getAttribute('data-code') || '';
    }
    current = current.parentElement;
  }
  return '';
}

(async () => {
  try {
    const response = await fetch(new URL(remoteUrl, location.origin).toString(), {
      credentials: 'same-origin',
      headers: { 'X-Requested-With': 'XMLHttpRequest' }
    });
    if (!response.ok) {
      done({ok: false, reason: 'modal fetch failed'});
      return;
    }
    const html = await response.text();
    const doc = new DOMParser().parseFromString(html, 'text/html');
    const candidates = Array.from(doc.querySelectorAll('label, span, a, li, div'));
    const exact = candidates.find((node) => node.textContent && node.textContent.trim() === optionText.trim());
    const partial = candidates.find((node) => node.textContent && node.textContent.includes(optionText.trim()));
    const matchNode = exact || partial;
    if (!matchNode) {
      done({ok: false, reason: 'modal text not found'});
      return;
    }
    const code = findCodeAround(matchNode);
    if (!code) {
      done({ok: false, reason: 'modal code not found'});
      return;
    }

    let option = Array.from(select.options).find((opt) => opt.value === code);
    if (!option) {
      option = new Option(optionText, code, true, true);
      select.add(option);
    } else {
      option.selected = true;
    }
    select.value = code;
    select.dispatchEvent(new Event('change', { bubbles: true }));

    const root = select.closest('.sansan-industrial-classification-input-area');
    const hidden = root ? root.querySelector('input[data-input-sicc]') : null;
    if (hidden) {
      hidden.value = code;
    }
    done({ok: true, code});
  } catch (error) {
    done({ok: false, reason: String(error)});
  }
})();
"""
        result = driver.execute_async_script(script, css_selector, option_text)
        if result and result.get("ok"):
            for element in target_elements():
                if selected_option_text_from_element(element, By) == option_text:
                    return element
        return None

    def find_select():
        for element in target_elements():
            options = element.find_elements(By.TAG_NAME, "option")
            for option in options:
                if option.text.strip() == option_text:
                    return element, option.get_attribute("value")
        return None

    def find_visible_select():
        for element in target_elements():
            try:
                container = element.find_element(
                    By.XPATH,
                    "./following-sibling::span[contains(@class, 'select2')][1]",
                )
                if container.is_displayed():
                    return element
            except Exception:  # noqa: BLE001
                continue
        return None

    last_error = None
    for _ in range(3):
        try:
            override_element = apply_override()
            if override_element is not None:
                return override_element
            result = find_select()
            if result is not None:
                element, value = result
                driver.execute_script(
                    """
                    const select = arguments[0];
                    const value = arguments[1];
                    select.value = value;
                    select.dispatchEvent(new Event('change', { bubbles: true }));
                    """,
                    element,
                    value,
                )
                sync_industry_condition_input(driver, element, value)
                return element

            select_element = wait.until(lambda d: find_visible_select())
            opened = driver.execute_script(
                """
                const select = arguments[0];
                if (window.jQuery) {
                    const $select = window.jQuery(select);
                    if ($select.data('select2')) {
                        $select.select2('open');
                        return true;
                    }
                }
                return false;
                """,
                select_element,
            )
            if not opened:
                select2_container = select_element.find_element(
                    By.XPATH,
                    "./following-sibling::span[contains(@class, 'select2')][1]",
                )
                driver.execute_script("arguments[0].click();", select2_container)

            search_box = wait.until(lambda d: d.find_element(By.CLASS_NAME, "select2-search__field"))
            search_box.clear()
            search_box.send_keys(option_text)
            time.sleep(0.5)
            search_box.send_keys("\ue007")

            try:
                wait.until(
                    lambda d: has_selected_option_text() or has_selected_value() or find_select() is not None
                )
                result = find_select()
                if result is not None:
                    element, _ = result
                    sync_industry_condition_input(driver, element, element.get_attribute("value") or "")
                    return element
                visible = find_visible_select()
                if visible is not None:
                    return visible
                return None
            except Exception:
                result_option_xpath = (
                    f"//li[contains(@class, 'select2-results__option') and contains(normalize-space(), '{option_text}')]"
                )
                result_option = wait.until(lambda d: d.find_element(By.XPATH, result_option_xpath))
                driver.execute_script("arguments[0].click();", result_option)
                result = find_select()
                if result is not None:
                    element, _ = result
                    sync_industry_condition_input(driver, element, element.get_attribute("value") or "")
                    return element
                visible = find_visible_select()
                if visible is not None:
                    return visible
                return None
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            try:
                ajax_element = select_via_ajax()
                if ajax_element is not None:
                    return ajax_element
            except Exception as ajax_exc:  # noqa: BLE001
                last_error = ajax_exc
            try:
                modal_element = select_via_modal()
                if modal_element is not None:
                    return modal_element
            except Exception as modal_exc:  # noqa: BLE001
                last_error = modal_exc
            time.sleep(0.5)

    raise RuntimeError(f"failed to select option '{option_text}' for selector '{css_selector}': {last_error}")


def row_signature(driver, By) -> Optional[str]:
    try:
        rows = driver.find_elements(By.CLASS_NAME, "search-result-list-table-data-row")
        if not rows:
            return None
        parts = []
        for row in rows[:5]:
            try:
                parts.append((row.get_attribute("data-latest-soc") or row.text or "").strip())
            except Exception:
                continue
        signature = "||".join(part for part in parts if part)
        return signature or None
    except Exception:
        return None


def result_marker(driver, By) -> str:
    sig = row_signature(driver, By) or ""
    label = pager_label(driver, By)
    return f"{label}||{sig}"


def pager_label(driver, By) -> str:
    try:
        element = driver.find_element(By.CSS_SELECTOR, "ul.search-result-page-nav button.dropdown-toggle")
        return element.text.strip()
    except Exception:
        return ""


def selected_option_text(driver, By, css_selector: str) -> str:
    for element in driver.find_elements(By.CSS_SELECTOR, css_selector):
        try:
            selected = element.find_elements(By.CSS_SELECTOR, "option:checked")
            for option in selected:
                text = option.text.strip()
                if text:
                    return text
            value = (element.get_attribute("value") or "").strip()
            if value:
                for option in element.find_elements(By.TAG_NAME, "option"):
                    if (option.get_attribute("value") or "").strip() == value:
                        text = option.text.strip()
                        if text:
                            return text
        except Exception:  # noqa: BLE001
            continue
    return ""


def selected_industry_option_text(driver, By, css_selector: str) -> str:
    root = driver.find_element(By.CSS_SELECTOR, "#sansan-industrial-classification-conditions")
    for element in root.find_elements(By.CSS_SELECTOR, css_selector):
        try:
            selected = element.find_elements(By.CSS_SELECTOR, "option:checked")
            for option in selected:
                text = option.text.strip()
                if text:
                    return text
            value = (element.get_attribute("value") or "").strip()
            if value:
                for option in element.find_elements(By.TAG_NAME, "option"):
                    if (option.get_attribute("value") or "").strip() == value:
                        text = option.text.strip()
                        if text:
                            return text
        except Exception:  # noqa: BLE001
            continue
    return ""


def reset_industry_conditions(driver) -> None:
    driver.execute_script(
        """
        const root = document.querySelector('#sansan-industrial-classification-conditions');
        if (!root) {
          return;
        }
        const hiddenInputs = root.querySelectorAll('input[data-input-sicc]');
        hiddenInputs.forEach((input) => {
          input.value = '';
        });
        const selects = root.querySelectorAll("select[data-is-division='True'], select[data-is-major-group='True']");
        selects.forEach((select) => {
          select.innerHTML = '<option value=""></option>';
          select.value = '';
          select.dispatchEvent(new Event('change', { bubbles: true }));
        });
        root.querySelectorAll('.select2-selection__rendered').forEach((rendered) => {
          const select = rendered.closest('span')?.querySelector("select[data-is-division='True'], select[data-is-major-group='True']");
          if (!select) {
            rendered.textContent = '';
            return;
          }
          rendered.textContent = select.getAttribute('data-placeholder') || '';
          rendered.removeAttribute('title');
        });
        document.querySelectorAll('.select2-container--open').forEach((node) => {
          node.classList.remove('select2-container--open');
        });
        document.querySelectorAll('.select2-dropdown').forEach((node) => {
          node.remove();
        });
        """
    )


def fetch_next_page_via_xhr(driver, href: str):
    script = """
    const href = arguments[0];
    const done = arguments[arguments.length - 1];
    const token =
      document.querySelector('#company-index input[name="__RequestVerificationToken"]') ||
      document.querySelector('input[name="__RequestVerificationToken"]');

    fetch(href, {
      method: 'GET',
      credentials: 'include',
      headers: {
        'X-Requested-With': 'XMLHttpRequest',
        ...(token ? { 'RequestVerificationToken': token.value } : {})
      }
    }).then(async (resp) => {
      const text = await resp.text();
      done({ ok: resp.ok, status: resp.status, text });
    }).catch((err) => {
      done({ ok: false, status: 0, error: String(err) });
    });
    """
    try:
        return driver.execute_async_script(script, href)
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "status": -1, "error": str(exc)}


def goto_next_page(driver, wait, By, logger: logging.Logger) -> bool:
    next_buttons = driver.find_elements(By.CSS_SELECTOR, "a.btn-next-page")
    if not next_buttons:
        logger.info("next page button not found url=%s", driver.current_url)
        return False
    next_button = next_buttons[0]
    href = next_button.get_attribute("href")
    if not href:
        logger.info("next page href missing url=%s", driver.current_url)
        return False

    current_sig = row_signature(driver, By)
    current_url = driver.current_url
    current_label = pager_label(driver, By)
    logger.info(
        "attempt next page current_url=%s next_href=%s current_sig=%s current_label=%s",
        current_url,
        href,
        current_sig,
        current_label,
    )

    click_exc = None
    try:
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", next_button)
        driver.execute_script(
            """
            const el = arguments[0];
            el.dispatchEvent(new MouseEvent('mouseover', {bubbles: true}));
            el.dispatchEvent(new MouseEvent('mousedown', {bubbles: true}));
            el.dispatchEvent(new MouseEvent('mouseup', {bubbles: true}));
            el.dispatchEvent(new MouseEvent('click', {bubbles: true, cancelable: true, view: window}));
            """,
            next_button,
        )
        wait.until(lambda d: d.execute_script("return document.readyState") == "complete")
        wait.until(
            lambda d: row_signature(d, By) not in (None, current_sig) or pager_label(d, By) != current_label
        )
        logger.info(
            "next page moved by event url=%s new_sig=%s new_label=%s",
            driver.current_url,
            row_signature(driver, By),
            pager_label(driver, By),
        )
        return True
    except Exception as click_exc:  # noqa: BLE001
        logger.warning("next page click failed: %s", click_exc)
        if "invalid session id" in str(click_exc).lower():
            raise RuntimeError("webdriver session lost during next page click") from click_exc

    xhr_result = fetch_next_page_via_xhr(driver, href)
    logger.info("next page xhr result status=%s ok=%s", xhr_result.get("status"), xhr_result.get("ok"))
    if "invalid session id" in str(xhr_result.get("error", "")).lower():
        raise RuntimeError("webdriver session lost during next page xhr fallback") from click_exc

    if xhr_result.get("ok") and xhr_result.get("text"):
        driver.execute_script(
            """
            const html = arguments[0];
            const parser = new DOMParser();
            const doc = parser.parseFromString(html, 'text/html');

            const currentResult = document.querySelector('#company-index-search-result-list');
            const nextResult = doc.querySelector('#company-index-search-result-list');
            if (currentResult && nextResult) {
              currentResult.replaceWith(nextResult);
            }

            const currentPager = document.querySelector('ul.search-result-page-nav');
            const pagers = doc.querySelectorAll('ul.search-result-page-nav');
            const nextPager = pagers.length ? pagers[pagers.length - 1] : null;
            if (currentPager && nextPager) {
              currentPager.replaceWith(nextPager);
            }
            """,
            xhr_result["text"],
        )
        wait.until(
            lambda d: row_signature(d, By) not in (None, current_sig) or pager_label(d, By) != current_label
        )
        logger.info(
            "next page moved by xhr swap url=%s new_sig=%s new_label=%s",
            driver.current_url,
            row_signature(driver, By),
            pager_label(driver, By),
        )
        return True

    dump_debug_artifacts(driver, "next_page_failure", logger)
    if click_exc is not None:
        raise RuntimeError(f"failed to move to next page href={href}") from click_exc
    raise RuntimeError(f"failed to move to next page href={href}")


def parse_page_rows(driver, By, logger: logging.Logger) -> List[Dict[str, str]]:
    script = r"""
const pickText = (row, className) => {
  const cell = row.querySelector(`.${className}`);
  if (!cell) return "";
  const titled = cell.querySelector("[title]");
  if (titled && titled.getAttribute("title") != null) {
    return titled.getAttribute("title").trim();
  }
  return (cell.textContent || "").trim();
};

const rows = Array.from(document.querySelectorAll(".search-result-list-table-data-row"));
return rows.map((row) => {
  const urlAnchor = row.querySelector(".url a");
  return {
    "会社名": pickText(row, "company-name-label"),
    "_company_name": pickText(row, "company-name-label"),
    "名刺所有枚数": pickText(row, "number-of-bizcards"),
    "最終名刺交換日": pickText(row, "last-bizcard-exchanged-at"),
    "役員・管理職": pickText(row, "officer-and-manager"),
    "URL": urlAnchor ? (urlAnchor.getAttribute("href") || "").trim() : "",
    "郵便番号": pickText(row, "postal-code"),
    "住所": pickText(row, "location"),
    "電話番号": pickText(row, "phone-number"),
    "代表者の役職名": pickText(row, "representative-title"),
    "代表者の氏名": pickText(row, "representative-name"),
    "主業：大分類": pickText(row, "sansan-industrial-classification-1-division"),
    "主業：中分類": pickText(row, "sansan-industrial-classification-1-major-group"),
    "従業：大分類": pickText(row, "sansan-industrial-classification-2-division"),
    "従業：中分類": pickText(row, "sansan-industrial-classification-2-major-group"),
    "従業員数": pickText(row, "employee-number"),
    "資本金（円）": pickText(row, "legal-capital"),
    "売上高（円）": pickText(row, "latest-sales-accounting-term-sales"),
    "決算年月": pickText(row, "latest-sales-accounting-term"),
    "創業年月": pickText(row, "established-at"),
    "設立年月": pickText(row, "created-at"),
    "株式公開区分": pickText(row, "public-offering"),
    "法人番号": pickText(row, "corporate-number"),
    "会社キーワード": pickText(row, "company-keyword"),
    "会社メモ": pickText(row, "company-memo")
  };
});
"""
    try:
        result = driver.execute_script(script)
        return result if isinstance(result, list) else []
    except Exception as exc:  # noqa: BLE001
        logger.warning("page parse via js failed: %s", exc)
        raise


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
    allowed_sales_indexes = set(selected_sales_indexes(args))
    logger.info("sales ranges selected: %s", [SALES_RANGES[i] for i in sorted(allowed_sales_indexes)])

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
        "split_context": None,
    }

    if args.resume:
        loaded = state_store.load()
        if loaded:
            state = loaded
            logger.info("resume enabled; restored state from %s", state_path)

    if args.cursor_sales_index is not None or args.cursor_industry_index is not None or args.cursor_page is not None:
        set_cursor_state(
            state,
            args.cursor_sales_index if args.cursor_sales_index is not None else state["cursor"].get("sales_index", 0),
            args.cursor_industry_index if args.cursor_industry_index is not None else state["cursor"].get("industry_index", 0),
            args.cursor_page if args.cursor_page is not None else state["cursor"].get("page", 1),
        )
        state["status"] = "running"
        state["last_error"] = None
        state_store.save(state)
        logger.info("cursor overridden to sales_index=%s industry_index=%s page=%s", state["cursor"]["sales_index"], state["cursor"]["industry_index"], state["cursor"]["page"])

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
        last_condition_context: Dict[str, str] = {"condition_id": "", "marker": ""}
        for si, (sales_from, sales_to) in enumerate(SALES_RANGES):
            if si not in allowed_sales_indexes:
                continue
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
                if (
                    state.get("split_context")
                    and state["split_context"].get("sales_index") == si
                    and state["split_context"].get("industry_index") == ii
                ):
                    task_queue = state["split_context"].get("pending_tasks", [])
                else:
                    initial_page = start_cursor.page if (si == start_cursor.sales_index and ii == start_cursor.industry_index) else 1
                    initial_level = "manual" if (args.employee_from or args.employee_to or args.location) else "base"
                    task_queue = [
                        make_split_task(
                            employee_from=args.employee_from,
                            employee_to=args.employee_to,
                            location=args.location,
                            page=initial_page,
                            split_level=initial_level,
                        )
                    ]

                while task_queue:
                    current_task = task_queue.pop(0)
                    page = int(current_task.get("page", 1))
                    condition_id = build_condition_id(
                        sales_from,
                        sales_to,
                        industry,
                        current_task.get("employee_from"),
                        current_task.get("employee_to"),
                        current_task.get("location"),
                    )
                    persist_current_task(state, si, ii, page, current_task, task_queue)
                    state_store.save(state)
                    logger.info("condition start: %s page=%s", condition_id, page)
                    search_context: Dict[str, Any] = {"total_count": None}

                    def open_and_search():
                        step = "open page"
                        try:
                            driver.get("https://ap.sansan.com/v/companies/")
                            wait.until(lambda d: d.execute_script("return document.readyState") == "complete")
                            step = "capture pre-search state"
                            before_sig = row_signature(driver, By)
                            before_label = pager_label(driver, By)
                            before_total_count = parse_total_count(driver, By)

                            step = "find sales range selects"
                            sales_from_el = wait.until(
                                lambda d: find_first(
                                    d,
                                    By.CSS_SELECTOR,
                                    [
                                        "#SearchInput_LatestSalesAccountingTermSalesFrom",
                                        "select[id*='LatestSalesAccountingTermSalesFrom']",
                                        "select[name*='LatestSalesAccountingTermSalesFrom']",
                                    ],
                                )
                            )
                            sales_to_el = wait.until(
                                lambda d: find_first(
                                    d,
                                    By.CSS_SELECTOR,
                                    [
                                        "#SearchInput_LatestSalesAccountingTermSalesTo",
                                        "select[id*='LatestSalesAccountingTermSalesTo']",
                                        "select[name*='LatestSalesAccountingTermSalesTo']",
                                    ],
                                )
                            )

                            if sales_from_el is None or sales_to_el is None:
                                raise RuntimeError("sales fields not found")

                            step = f"set sales from={sales_from} to={sales_to}"
                            Select(sales_from_el).select_by_visible_text(sales_from)
                            Select(sales_to_el).select_by_visible_text(sales_to)

                            step = "find employee number selects"
                            employee_from_el = find_first(
                                driver,
                                By.CSS_SELECTOR,
                                [
                                    "#SearchInput_EmployeeNumberFrom",
                                    "select[id*='EmployeeNumberFrom']",
                                    "select[name='EmployeeNumberFrom']",
                                ],
                            )
                            employee_to_el = find_first(
                                driver,
                                By.CSS_SELECTOR,
                                [
                                    "#SearchInput_EmployeeNumberTo",
                                    "select[id*='EmployeeNumberTo']",
                                    "select[name='EmployeeNumberTo']",
                                ],
                            )
                            if employee_from_el is None or employee_to_el is None:
                                raise RuntimeError("employee number fields not found")
                            step = (
                                f"set employee range from={current_task.get('employee_from') or '--'} "
                                f"to={current_task.get('employee_to') or '--'}"
                            )
                            Select(employee_from_el).select_by_value(current_task.get("employee_from") or "")
                            Select(employee_to_el).select_by_value(current_task.get("employee_to") or "")

                            step = "set location"
                            location_el = find_first(
                                driver,
                                By.CSS_SELECTOR,
                                [
                                    "#SearchInput_Location",
                                    "input[id*='Location']",
                                    "input[name='Location']",
                                ],
                            )
                            if location_el is None:
                                raise RuntimeError("location field not found")
                            location_el.clear()
                            if current_task.get("location"):
                                location_el.send_keys(current_task["location"])

                            step = "reset industry conditions"
                            reset_industry_conditions(driver)

                            pending_division = industry_division(industry)
                            pending_major_group = industry_major_group(industry)

                            step = f"set division={pending_division}"
                            division_select = select_option_by_text(
                                driver, wait, By, INDUSTRY_DIVISION_SELECTOR, pending_division
                            )
                            step = f"set major group={pending_major_group}"
                            major_group_select = select_option_by_text(
                                driver, wait, By, INDUSTRY_MAJOR_GROUP_SELECTOR, pending_major_group
                            )

                            step = "verify selected filters"
                            selected_division = selected_option_text_from_element(division_select, By) if division_select else ""
                            selected_major_group = (
                                selected_option_text_from_element(major_group_select, By) if major_group_select else ""
                            )
                            if selected_division != pending_division:
                                logger.warning(
                                    "division verify mismatch selected='%s' expected='%s'",
                                    selected_division,
                                    pending_division,
                                )
                            if selected_major_group != pending_major_group:
                                logger.warning(
                                    "major group verify mismatch selected='%s' expected='%s'",
                                    selected_major_group,
                                    pending_major_group,
                                )

                            step = "submit detail search"
                            search_button = wait.until(EC.element_to_be_clickable((By.ID, "button-detail-search")))
                            driver.execute_script("arguments[0].click();", search_button)
                            wait.until(lambda d: d.execute_script("return document.readyState") == "complete")
                            step = "wait for search results change"
                            try:
                                changed = WebDriverWait(driver, args.short_timeout_sec + 5).until(
                                    lambda d: (
                                        row_signature(d, By) != before_sig
                                        or pager_label(d, By) != before_label
                                        or parse_total_count(d, By) != before_total_count
                                    )
                                )
                            except Exception:
                                total_count = parse_total_count(driver, By)
                                if total_count == 0:
                                    logger.info(
                                        "search results unchanged but zero-count accepted condition=%s",
                                        condition_id,
                                    )
                                    changed = True
                                else:
                                    raise
                            if not changed:
                                raise RuntimeError("search results did not change after submitting filters")
                            step = "validate result marker changed from previous condition"
                            current_marker = result_marker(driver, By)
                            previous_condition_id = last_condition_context["condition_id"]
                            previous_marker = last_condition_context["marker"]
                            if previous_condition_id and previous_condition_id != condition_id and current_marker == previous_marker:
                                raise RuntimeError(
                                    f"search results repeated previous condition marker previous='{previous_condition_id}' current='{condition_id}' marker='{current_marker}'"
                                )
                            total_count = parse_total_count(driver, By)
                            search_context["total_count"] = total_count
                            if total_count is not None:
                                logger.info("condition total_count=%s condition=%s", total_count, condition_id)
                        except Exception as exc:
                            logger.warning("search setup step failed at %s: %s", step, exc)
                            logger.warning(
                                "search page inspect url=%s title=%s iframes=%s",
                                driver.current_url,
                                driver.title,
                                len(driver.find_elements(By.TAG_NAME, "iframe")),
                            )
                            dump_debug_artifacts(driver, "search_setup_failure", logger)
                            raise

                    retry_call(open_and_search, args.retries, logger, "search setup")

                    total_count = search_context.get("total_count")
                    if page == 1 and total_count is not None and total_count > args.split_threshold:
                        if (
                            current_task.get("split_level") == "base"
                            and not current_task.get("employee_from")
                            and not current_task.get("employee_to")
                            and not current_task.get("location")
                        ):
                            split_tasks = build_split_tasks_for_employee()
                            logger.info(
                                "auto split by employee count total_count=%s threshold=%s condition=%s subtasks=%s",
                                total_count,
                                args.split_threshold,
                                condition_id,
                                len(split_tasks),
                            )
                            task_queue = split_tasks + task_queue
                            set_split_context(state, si, ii, task_queue)
                            state_store.save(state)
                            continue
                        if current_task.get("split_level") in ("employee", "manual") and not current_task.get("location"):
                            split_tasks = build_split_tasks_for_prefecture(current_task)
                            logger.info(
                                "auto split by prefecture total_count=%s threshold=%s condition=%s subtasks=%s",
                                total_count,
                                args.split_threshold,
                                condition_id,
                                len(split_tasks),
                            )
                            task_queue = split_tasks + task_queue
                            set_split_context(state, si, ii, task_queue)
                            state_store.save(state)
                            continue
                        logger.warning(
                            "condition still exceeds split threshold after auto split total_count=%s threshold=%s condition=%s",
                            total_count,
                            args.split_threshold,
                            condition_id,
                        )

                    for p in range(1, page):
                        try:
                            if not goto_next_page(driver, wait, By, logger):
                                raise RuntimeError("next page not found while fast-forwarding")
                        except Exception as e:  # noqa: BLE001
                            logger.error("resume fast-forward failed at page=%s: %s", p, e)
                            break

                    while True:
                        persist_current_task(state, si, ii, page, current_task, task_queue)
                        try:
                            wait.until(EC.presence_of_element_located((By.CLASS_NAME, "search-result-list-table-data-row")))
                        except TimeoutException:
                            if page > 1:
                                logger.warning("no rows after pagination url=%s page=%s", driver.current_url, page)
                                dump_debug_artifacts(driver, f"pagination_no_rows_page_{page}", logger)
                            logger.info("no rows at condition=%s page=%s", condition_id, page)
                            break

                        if page == 1:
                            last_condition_context["condition_id"] = condition_id
                            last_condition_context["marker"] = result_marker(driver, By)

                        raw_rows = retry_call(lambda: parse_page_rows(driver, By, logger), args.retries, logger, "parse page")
                        state["stats"]["rows_seen"] += len(raw_rows)

                        new_rows: List[Dict[str, Any]] = []
                        for row in raw_rows:
                            name = row.get("_company_name", "")
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
                                row["重複判定キー"], row["_company_name"], row["住所"], row["検索条件ID"], int(row["取得ページ番号"])
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
                            if not goto_next_page(driver, WebDriverWait(driver, args.short_timeout_sec), By, logger):
                                break
                            page += 1
                        except TimeoutException:
                            logger.info("next page timeout at condition=%s page=%s url=%s", condition_id, page, driver.current_url)
                            break
                        except Exception as e:  # noqa: BLE001
                            logger.warning("next page transition failed at condition=%s page=%s: %s", condition_id, page, e)
                            break

                    set_split_context(state, si, ii, task_queue)
                    state_store.save(state)

                state["stats"]["conditions_done"] += 1
                set_cursor_state(state, si, ii + 1, 1)
                set_split_context(state, si, ii, None)
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
        try:
            driver.quit()
        except Exception:
            pass


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
    p.add_argument("--sales-range", choices=sorted(SALES_RANGE_CHOICES.keys()), default="")
    p.add_argument("--employee-from", choices=EMPLOYEE_RANGE_VALUES, default=None)
    p.add_argument("--employee-to", choices=EMPLOYEE_RANGE_VALUES, default=None)
    p.add_argument("--location", default=None)
    p.add_argument("--split-threshold", type=int, default=1000)
    p.add_argument("--timeout-sec", type=int, default=20)
    p.add_argument("--short-timeout-sec", type=int, default=3)
    p.add_argument("--retries", type=int, default=3)
    p.add_argument("--cursor-sales-index", type=int, default=None)
    p.add_argument("--cursor-industry-index", type=int, default=None)
    p.add_argument("--cursor-page", type=int, default=None)
    p.add_argument("--verbose", action="store_true")
    return p


if __name__ == "__main__":
    parser = build_parser()
    args = parser.parse_args()
    raise SystemExit(run(args))
