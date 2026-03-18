# Sansan Scraper

Sansan の企業検索結果を、売上レンジと業種の組み合わせで巡回して CSV に出力するスクリプトです。

## Files

- `sansan_scraper.py`: メインスクリプト
- `industries.csv`: 検索対象の業種一覧
- `requirements.txt`: Python 依存

## Requirements

- Windows
- Python 3.10+
- Google Chrome
- Selenium 4

## Setup

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## Quick Check

1 条件だけ試す:

```powershell
python sansan_scraper.py --industries-csv industries.csv --output-csv output\check.csv --state-file state\check.json --sqlite-file state\check.db --log-file logs\check.log --max-conditions 1 --verbose
```

## Full Run

```powershell
python sansan_scraper.py --industries-csv industries.csv --output-csv output\rebuild_full.csv --state-file state\rebuild_full.json --sqlite-file state\rebuild_full.db --log-file logs\rebuild_full.log --verbose
```

## Resume

```powershell
python sansan_scraper.py --industries-csv industries.csv --output-csv output\rebuild_full.csv --state-file state\rebuild_full.json --sqlite-file state\rebuild_full.db --log-file logs\rebuild_full.log --resume --verbose
```

## Monitoring

```powershell
Get-Content .\logs\rebuild_full.log -Wait
Get-Content .\state\rebuild_full.json
```

## Notes

- ログイン ID / パスワードは毎回対話入力です
- Chrome を手で操作しないでください
- PC のスリープは無効化してください
- `debug/`, `logs/`, `output/`, `state/` はランタイム生成物なので Git 管理しません
