# Sansan Scraper

Sansan の企業検索結果を、売上レンジと業種の組み合わせで巡回し、CSV に出力するスクリプトです。

## 含まれるファイル

- `sansan_scraper.py`
  - メインスクリプト
- `industries.csv`
  - 検索対象の業種一覧
- `requirements.txt`
  - Python 依存
- `OTHER_PC_SETUP.md`
  - 別PCで実行するための最短手順

## 前提

- Windows
- Python 3.10 以上
- Google Chrome
- ネットワーク接続
- Sansan にログインできるアカウント

## セットアップ

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## 動作確認

1条件だけ試す場合:

```powershell
python sansan_scraper.py --industries-csv industries.csv --output-csv output\check.csv --state-file state\check.json --sqlite-file state\check.db --log-file logs\check.log --max-conditions 1 --verbose
```

## 本番実行

```powershell
python sansan_scraper.py --industries-csv industries.csv --output-csv output\rebuild_full.csv --state-file state\rebuild_full.json --sqlite-file state\rebuild_full.db --log-file logs\rebuild_full.log --verbose
```

売上レンジを分けて実行する場合:

```powershell
python sansan_scraper.py --industries-csv industries.csv --sales-range 5000man-1oku --output-csv output\rebuild_5000man_1oku.csv --state-file state\rebuild_5000man_1oku.json --sqlite-file state\rebuild_5000man_1oku.db --log-file logs\rebuild_5000man_1oku.log --verbose
python sansan_scraper.py --industries-csv industries.csv --sales-range 1oku-3oku --output-csv output\rebuild_1oku_3oku.csv --state-file state\rebuild_1oku_3oku.json --sqlite-file state\rebuild_1oku_3oku.db --log-file logs\rebuild_1oku_3oku.log --verbose
python sansan_scraper.py --industries-csv industries.csv --sales-range 3oku-5oku --output-csv output\rebuild_3oku_5oku.csv --state-file state\rebuild_3oku_5oku.json --sqlite-file state\rebuild_3oku_5oku.db --log-file logs\rebuild_3oku_5oku.log --verbose
python sansan_scraper.py --industries-csv industries.csv --sales-range 5oku-10oku --output-csv output\rebuild_5oku_10oku.csv --state-file state\rebuild_5oku_10oku.json --sqlite-file state\rebuild_5oku_10oku.db --log-file logs\rebuild_5oku_10oku.log --verbose
python sansan_scraper.py --industries-csv industries.csv --sales-range 10-30 --output-csv output\rebuild_10_30.csv --state-file state\rebuild_10_30.json --sqlite-file state\rebuild_10_30.db --log-file logs\rebuild_10_30.log --verbose
python sansan_scraper.py --industries-csv industries.csv --sales-range 30-50 --output-csv output\rebuild_30_50.csv --state-file state\rebuild_30_50.json --sqlite-file state\rebuild_30_50.db --log-file logs\rebuild_30_50.log --verbose
python sansan_scraper.py --industries-csv industries.csv --sales-range 50-100 --output-csv output\rebuild_50_100.csv --state-file state\rebuild_50_100.json --sqlite-file state\rebuild_50_100.db --log-file logs\rebuild_50_100.log --verbose
python sansan_scraper.py --industries-csv industries.csv --sales-range 100-300 --output-csv output\rebuild_100_300.csv --state-file state\rebuild_100_300.json --sqlite-file state\rebuild_100_300.db --log-file logs\rebuild_100_300.log --verbose
python sansan_scraper.py --industries-csv industries.csv --sales-range 300-500 --output-csv output\rebuild_300_500.csv --state-file state\rebuild_300_500.json --sqlite-file state\rebuild_300_500.db --log-file logs\rebuild_300_500.log --verbose
python sansan_scraper.py --industries-csv industries.csv --sales-range 500-1000 --output-csv output\rebuild_500_1000.csv --state-file state\rebuild_500_1000.json --sqlite-file state\rebuild_500_1000.db --log-file logs\rebuild_500_1000.log --verbose
python sansan_scraper.py --industries-csv industries.csv --sales-range 1000-3000 --output-csv output\rebuild_1000_3000.csv --state-file state\rebuild_1000_3000.json --sqlite-file state\rebuild_1000_3000.db --log-file logs\rebuild_1000_3000.log --verbose
python sansan_scraper.py --industries-csv industries.csv --sales-range 3000-5000 --output-csv output\rebuild_3000_5000.csv --state-file state\rebuild_3000_5000.json --sqlite-file state\rebuild_3000_5000.db --log-file logs\rebuild_3000_5000.log --verbose
python sansan_scraper.py --industries-csv industries.csv --sales-range 5000-1cho --output-csv output\rebuild_5000_1cho.csv --state-file state\rebuild_5000_1cho.json --sqlite-file state\rebuild_5000_1cho.db --log-file logs\rebuild_5000_1cho.log --verbose
```

## 再開

```powershell
python sansan_scraper.py --industries-csv industries.csv --output-csv output\rebuild_full.csv --state-file state\rebuild_full.json --sqlite-file state\rebuild_full.db --log-file logs\rebuild_full.log --resume --verbose
```

## 進捗確認

ログを追う:

```powershell
Get-Content .\logs\rebuild_full.log -Wait
```

状態を見る:

```powershell
Get-Content .\state\rebuild_full.json
```

## 注意

- ログインIDとパスワードは毎回対話入力です
- 実行中の Chrome は手で操作しないでください
- 実行中の元 PowerShell は閉じないでください
- PC のスリープは無効化してください
- `debug/`, `logs/`, `output/`, `state/` はランタイム生成物なので Git 管理しません
