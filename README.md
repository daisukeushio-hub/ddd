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
