# Other PC Setup

別PCで最短で動かす手順です。

## 1. リポジトリを取得

```powershell
git clone -b main https://github.com/daisukeushio-hub/ddd.git
cd ddd
```

`main` にまだ反映されていない場合は `work` を使います。

```powershell
git clone -b work https://github.com/daisukeushio-hub/ddd.git
cd ddd
```

## 2. Python 仮想環境を作成

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## 3. まず1条件だけ確認

```powershell
python sansan_scraper.py --industries-csv industries.csv --output-csv output\check.csv --state-file state\check.json --sqlite-file state\check.db --log-file logs\check.log --max-conditions 1 --verbose
```

## 4. 本番実行

```powershell
python sansan_scraper.py --industries-csv industries.csv --output-csv output\rebuild_full.csv --state-file state\rebuild_full.json --sqlite-file state\rebuild_full.db --log-file logs\rebuild_full.log --verbose
```

## 5. 再開

```powershell
python sansan_scraper.py --industries-csv industries.csv --output-csv output\rebuild_full.csv --state-file state\rebuild_full.json --sqlite-file state\rebuild_full.db --log-file logs\rebuild_full.log --resume --verbose
```

## 6. 進捗確認

```powershell
Get-Content .\logs\rebuild_full.log -Wait
```

```powershell
Get-Content .\state\rebuild_full.json
```

## 7. 運用上の注意

- 実行中の Chrome を触らない
- 元の PowerShell を閉じない
- 別の PowerShell でログ確認する
- PC のスリープを切る
