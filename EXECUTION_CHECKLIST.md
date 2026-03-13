# Sansanスクレイパー 実行前チェックリスト

このチェックリストは、`sansan_scraper.py` を実行する前に「つまずきやすいポイント」を先に潰すためのものです。

## 1) まず最初に確認（GitHub / PR）
- [ ] この変更は **main ではなく作業ブランチ** にある
- [ ] PR（Pull Request）が作成されている
- [ ] PRがマージ済み、またはローカルで最新ブランチを取得済み

> 補足: 「Codexの右上の PRを作成する」ボタンを押すのは正しい操作です。  
> ただし、押しただけではGitHub本体に反映されず、**PRをマージ**して初めてmainに反映されます。

## 2) ファイル配置
- [ ] `sansan_scraper.py` がある
- [ ] `industries.csv` がある（同じディレクトリ推奨）
- [ ] `industries.csv` のヘッダーが `大分類,中分類,小分類`

確認コマンド例:
```bash
python sansan_scraper.py --help
```

## 3) Python環境
- [ ] Python 3.10+ が使える
- [ ] `selenium` がインストール済み
- [ ] Chrome がインストール済み
- [ ] ChromeDriver が実行可能（Selenium Managerで解決できる環境なら不要）

最小インストール例:
```bash
pip install selenium
```

## 4) Sansanログイン前提
- [ ] ログインID/パスワードが手元にある
- [ ] CAPTCHA / 2段階認証は無効（今回の前提）
- [ ] 会社PCのセキュリティ製品でブラウザ自動操作がブロックされない

## 5) 初回は小さくテスト（推奨）
- [ ] まず `--max-conditions 1` で動作確認
- [ ] CSVが出力されることを確認
- [ ] `state/state.json` と `state/dedupe.db` が作られることを確認

実行例:
```bash
python sansan_scraper.py --industries-csv industries.csv --max-conditions 1
```

## 6) 本番実行
- [ ] ログを確認しながら通常実行
- [ ] 長時間実行時はPCスリープを無効化

実行例:
```bash
python sansan_scraper.py --industries-csv industries.csv
```

## 7) 中断・再開
- [ ] 中断後は `--resume` で再開
- [ ] 再開後、同じデータが重複して増えていないことを確認

再開例:
```bash
python sansan_scraper.py --industries-csv industries.csv --resume
```

## 8) トラブル時の確認ポイント
- [ ] `logs/run_YYYYMMDD.log` を確認
- [ ] `state/state.json` の `status` / `cursor` を確認
- [ ] `industries.csv` に空行や文字化けがないか確認
- [ ] Sansan画面のDOM変更でセレクタが崩れていないか確認

## 9) 実行後チェック
- [ ] CSV件数が期待値に近い
- [ ] 明らかな重複（企業名+住所）が増えていない
- [ ] 次回再開のため `state/` を消していない

---

## 10) 最短運用コマンド（コピペ用）

### 初回テスト
```bash
python sansan_scraper.py --industries-csv industries.csv --max-conditions 1 --verbose
```

### 本番
```bash
python sansan_scraper.py --industries-csv industries.csv --verbose
```

### 再開
```bash
python sansan_scraper.py --industries-csv industries.csv --resume --verbose
```
