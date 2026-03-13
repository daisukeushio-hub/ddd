# Sansan企業データ収集ツール 設計書（状態管理・リトライ・CSV/SQLite）& 実装タスク分解（v1）

## 1. 本書の目的
要件定義（`requirements_sansan_scraper.md`）を実装可能なレベルまで落とし込み、以下を具体化する。
- 状態管理（ページ単位再開）
- リトライ設計（止まりにくさ）
- CSV/SQLite方針（重複排除と運用安定）
- 実装タスク分解（着手順・完了条件）

---

## 2. 全体アーキテクチャ（推奨）

### 2.1 コンポーネント
1. `runner.py`（エントリポイント）
   - CLI引数処理
   - 初期化（ログ、状態、DB、出力先）
   - ジョブ実行制御
2. `auth.py`
   - ログイン処理
   - 任意で資格情報保存（Keyring）
3. `search_executor.py`
   - 条件設定（売上レンジ・業種）
   - 検索実行
4. `page_scraper.py`
   - 1ページ分の行抽出
   - 1件レコード整形（欠損許容）
5. `dedupe_store.py`
   - 重複キー生成
   - 既存確認 + 登録（SQLite）
6. `state_store.py`
   - 状態保存/読込（JSON）
   - 原子的保存（tmp→rename）
7. `csv_writer.py`
   - CSV追記
   - ヘッダ制御
8. `retry.py`
   - 共通リトライ（指数バックオフ + ジッタ）

### 2.2 処理フロー
1. 起動 (`--resume` なら状態復元)
2. ログイン
3. 条件ループ（売上×業種）
4. ページループ（1,2,3...）
5. ページから行抽出
6. 各行で重複判定（企業名+住所）
7. 新規のみCSV追記 + SQLite登録
8. ページ完了時に状態保存
9. 次ページがなければ条件完了マーク
10. 全条件完了で状態を `completed` に更新

---

## 3. 状態管理設計（ページ単位再開）

## 3.1 状態ファイル配置
- `state/state.json`
- `state/state.json.bak`（直前バックアップ）

## 3.2 状態スキーマ（案）
```json
{
  "version": 1,
  "job_id": "2026-03-13T03-00-00",
  "status": "running",
  "cursor": {
    "sales_index": 0,
    "industry_index": 12,
    "page": 5
  },
  "current_condition": {
    "sales_from": "10億",
    "sales_to": "30億",
    "major": "製造業",
    "middle": "食品",
    "minor": "調味料"
  },
  "stats": {
    "rows_seen": 12500,
    "rows_written": 9210,
    "rows_duplicated": 2890,
    "conditions_done": 37,
    "errors": 14
  },
  "updated_at": "2026-03-13T07:12:10+09:00",
  "last_error": null
}
```

## 3.3 保存タイミング
- 必須: **1ページ処理完了ごと**
- 追加: 例外発生時、条件完了時、終了時

## 3.4 原子的保存
1. `state.json.tmp` に書く
2. `flush + fsync`
3. 既存 `state.json` を `state.json.bak` に退避
4. `os.replace(tmp, state.json)`

## 3.5 再開アルゴリズム
- `--resume` のとき `state.json` を読む
- 破損時は `state.json.bak` を試す
- 復元後、`cursor` の条件・ページから再開
- すでにCSV/SQLiteへ保存済みの重複はSQLiteで弾くため安全

---

## 4. リトライ設計

## 4.1 リトライ対象
- 検索条件入力時の要素取得失敗
- 検索ボタンクリック後の結果表示待機失敗
- 次ページ遷移失敗
- 行抽出時の `StaleElementReferenceException` など一時エラー

## 4.2 リトライしない対象
- 認証エラー（ID/パスワード誤り）
- セレクタが完全に変わった継続失敗（同一箇所で閾値超え）

## 4.3 ポリシー
- `max_retries = 3`
- 待機: `base=1.5s`, `factor=2.0`, `max=20s`, `jitter=±20%`
- 失敗時: 
  - ページ単位失敗 → そのページを最大3回再試行
  - 条件単位で閾値超え → 条件スキップして次へ

## 4.4 サーキットブレーカ（簡易）
- 連続失敗条件数が閾値（例: 10）を超えたら一時停止（例: 300秒）して再試行
- 回復しなければジョブ停止（状態保存して終了）

## 4.5 待機戦略
- 固定 `sleep` は最小化
- 基本は `WebDriverWait` + `EC`
- DOM更新確認には `staleness_of` を優先

---

## 5. CSV / SQLite方針

## 5.1 結論（推奨）
- **CSVを成果物の正本**とし、**SQLiteを重複管理と進捗補助に使う**ハイブリッド構成。

## 5.2 理由
- CSVはユーザー利用しやすい（Excel等）
- SQLiteは高速な存在確認・再実行時の冪等性に強い
- 10,000件規模ならSQLite運用コストは低い

## 5.3 SQLiteスキーマ（案）
```sql
CREATE TABLE IF NOT EXISTS seen_companies (
  dedupe_key TEXT PRIMARY KEY,
  first_seen_at TEXT NOT NULL,
  company_name TEXT,
  address TEXT,
  source_condition_id TEXT,
  source_page INTEGER
);

CREATE TABLE IF NOT EXISTS run_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts TEXT NOT NULL,
  level TEXT NOT NULL,
  event_type TEXT NOT NULL,
  message TEXT,
  context_json TEXT
);
```

## 5.4 重複キー仕様
- `dedupe_key = normalize(企業名) + "||" + normalize(住所)`
- `normalize`:
  - 前後空白除去
  - 連続空白の1個化
  - 全角/半角の標準化（可能ならNFKC）
  - 改行・タブ除去

## 5.5 CSV書き込み仕様
- `output/sansan_companies_YYYYMMDD.csv`
- `utf-8-sig`
- ページ単位でバッファ→一括追記
- 追記成功後にSQLite登録（または同一トランザクション管理）

## 5.6 障害時整合性
- 推奨順序: 
  1) SQLiteに仮登録（トランザクション開始）
  2) CSV追記
  3) SQLiteコミット
- 実装簡易版: CSV成功後SQLite登録でも可（重複が増えにくい）

---

## 6. 設定値（config.yaml 例）
```yaml
runtime:
  headless: false
  resume: true
  max_conditions: null

wait:
  default_timeout_sec: 20
  short_timeout_sec: 3
  page_load_timeout_sec: 30

retry:
  max_retries: 3
  backoff_base_sec: 1.5
  backoff_factor: 2.0
  backoff_max_sec: 20
  jitter_ratio: 0.2

paths:
  output_dir: output
  log_dir: logs
  state_file: state/state.json
  sqlite_file: state/dedupe.db
```

---

## 7. ログ/監視設計

## 7.1 ログ方針
- 人間可読ログ + JSONライクな構造情報
- 例: `INFO condition_start ...`, `WARN page_retry ...`, `ERROR condition_skipped ...`

## 7.2 最低限のメトリクス
- `rows_written_total`
- `rows_duplicate_total`
- `conditions_completed`
- `page_retry_total`
- `error_total`
- `estimated_remaining_time`

---

## 8. セキュリティ設計（認証情報）

## 8.1 基本方針
- 既定は都度入力（安全側）
- 任意で `--save-credentials` を許可

## 8.2 保存方式
- 平文ファイル禁止
- 可能なら `keyring` ライブラリでOSキーチェーン使用
- 保存時は明示確認（初回のみ）

---

## 9. 実装タスク分解（WBS）

## フェーズ0: 土台整備
1. ディレクトリ構成作成（`src/`, `state/`, `logs/`, `output/`）
2. 設定ファイル読み込み基盤（`config.yaml`）
3. ロガー初期化（コンソール+ファイル）

**完了条件**: 実行すると設定が読み込まれ、ログが出る。

## フェーズ1: 状態管理
1. `state_store.py` 作成
2. 状態スキーマ定義（version付き）
3. 原子的保存（tmp→replace）
4. `--resume` で再開

**完了条件**: 手動停止後、同一ページから再開できる。

## フェーズ2: 重複排除
1. `dedupe_store.py`（SQLite接続、テーブル作成）
2. `dedupe_key` 正規化関数
3. 存在確認 + 登録
4. 既存CSV取り込み（初回起動時オプション）

**完了条件**: 再実行しても重複行が増えない。

## フェーズ3: スクレイピング本体の分割
1. 認証処理を `auth.py` に分離
2. 検索条件設定を `search_executor.py` に分離
3. ページ抽出を `page_scraper.py` に分離
4. CSV書き込みを `csv_writer.py` に分離

**完了条件**: 各モジュール単体で呼び出し可能。

## フェーズ4: リトライ実装
1. 共通 `retry.py` を導入
2. ページ遷移・取得処理に適用
3. 条件スキップ判定と連続失敗制御

**完了条件**: 一時的な失敗で停止せず継続する。

## フェーズ5: 運用オプション
1. `argparse` でCLI追加（`--resume`, `--headless`, `--max-conditions`, `--save-credentials`）
2. 実行サマリー出力（件数・時間・失敗数）

**完了条件**: 実運用に必要な引数で制御できる。

## フェーズ6: 検証
1. ドライラン（1条件）
2. 中断再開テスト（ページ途中で停止→再開）
3. 重複テスト（同条件再実行）
4. 長時間テスト（3時間以上）

**完了条件**: 受け入れ基準を満たす。

---

## 10. 実装優先順位（最短で使える順）
1. 状態管理（再開）
2. 重複排除（SQLite）
3. リトライ（止まりにくさ）
4. モジュール分割（保守性）
5. 認証情報保存（任意）

---

## 11. 直近スプリント提案（3日）
- Day1: フェーズ0〜1
- Day2: フェーズ2〜3
- Day3: フェーズ4〜6（ドライラン + 中断再開確認）

成果物:
- 実行可能スクリプト
- 状態/重複管理付き
- 最低限の運用ドキュメント

