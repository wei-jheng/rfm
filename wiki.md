# rfm Wiki

> RFM analytics transformation pipeline (silver_standard + gold_mart) for Databricks

## 專案概述

從 POS（BDA）與 OPEI 兩個 silver_source 來源整合交易資料，產出 RFM 分析所需的 silver_standard 事實表與 gold_mart 分析資料集。整個 pipeline 由 job `ing_set_rfm` 編排，在上游三張來源表全數更新後自動觸發。

---

## 架構說明

### 資料流

```
silver_source
  ├── {catalog_ss}.{schema_bda}.pos_master_member        (BDA POS)
  ├── {catalog_ss}.{schema_opei}.opei_frontend_invoice_list  (OPEI)
  └── {catalog_ss}.{schema_opei}.opei_iuo_invoice_detail     (OPEI IUO)
         │
         ▼  pln_rfm_ingestion (PPL003a)
         │  ・union POS + OPEI
         │  ・merchant 屬性解析（TOML regex rules）
         │  ・IUO override affiliation_type
         │  ・data quality: valid_amount > 0, len(gid) in {17, 32}
         │  ・CDC SCD Type 1（keys: gid, transaction_id, transaction_date）
         ▼
silver_standard
  └── {catalog_ss_std}.{schema_rfm}.fact_txn_unified  (streaming CDC table)
         │
         ▼  pln_rfm_gold (PPL003b)
         ├── fact_mthly_gid_mcht_rfm        (月度 GID × 通路 RFM 事實表，MV)
         │     └── fact_mthly_gid_affi_rfm  (彙整至集團歸屬層，MV)
         ├── mart_mthly_mcht_sales          (通路月度銷售彙總，MV)
         ├── mart_mthly_affi_sales          (集團歸屬月度銷售彙總，MV)
         ├── mart_mthly_mcht_recency/frequency/monetary  (通路月度 RFM 統計，各1 MV)
         ├── mart_mthly_affi_recency/frequency/monetary  (集團歸屬月度 RFM 統計，各1 MV)
         └── mart_mthly_txn_range           (各月交易日期區間，MV)

gold_mart（共 10 張 MV）
```

### 模組結構

```
src/rfm/
├── common/
│   └── spark_conf.py          # get_ingest_dt / get_flow_version / get_today
├── config/
│   └── merchant_mapping.py    # MERCHANT_RULES、load_merchant_rules、get_merchant_name_col
├── transforms/
│   ├── txn_helpers.py         # resolve_affiliation_type、enrich_opei_with_merchant_attrs
│   ├── rfm_calc.py            # compute_mcht_rfm、compute_affi_rfm
│   ├── sales.py               # compute_sales、add_avg_metrics
│   ├── stats.py               # compute_stats
│   └── txn_range.py           # compute_txn_range
└── _version.py                # 自動生成（hatch-vcs）

workflow/pln_rfm/transformations/  ← DLT 組裝層，不含商業邏輯
├── fact_txn_unified.py         → pln_rfm_ingestion：union POS + OPEI，CDC flow
└── gold_mart/
    ├── fact_mthly_rfm.py       → fact_mthly_gid_mcht_rfm / fact_mthly_gid_affi_rfm
    ├── mart_mthly_sales.py     → mart_mthly_mcht_sales / mart_mthly_affi_sales
    ├── mart_mthly_stats.py     → 6 張統計 MV（3 metrics × 2 granularities）
    └── mart_mthly_txn_range.py → mart_mthly_txn_range

resources/config/rfm/
└── merchant_mapping.toml       # 通路對應規則（pattern → name/affiliation/category/channel）
```

---

## 輸出資料表

### silver_standard

| 資料表 | 類型 | 說明 |
|--------|------|------|
| `fact_txn_unified` | Streaming CDC (SCD1) | 整合 POS + OPEI 的交易事實表 |

**fact_txn_unified schema：**

| 欄位 | 說明 |
|------|------|
| `gid` | 會員 ID（17 或 32 碼） |
| `affiliation_type` | 集團內 / 集團外 |
| `merchant_category` | 通路分類（超商、藥妝、咖啡廳、百貨_雙北、百貨_高雄） |
| `merchant_name` | 標準通路名稱 |
| `sales_channel` | 線下 |
| `place_name` | 消費地點原始名稱 |
| `transaction_date` | 交易日期 |
| `transaction_id` | 交易編號（CDC PK） |
| `transaction_amount` | 交易金額 |
| `_source_file` | 來源檔案路徑 |
| `_ingest_dt` | 攝取時間戳 |

CDC keys: `(gid, transaction_id, transaction_date)`；sequence by: `_ingest_dt`

### gold_mart

| 資料表 | Granularity | 說明 |
|--------|-------------|------|
| `fact_mthly_gid_mcht_rfm` | GID × 月 × 通路 | 個人通路層 RFM（recency/frequency/monetary） |
| `fact_mthly_gid_affi_rfm` | GID × 月 × 集團歸屬 | 個人集團歸屬層 RFM |
| `mart_mthly_mcht_sales` | 月 × 通路 | 通路月度銷售彙總（unique_member_count, total_txn_count, total/avg amounts） |
| `mart_mthly_affi_sales` | 月 × 集團歸屬 | 集團歸屬月度銷售彙總 |
| `mart_mthly_mcht_recency` | 月 × 通路 | 通路月度 recency 統計（avg/min/max/median） |
| `mart_mthly_mcht_frequency` | 月 × 通路 | 通路月度 frequency 統計 |
| `mart_mthly_mcht_monetary` | 月 × 通路 | 通路月度 monetary 統計 |
| `mart_mthly_affi_recency` | 月 × 集團歸屬 | 集團歸屬月度 recency 統計 |
| `mart_mthly_affi_frequency` | 月 × 集團歸屬 | 集團歸屬月度 frequency 統計 |
| `mart_mthly_affi_monetary` | 月 × 集團歸屬 | 集團歸屬月度 monetary 統計 |
| `mart_mthly_txn_range` | 月 | 各月交易日期區間（start_date / end_date） |

---

## 通路對應規則（merchant_mapping.toml）

`resources/config/rfm/merchant_mapping.toml` 定義 `place_name` → 通路屬性的 regex 對應規則，每個 `[[rules]]` 區塊包含：

| 欄位 | 說明 |
|------|------|
| `pattern` | `place_name` 比對 regex（先 match 先贏，順序重要） |
| `name` | 標準通路名稱（`merchant_name`） |
| `affiliation_type` | `集團內` / `集團外` |
| `merchant_category` | 通路分類 |
| `sales_channel` | `線下` |

目前已定義通路：

| 通路名稱 | 集團歸屬 | merchant_category |
|----------|----------|-------------------|
| 7-ELEVEN | 集團內 | 超商 |
| FamilyMart | 集團外 | 超商 |
| Hi-Life | 集團外 | 超商 |
| OK-Mart | 集團外 | 超商 |
| 美廉社 | 集團外 | 超商 |
| 康是美 | 集團內 | 藥妝 |
| 屈臣氏 | 集團外 | 藥妝 |
| 寶雅 | 集團外 | 藥妝 |
| 星巴克 | 集團內 | 咖啡廳 |
| 路易莎 | 集團外 | 咖啡廳 |
| 85度C | 集團外 | 咖啡廳 |
| Dreamers Coffee | 集團外 | 咖啡廳 |
| Cama | 集團外 | 咖啡廳 |
| 統一時代台北 | 集團內 | 百貨_雙北 |
| 夢時代 | 集團內 | 百貨_高雄 |
| 新光三越台北 | 集團外 | 百貨_雙北 |
| 新光三越三多 | 集團外 | 百貨_高雄 |
| 新光三越左營 | 集團外 | 百貨_高雄 |
| 遠東百貨雙北 | 集團外 | 百貨_雙北 |
| 遠東百貨高雄 | 集團外 | 百貨_高雄 |
| SOGO台北 | 集團外 | 百貨_雙北 |
| SOGO高雄 | 集團外 | 百貨_高雄 |
| 微風廣場 | 集團外 | 百貨_雙北 |
| 漢神巨蛋 | 集團外 | 百貨_高雄 |
| 漢神百貨 | 集團外 | 百貨_高雄 |
| 義大世界 | 集團外 | 百貨_高雄 |

未符合任何規則的 `place_name` → `merchant_name = '其他'`。

### 新增通路規則（不需 rebuild wheel）

TOML 是 DAB bundle 的 workspace files，**不打包進 wheel**。只需將新版 TOML 直接上傳至 Databricks workspace，下次 Pipeline 執行時即自動生效，無需重新 build wheel 或 `bundle deploy`。

```bash
databricks workspace import --overwrite \
  rfm/resources/config/rfm/merchant_mapping.toml \
  /Shared/bundles/rfm/<target>/files/resources/config/rfm/merchant_mapping.toml
```

> `<target>` 替換為 `dev`、`uat` 或 `prod`。sandbox 路徑為 `~/.bundle/rfm/files/...`（個人路徑）。

---

## DAB 變數與部署

### Bundle Variables

| Variable | 預設值 | 說明 |
|----------|--------|------|
| `whl_version` | `"0.1.0"` | Wheel 版本，須與 git tag 一致。CI 自動設定；手動部署須用 `--var=whl_version=<tag>` |
| `today` | `""` | 目標執行日期（`yyyy-MM-dd`）。空值 = Asia/Taipei 當下日期 |
| `catalog_silver_source` | `"sandbox"` | silver_source layer 的 Unity Catalog 名稱 |
| `catalog_silver_standard` | `"sandbox"` | silver_standard layer 的 Unity Catalog 名稱 |
| `catalog_gold_mart` | `"sandbox"` | gold_mart layer 的 Unity Catalog 名稱 |
| `schema_bda` | `"set_bda"` | BDA POS silver_source schema |
| `schema_opei` | `"set_opei"` | OPEI silver_source schema |
| `schema_rfm` | `"set_wei_jheng"` | RFM pipeline 輸出 schema |
| `slack_notify_destination_id` | `""` | Slack #data-team-notify Destination ID（job start/success） |
| `slack_alert_destination_id` | `""` | Slack #data-team-alert Destination ID（job failure） |
| `task_retry_on_failure` | `0` | 各 task 失敗最大重試次數 |
| `flow_version_fact_txn_unified` | `"0"` | fact_txn_unified CDC flow 版本。遞增可強制重新處理全量資料 |
| `trigger_wait_after_last_change_seconds` | `61` | 來源表最後更新後等待秒數才觸發 job（> 60；prod/uat 建議 600） |

### Targets

| Target | Catalog（SS/SS_Std/GM） | schema_rfm | 說明 |
|--------|------------------------|------------|------|
| `sandbox`（預設） | sandbox / sandbox / sandbox | set_wei_jheng | 個人開發環境 |
| `dev` | silver_source / silver_standard / gold_mart | rfm | 開發環境；有 Slack 通知 |
| `uat` | silver_source / silver_standard / gold_mart | rfm | UAT；trigger wait = 600s；有 Slack 通知 |
| `prod` | silver_source / silver_standard / gold_mart | rfm | 正式環境；trigger wait = 600s；有 Slack 通知 |

### 部署指令

```bash
# 部署到 sandbox（預設）
databricks bundle deploy

# 部署到指定環境
databricks bundle deploy -t dev
databricks bundle deploy -t uat
databricks bundle deploy -t prod

# 手動指定 wheel 版本
databricks bundle deploy -t dev --var=whl_version=0.2.0

# 手動執行 job
databricks bundle run ing_set_rfm -t dev

# 單獨執行 pipeline
databricks bundle run pln_rfm_ingestion -t dev
databricks bundle run pln_rfm_gold -t dev
```

---

## 重新處理全量資料

遞增 `flow_version_fact_txn_unified` 會建立一個新的 CDC flow，讓 pipeline 重新從來源讀取所有資料寫入 `fact_txn_unified`。**但不會自動清空既有資料**，直接執行會產生重複記錄。

`fact_txn_unified` 是 DLT 管理的 streaming CDC table，TRUNCATE 只移除資料列但保留 Delta 元數據與 streaming state，DLT 執行結果不可預期。**必須先 DROP TABLE**，讓 DLT 下次執行時完全從頭重建：

```sql
DROP TABLE {catalog_silver_standard}.{schema_rfm}.fact_txn_unified;
```

步驟：
1. 手動 DROP `fact_txn_unified`
2. 在 `databricks.yml` 將 `flow_version_fact_txn_unified` 的 `default` 值遞增（例如 `"0"` → `"1"`）
3. 重新部署：`databricks bundle deploy -t <target>`
4. 觸發 pipeline 執行

### Schema 異動的 Full Refresh

若 `fact_txn_unified` 的 schema 有異動（新增/移除欄位），建議使用 DLT 的 **Full Refresh**，它會自動處理 drop + recreate 並確保 streaming state 完全重置，不需手動 DROP TABLE。

在 Databricks UI 中對 `pln_rfm_ingestion` pipeline 選擇「Full Refresh」，或參考 [Pipeline Refresh Semantics](https://docs.databricks.com/aws/en/ldp/updates#pipeline-refresh-semantics) 了解各種 refresh 模式的差異。

---

## 安裝與開發

```bash
# 安裝開發依賴
uv sync

# 執行測試
uv run pytest

# Lint 檢查
uv run ruff check src/ tests/

# 格式化
uv run ruff format src/ tests/

# 型別檢查
uv run mypy src/

# 建置 wheel
uv build --wheel
```

## Troubleshooting

### Wheel 版本不符

`whl_version` 必須與實際 git tag 一致。CI 會自動設定，手動部署時需明確指定：

```bash
databricks bundle deploy -t dev --var=whl_version=0.2.0
```

### trigger_wait_after_last_change_seconds 設定錯誤

此值必須 > 60 秒。sandbox/dev 使用 61，uat/prod 使用 600 以避免來源表仍在寫入時就觸發 pipeline。

### IUO affiliation_type 未正確覆寫

`opei_iuo_invoice_detail` 中 `seq_number = '001'` 的 `invoice_number` 會被視為集團內交易，強制將 `affiliation_type` 覆蓋為 `'集團內'`。若覆寫結果異常，確認 `opei_iuo_invoice_detail` 的 `_ingest_dt` 是否與 `opei_frontend_invoice_list` 同批次。
