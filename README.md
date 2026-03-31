# rfm

> RFM analytics transformation pipeline (silver_standard + gold_mart) for Databricks

## 簡介

從 POS 與 OPEI 的 silver_source 資料，經過兩階段 DLT pipeline 產出 RFM 分析所需的資料集：

1. **pln_rfm_ingestion**（PPL003a）：整合 BDA POS 與 OPEI 交易資料，產出 `fact_txn_unified`（silver_standard）
2. **pln_rfm_gold**（PPL003b）：計算月度 RFM 指標，產出四張 gold_mart 資料表

Job `ing_set_rfm`（J003）在 POS/OPEI 來源表全數更新後自動觸發，依序執行上述兩個 pipeline。

詳細架構、資料表 schema、DAB variables 說明見 [wiki.md](wiki.md)。

## 安裝

```bash
uv sync
```

## 部署

```bash
# 部署到 sandbox（預設）
databricks bundle deploy

# 部署到 dev / uat / prod
databricks bundle deploy -t dev
databricks bundle deploy -t uat
databricks bundle deploy -t prod
```

## 開發

```bash
uv run pytest            # 執行測試
uv run ruff check src/   # Lint
uv run mypy src/         # 型別檢查
uv build --wheel         # 建置 wheel
```

## 相關文件

詳細架構、資料表設定、DAB variables、新增資料表步驟見 [wiki.md](wiki.md)。
