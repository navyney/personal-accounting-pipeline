# Midterm Project: Personal Accounting Pipeline

## Overview

You are a data engineer hired by a family of four to build a pipeline that analyzes their household spending history. They've exported 10 years of bank transaction data (2016–2025) and want to understand their spending patterns, find trends, and get answers to specific questions about their finances.

You will receive three CSV files:

| File | Description | Size |
|------|-------------|------|
| `transactions.csv` | Every purchase made by the family | Large (millions of rows) |
| `categories.csv` | Spending category lookup table | Small |
| `merchants.csv` | Merchant/store lookup table | Small |

### Transaction Columns

| Column | Type | Description |
|--------|------|-------------|
| `transaction_id` | String | Unique ID (e.g., `TXN0000000001`) |
| `date` | String | Date of purchase (`YYYY-MM-DD`) |
| `member_id` | String | Family member who made the purchase |
| `item_name` | String | What was bought |
| `category_id` | String | FK to categories table |
| `merchant_id` | String | FK to merchants table |
| `amount` | String | Amount spent (may be empty or negative) |
| `payment_method` | String | How they paid |

> ⚠️ The data is messy — just like real-world data. Expect nulls, invalid values, and inconsistencies. Part of your job is to handle these.

---

## Deliverables

Submit a GitHub repository containing your code and a completed `REPORT_TEMPLATE.md`.

---

## Part 1 — Setup & Data Exploration (10 points)

**Skills tested:** Ch 1 (Spark concepts), Ch 4a (Reading data, schemas)

1. Place the provided CSV files in the `data/` directory.
2. Define explicit schemas for all three tables in `src/etl/schemas.py` using `StructType` and `StructField`. Do NOT use `inferSchema=True`.
3. Read all three CSVs into Spark DataFrames using your schemas.
4. Explore the data and answer these questions in your report:
   - How many total transactions are there?
   - How many unique family members, merchants, and categories?
   - How many rows have null or empty `amount` values?
   - What is the date range of the transactions?

---

## Part 2 — Transformations & Validation with TDD (25 points)

**Skills tested:** Ch 3a/3b (Python, OOP), Ch 4b (Transforms), Ch 5 (TDD)

Use the TDD workflow (Red → Green → Refactor). Write your test FIRST, watch it fail, then implement the function.

### Required Functions

Implement in `src/etl/validations.py`:

- **`filter_valid_transactions(df) -> DataFrame`**
  - Remove rows where `amount` is null or empty
  - Remove rows where `date` is outside the range 2016-01-01 to 2025-12-31
  - Keep negative amounts (these are refunds — valid data)

Implement in `src/etl/transformations.py`:

- **`categorize_spending(df) -> DataFrame`**
  - Add a `spending_tier` column based on `amount`:
    - `"micro"` if amount < 10
    - `"small"` if 10 ≤ amount < 50
    - `"medium"` if 50 ≤ amount < 200
    - `"large"` if amount ≥ 200
  - For negative amounts (refunds), categorize by absolute value

- **`enrich_with_lookups(df, df_categories, df_merchants) -> DataFrame`**
  - Join transactions with categories and merchants tables
  - Use a left join to keep all transactions (some may have merchant_ids not found in the merchants table)
  - The result should include `category_name`, `budget_type`, `merchant_name`, and `merchant_type`

### Testing Requirements

- Write at least **6 test functions** across `tests/test_transformations.py` and `tests/test_validations.py`
- Use `pytest` and `chispa` (`assert_df_equality`)
- Test edge cases: nulls, negative amounts, boundary values, orphan merchant_ids
- A starter test is provided in `test_transformations.py` — it will fail (Red) until you implement `categorize_spending`

---

## Part 3 — Data Lifecycle (20 points)

**Skills tested:** Ch 6 (Data lifecycle, raw/staged/analytics layers)

Implement a pipeline that writes data to three layers:

```
data/                          ← Source CSVs (provided)
output/
├── raw/                       ← Original data converted to Parquet (no changes)
│   ├── transactions/
│   ├── categories/
│   └── merchants/
├── staged/                    ← Cleaned & validated
│   └── transactions/
└── analytics/                 ← Aggregated & enriched
    ├── enriched_transactions/
    ├── monthly_by_category/
    ├── yearly_by_member/
    ├── top_merchants_by_year/
    └── avg_amount_by_year/
```

**Raw layer:** Convert CSVs to Parquet as-is. No transformations. This preserves the original data.

**Staged layer:** Apply `filter_valid_transactions()` and write cleaned data as Parquet.

**Analytics layer:** Enriched and aggregated tables (see Parts 4 and 5).

---

## Part 4 — Joins (15 points)

**Skills tested:** Ch 7 (Joining data)

1. Join your staged transactions with `categories` and `merchants` using `enrich_with_lookups()`.
2. Save the enriched result to `output/analytics/enriched_transactions/` as Parquet.
3. In your report, answer:
   - What join type did you use and why?
   - How many transactions have no matching merchant in the merchants table?
   - What would happen if you used an inner join instead?

---

## Part 5 — Analytics & Insights (15 points)

**Skills tested:** Ch 4b (Transforms, aggregations), Ch 6 (Analytics layer), Ch 7 (Joins)

Using the enriched transactions, produce these summary tables and save each to the analytics layer as Parquet:

1. **Monthly spending by category** — For each year-month, the total amount per `category_name`.
2. **Yearly spending by member** — For each year, the total amount per `member_id`.
3. **Top 10 merchants by total revenue per year** — For each year, the 10 merchants with the highest total spending.
4. **Average transaction amount per year** — For each year, the average `amount` across all transactions.

### Analytics Questions (answer in your report)

- Look at the **average transaction amount per year** table. Do you notice a trend? Calculate the approximate year-over-year percentage change. What might explain this?
- Which category has the **highest total spending**? Which has **grown the fastest** over 10 years?
- Compare spending between family members. Who spends the most? On what?
- What percentage of transactions fall in each **spending tier**? Has this distribution changed over the years?

---

## Part 6 — Written Report (15 points)

**Skills tested:** Ch 8 (CI/CD concepts), Ch 9 (Orchestration concepts)

Complete the `REPORT_TEMPLATE.md` file. It contains questions from the family and their developer friend. Answer each question in 3–5 sentences.

---

## Code Organization & Style (deductions if poor)

- Use type hints on all function signatures
- Include docstrings for all functions
- No dead code or commented-out blocks
- Clean imports (no unused imports)
- Follow the provided project structure

---

## Grading Summary

| Part | Points | Key Skills |
|------|--------|------------|
| 1. Setup & Exploration | 10 | Schemas, DataFrames, Spark basics |
| 2. Transformations & TDD | 25 | TDD, pytest, chispa, transforms |
| 3. Data Lifecycle | 20 | Raw/staged/analytics layers, Parquet |
| 4. Joins | 15 | Left join, orphan handling |
| 5. Analytics & Insights | 15 | Aggregations, trend analysis |
| 6. Written Report | 15 | CI/CD, orchestration, architecture |
| **Total** | **100** | |

---

## Getting Started

```bash
# 1. Clone this repo
git clone <your-repo-url>
cd personal-accounting-pipeline

# 2. Install dependencies
pip install -r requirements.txt

# 3. Place CSV files in data/
cp /path/to/provided/files/*.csv data/

# 4. Run the starter test (should FAIL — this is expected)
pytest tests/ -v

# 5. Start coding! Follow the TDD cycle: Red → Green → Refactor
```

Good luck! 🚀
