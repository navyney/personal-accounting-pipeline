# Midterm Report

**Student Name:** Boontharika Korkitrotjana 
**Student ID:** 660610769
**Date:** 18 Mar 2026

---

## Part 1: Data Exploration Answers

> How many total transactions are there?

_There are 72,586,000 total transactions in the dataset._

> How many unique family members, merchants, and categories?

_There are 4 unique family members, 48 unique merchants, and 18 unique spending categories._

> How many rows have null or empty `amount` values?

_1,450,421 rows contain null or empty values in the amount column._

> What is the date range of the transactions?

_The transaction dates range from 2010-01-01 to 2025-12-31_

---

## Part 4: Join Analysis

> What join type did you use for enriching transactions, and why?

_Left join because we want to keep all transactions even if some of merchant ids do not exist in the merchants lookup table._

> How many transactions have no matching merchant in the merchants table?

_Transactions with missing merchant:  212089_

> What would happen if you used an inner join instead?

_If an inner join were used instead of a left join, transactions that do not have matching merchant records would be removed from the dataset. This would lead to data loss and inaccurate spending analysis because those transactions would no longer be counted in the analytics layer. Since the transactions table is the source of truth for spending activity, we want to preserve all records even if lookup information is missing. Therefore, a left join is the safest option for maintaining complete transaction data._

---

## Part 5: Analytics Insights

> Look at the average transaction amount per year table. Do you notice a trend? Calculate the approximate year-over-year percentage change. What might explain this?

_Your answer here. Show your calculation:_

| Year | Avg Amount | YoY Change (%) |
|------|-----------|-----------------|
| 2016 | 55.982521244951755| - |
| 2017 | 57.11752221721342| 2.0274202501446177|
| 2018 | 58.33021620698671| 2.1231558070070213|
| 2019 | 59.46755453944663| 1.9498270474843364|
| 2020 | 60.58627702195049| 1.8812316920847536|
| 2021 |  61.8715038857499| 2.1213167848781604|
| 2022 | 63.08090033879223| 1.9546905717300271|
| 2023 |   64.412011563428| 2.1101652282809638|
| 2024 | 65.54140422011058| 1.7533882722641632|
| 2025 | 66.94052935095615| 2.1347194914329766|

_The average transaction amount shows a steady upward trend from 2016 to 2025. The year-over-year growth rate is approximately around 2% per year. This gradual increase is likely due to inflation and rising costs of goods and services, which causes the average transaction value to increase over time._

> Which category has the highest total spending? Which has grown the fastest over 10 years?

_The category with the highest total spending is Groceries, with approximately 818 million in total spending._
_From the yearly category spending table, Electronics and Education appear to show strong growth over time, likely due to increased spending on technology and educational services._

> Compare spending between family members. Who spends the most? On what?

_The family member who spends the most is MEM01, with approximately 1.96 billion in total spending. The largest spending categories for this member include Groceries, Education, Electronics, and Clothing, which represent major household and lifestyle expenses._

> What percentage of transactions fall in each spending tier? Has this distribution changed over the years?

|spending_tier|   count|        percentage|
|-------------|--------|------------------|
|        small|35059036| 49.53261597697125|
|       medium|22158435| 31.30620167381895|
|        micro| 9910619|14.002064546813974|
|        large| 3651608| 5.159117802395823|

_The distribution of spending tiers remains relatively stable across the years. Small transactions consistently represent the largest portion of purchases, followed by medium transactions. Large transactions remain a small percentage each year, indicating that most family spending consists of frequent low-value purchases._

---

## Section A: Data Architecture Questions

_The family has some questions about how the system works._

### Q1. Merchant Name Change

> "We just found out one of the merchants changed their name last year. Where in the pipeline do we update this, and what layers need to be reprocessed?"

_Your answer (3–5 sentences):_

---

### Q2. New Family Member

> "Our daughter started college and has her own credit card now. How do we add a new family member to the system without breaking existing data?"

_Your answer (3–5 sentences):_

---

### Q3. Average Monthly Grocery Spending

> "We want to know our average monthly grocery spending. Walk us through exactly which transformations and joins produce this number."

_Your answer (3–5 sentences):_

---

### Q4. Duplicate Transactions

> "Last month's bank export had 500 duplicate transactions. How does your pipeline handle this? If it doesn't yet, what would you add?"

_Your answer (3–5 sentences):_

---

### Q5. Data Backup & Recovery

> "We're worried about losing our data. What's your backup strategy? What's the most data we could lose if something crashes?"

_Hint: Think about RPO (Recovery Point Objective) and RTO (Recovery Time Objective)._

_Your answer (3–5 sentences):_

---

## Section B: Engineering Questions

_The family's developer friend has some technical questions._

### Q6. CI/CD Pipeline

> "If we set up CI/CD for this project, what would the pipeline look like? What gets tested automatically, and what triggers the tests?"

_Your answer (3–5 sentences):_

---

### Q7. Monthly Automation with Orchestration

> "We want this pipeline to run automatically every month when the bank exports new transactions. How would you set this up? Draw the DAG."

_Your answer (3–5 sentences):_

_Draw your DAG below (text-based diagram):_

```
Example format:
┌──────────┐     ┌──────────┐     ┌──────────┐
│  Task 1  │ ──▶ │  Task 2  │ ──▶ │  Task 3  │
└──────────┘     └──────────┘     └──────────┘

Your DAG:
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│  Load Raw    │ ──▶ │ Data Cleaning│ ──▶ │ Data Enrich  │
│ Transactions │     │  & Staging   │     │ (Join tables)│
└──────────────┘     └──────────────┘     └───────┬──────┘
                                                  │
                                                  ▼
                                          ┌──────────────┐
                                          │ Aggregations │
                                          │  Analytics   │
                                          └───────┬──────┘
                                                  │
                                                  ▼
                                          ┌──────────────┐
                                          │ Generate     │
                                          │ Reports      │
                                          └──────────────┘
```

---

## Section C: Analytics Insights

_The family wants your professional opinion._

### Q8. Price Trend Analysis

> "We looked at your yearly average transaction table and prices seem to go up. Can you calculate the exact rate? Is it consistent across all categories?"

_Show your work:_

_Your answer (3–5 sentences):_

---

### Q9. Spending Recommendations

> "Based on your summary tables, give us 3 actionable recommendations for how we can reduce spending next year."

1. _Recommendation 1:_
2. _Recommendation 2:_
3. _Recommendation 3:_

---

### Q10. Needs vs Wants

> "Which spending categories are 'needs' vs 'wants'? What percentage of our total spending goes to each?"

_Your answer (3–5 sentences):_

| Budget Type | Total Spending | Percentage |
|-------------|---------------|------------|
| Needs | | |
| Wants | | |
| Savings | | |
