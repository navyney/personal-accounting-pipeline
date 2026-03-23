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

_1,450,421 rows._

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
_Over the 10-year period, categories such as Electronics and Education show noticeable growth trends, indicating increasing spending on technology and educational services._

> Compare spending between family members. Who spends the most? On what?

_The family member who spends the most is MEM01, with approximately 1.96 billion in total spending. The largest spending categories for this member include Groceries, Education, Electronics, and Clothing, which represent major household and lifestyle expenses._

> What percentage of transactions fall in each spending tier? Has this distribution changed over the years?

_Spending Tier Distribution_
| Spending Tier | Count      | Percentage |
| ------------- | ---------- | ---------- |
| medium        | 22,158,435 | 31.31%     |
| small         | 35,059,036 | 49.53%     |
| micro         | 9,910,619  | 14.00%     |
| large         | 3,651,608  | 5.16%      |

_Spending Tier Distribution by Year (showing only 20 rows)_
| Year | Spending Tier | Count     |
| ---- | ------------- | --------- |
| 2016 | large         | 331,171   |
| 2016 | medium        | 1,936,568 |
| 2016 | small         | 3,682,258 |
| 2016 | micro         | 1,142,412 |
| 2017 | micro         | 1,103,402 |
| 2017 | medium        | 1,994,917 |
| 2017 | large         | 336,843   |
| 2017 | small         | 3,634,845 |
| 2018 | medium        | 2,056,658 |
| 2018 | micro         | 1,067,644 |
| 2018 | large         | 345,084   |
| 2018 | small         | 3,597,596 |
| 2019 | micro         | 1,035,518 |
| 2019 | medium        | 2,122,304 |
| 2019 | small         | 3,564,126 |
| 2019 | large         | 353,069   |
| 2020 | micro         | 1,006,484 |
| 2020 | small         | 3,536,737 |
| 2020 | large         | 361,125   |
| 2020 | medium        | 2,190,394 |

_Approximately 49.53% of transactions fall into the small spending tier, followed by 31.31% in the medium tier, 14.00% in the micro tier, and 5.16% in the large tier. This shows that the majority of transactions are relatively small in value. When examining the yearly distribution, the pattern remains consistent across the years with small and medium transactions dominating the dataset. There is no significant structural change in the distribution, indicating stable consumer spending behavior over time._

---

## Section A: Data Architecture Questions

_The family has some questions about how the system works._

### Q1. Merchant Name Change

> "We just found out one of the merchants changed their name last year. Where in the pipeline do we update this, and what layers need to be reprocessed?"

_The merchant name should be updated in the merchant reference data (such as the merchants.csv file). After updating the merchant information, the pipeline should be rerun to regenerate the staging and analytics layers. The raw transaction data does not need to be modified because it stores the original records. Reprocessing ensures that reports reflect the updated merchant name._

---

### Q2. New Family Member

> "Our daughter started college and has her own credit card now. How do we add a new family member to the system without breaking existing data?"

_A new family member can be added by inserting a new record in the members reference data wth a new member id. Future transactions will reference this new member id when they are ingested. Historical transactions remain unchanged because they are already reference existing members. This approach prevents breaking existing data._

---

### Q3. Average Monthly Grocery Spending

> "We want to know our average monthly grocery spending. Walk us through exactly which transformations and joins produce this number."

_The pipeline first loads transaction data from the raw layer. The transactions are then joined with the category table to identify which transactions belong to the groceries category. Next, the data is aggregated by month to calculate total grocery spending per month. Finally, the average monthly grocery spending is calculated from these monthly totals._

---

### Q4. Duplicate Transactions

> "Last month's bank export had 500 duplicate transactions. How does your pipeline handle this? If it doesn't yet, what would you add?"

_Duplicate transactions should be handled in the staging layer during data cleaning. A deduplicaion step can be added using a unique transcation identifier such as ransaction id. For example, using the dropDuplicates() to remove repeated records. This prevents duplicate transactions from affecting analytics results._

---

### Q5. Data Backup & Recovery

> "We're worried about losing our data. What's your backup strategy? What's the most data we could lose if something crashes?"

_Hint: Think about RPO (Recovery Point Objective) and RTO (Recovery Time Objective)._

_The raw data layer acts as the primary backup because it stores the original source data. If the staging or analytics layers are lost, they can be regenerated by rerunning the pipeline from the raw layer. This means the Recovery Point Objective (RPO) is minimal since raw data is preserved. The Recovery Time Objective (RTO) depends on how long it takes to rebuild the pipeline outputs._

---

## Section B: Engineering Questions

_The family's developer friend has some technical questions._

### Q6. CI/CD Pipeline

> "If we set up CI/CD for this project, what would the pipeline look like? What gets tested automatically, and what triggers the tests?"

_In a CI/CD pipeline, automated tests run whenever code is pushed to the repository. These tests can include unit tests for data transformations and validation checks for schema consistency. If all tests pass, the updated pipeline code can be deployed automatically. This helps ensure that pipeline changes do not break existing functionality._

---

### Q7. Monthly Automation with Orchestration

> "We want this pipeline to run automatically every month when the bank exports new transactions. How would you set this up? Draw the DAG."

_The pipeline can be scheduled using a workflow orchestration tool such as Apache Airflow or a cron job. Each step in the DAG performs tasks such as ingestion, cleaning, enrichment, and analytics generation._

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
                    ┌──────────────┐      ┌──────────────┐
                    │ Generate     │  ◀── │ Aggregations │
                    │ Reports      │      │  Analytics   │
                    └──────────────┘      └──────────────┘
```

---

## Section C: Analytics Insights

_The family wants your professional opinion._

### Q8. Price Trend Analysis

> "We looked at your yearly average transaction table and prices seem to go up. Can you calculate the exact rate? Is it consistent across all categories?"

_Show your work:_

| average_yoy_percent |
| ---- |
| 2.0 |

_The average transaction amount shows a steady upward trend over the years. The year-over-year growth rate is approximately 2.0% on average, indicating a consistent increase in transaction prices. When examining the category-level trends, most categories exhibit a similar gradual increase each year. This suggests that the price growth pattern is broadly consistent across categories rather than being driven by only a few specific ones._

---

### Q9. Spending Recommendations

> "Based on your summary tables, give us 3 actionable recommendations for how we can reduce spending next year."

1. _Reduce discretionary spending such as Dining Out and Entertainment. These categories account for a large portion of spending and can be reduced by cooking at home more often or limiting entertainment expenses._

2. _Review electronics and clothing purchases. These categories show relatively high total spending, so planning purchases in advance and avoiding impulse buying could significantly lower expenses._

3. _Increase the portion of income allocated to savings and investments. Redirecting a small percentage of discretionary spending into Savings Deposit or Investment categories can help improve long-term financial stability._

---

### Q10. Needs vs Wants

> "Which spending categories are 'needs' vs 'wants'? What percentage of our total spending goes to each?"

_Needs include essential categories such as groceries, rent or mortgage, utilities, healthcare, insurance, transportation, gasoline, and education. Wants include discretionary categories such as dining out, entertainment, clothing, electronics, gifts, personal care, subscriptions, and home improvement. Savings include categories such as savings deposits and investments. Based on the aggregated spending totals, approximately 49.77% of total spending goes toward needs, 43.82% toward wants, and 6.42% toward savings. This indicates that while most spending is allocated to essential expenses, a substantial portion is still used for discretionary purchases._

| Budget Type | Total Spending      | Percentage |
| ----------- | ------------------- | ---------- |
| Needs       | 2.160425364359854E9 | 49.77%     |
| Wants       | 1.902196973492909E9 | 43.82%     |
| Savings     | 2.784890597782593E8 | 6.42%      |
