# Midterm Report

**Student Name:** _______________  
**Student ID:** _______________  
**Date:** _______________

---

## Part 1: Data Exploration Answers

> How many total transactions are there?

_Your answer here._

> How many unique family members, merchants, and categories?

_Your answer here._

> How many rows have null or empty `amount` values?

_Your answer here._

> What is the date range of the transactions?

_Your answer here._

---

## Part 4: Join Analysis

> What join type did you use for enriching transactions, and why?

_Your answer here._

> How many transactions have no matching merchant in the merchants table?

_Your answer here._

> What would happen if you used an inner join instead?

_Your answer here._

---

## Part 5: Analytics Insights

> Look at the average transaction amount per year table. Do you notice a trend? Calculate the approximate year-over-year percentage change. What might explain this?

_Your answer here. Show your calculation:_

| Year | Avg Amount | YoY Change (%) |
|------|-----------|----------------|
| 2016 | | |
| 2017 | | |
| 2018 | | |
| 2019 | | |
| 2020 | | |
| 2021 | | |
| 2022 | | |
| 2023 | | |
| 2024 | | |
| 2025 | | |

_Your explanation:_

> Which category has the highest total spending? Which has grown the fastest over 10 years?

_Your answer here._

> Compare spending between family members. Who spends the most? On what?

_Your answer here._

> What percentage of transactions fall in each spending tier? Has this distribution changed over the years?

_Your answer here._

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
