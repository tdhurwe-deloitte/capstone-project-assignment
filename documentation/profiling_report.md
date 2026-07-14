# Data Profiling Report

## Dataset Overview

The dataset contains sales transactions from 2020-2022 along with customer, product, territory and calendar dimensions.

---

# File Profiling Summary


| File | Type | Primary Key | Rows |
|---|---|---|---|
| Sales 2020 | Fact | OrderNumber + OrderLineItem | TBD |
| Sales 2021 | Fact | OrderNumber + OrderLineItem | TBD |
| Sales 2022 | Fact | OrderNumber + OrderLineItem | TBD |
| Customer | Dimension | CustomerKey | TBD |
| Product | Dimension | ProductKey | TBD |


---

# Entity Relationships

## Sales

Foreign Keys:

- CustomerKey → Customer
- ProductKey → Product
- TerritoryKey → Territory


---

# Data Quality Issues Identified

## Issue 1
Missing values in mandatory fields.

Impact:
Incorrect joins and inaccurate reporting.


## Issue 2
Duplicate customer records.

Impact:
Incorrect customer aggregation.


## Issue 3
Invalid date values.

Impact:
Incorrect time-based analysis.


## Issue 4
Products without category mapping.

Impact:
Incomplete product hierarchy.


## Issue 5
Returns without matching sales records.

Impact:
Incorrect net revenue calculations.
