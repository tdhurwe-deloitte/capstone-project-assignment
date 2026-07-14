# Silver Layer Data Quality Report


## Data Cleansing Rules Applied


### Duplicate Removal

Duplicate records were removed using business keys.

Examples:

- CustomerKey for customers
- ProductKey for products
- OrderNumber + OrderLineItem for sales


### Null Handling

Missing values were handled:

- Missing AnnualIncome replaced with 0
- Missing ProductDescription replaced with UNKNOWN


### Data Type Standardization

All columns were normalized to uppercase naming convention.

Numeric fields were validated before calculations.


### Derived Metrics

Sales metrics generated:

- Revenue
- Cost
- Profit
- Profit Margin
- Net Revenue


### Return Integration

Returns were aggregated by:

- Product
- Territory

and integrated into sales calculations.