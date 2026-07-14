USE DATABASE CAPSTONE_BRONZE_DB;

USE SCHEMA RAW;


CREATE OR REPLACE TABLE BRONZE_SALES
(
    OrderDate DATE,
    StockDate DATE,
    OrderNumber STRING,
    ProductKey NUMBER,
    CustomerKey NUMBER,
    TerritoryKey NUMBER,
    OrderLineItem NUMBER,
    OrderQuantity NUMBER,

    LOAD_TIMESTAMP TIMESTAMP,
    SOURCE_FILE STRING
);



CREATE OR REPLACE TABLE BRONZE_CUSTOMER
(
    CustomerKey NUMBER,
    Prefix STRING,
    FirstName STRING,
    LastName STRING,
    BirthDate DATE,
    MaritalStatus STRING,
    Gender STRING,
    EmailAddress STRING,
    AnnualIncome NUMBER,
    TotalChildren NUMBER,
    EducationLevel STRING,
    Occupation STRING,
    HomeOwner STRING,

    LOAD_TIMESTAMP TIMESTAMP,
    SOURCE_FILE STRING
);



CREATE OR REPLACE TABLE BRONZE_PRODUCT
(
    ProductKey NUMBER,
    ProductSubcategoryKey NUMBER,
    ProductSKU STRING,
    ProductName STRING,
    ModelName STRING,
    ProductDescription STRING,
    ProductColor STRING,
    ProductSize STRING,
    ProductStyle STRING,
    ProductCost FLOAT,
    ProductPrice FLOAT,

    LOAD_TIMESTAMP TIMESTAMP,
    SOURCE_FILE STRING
);



CREATE OR REPLACE TABLE BRONZE_PRODUCT_CATEGORY
(
    ProductCategoryKey NUMBER,
    CategoryName STRING,

    LOAD_TIMESTAMP TIMESTAMP,
    SOURCE_FILE STRING
);



CREATE OR REPLACE TABLE BRONZE_PRODUCT_SUBCATEGORY
(
    ProductSubcategoryKey NUMBER,
    SubcategoryName STRING,
    ProductCategoryKey NUMBER,

    LOAD_TIMESTAMP TIMESTAMP,
    SOURCE_FILE STRING
);



CREATE OR REPLACE TABLE BRONZE_TERRITORY
(
    SalesTerritoryKey NUMBER,
    Region STRING,
    Country STRING,
    Continent STRING,

    LOAD_TIMESTAMP TIMESTAMP,
    SOURCE_FILE STRING
);



CREATE OR REPLACE TABLE BRONZE_CALENDAR
(
    Date DATE,

    LOAD_TIMESTAMP TIMESTAMP,
    SOURCE_FILE STRING
);



CREATE OR REPLACE TABLE BRONZE_RETURNS
(
    ReturnDate DATE,
    TerritoryKey NUMBER,
    ProductKey NUMBER,
    ReturnQuantity NUMBER,

    LOAD_TIMESTAMP TIMESTAMP,
    SOURCE_FILE STRING
);