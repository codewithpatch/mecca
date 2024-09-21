- [Requirements](#requirements)
- [Instructions](#instructions)
- [Answer](#answer)
  - [Using current model](#using-current-model)
  - [Recommendations](#recommendations)
    - [Data model](#data-model)
      - [Transform the date column into a date field](#transform-the-date-column-into-a-date-field)
      - [Create a date dimension](#create-a-date-dimension)
    - [Apply clustering on tables](#apply-clustering-on-tables)

# Requirements

1. Write a query to determine total credit card transaction value by Customer_Segment for customers between the age group 30 - 40.
2. Write a query to determine top 5 customers who have made the most credit card transaction spend.
3. Write a query to determine list of customers who have spent more than their credit limit before June 2016.
4. Write a query to determine minimum, maximum and average credit card transaction value by Card_Family
5. Write a query to determine total credit card transaction value by month, percentage growth from previous month and Year to date transaction value.

    - Note:
        Year to date (YTD) transaction value = Transaction value between the beginning of the year and the current month.

# Instructions

1. Work on the above requirements using SQL and document the queries.
2. Feel free to add any recommendation on how you can improve query performance keeping in mind the above requirements or  a data model to
answer them efficiently.
3. Once completed, email the documentation file back to us.
4. Come prepared to talk about your code and possible extensions or modifications.

# Answer

## Using current model
![alt text](datamodel.png)

1. Write a query to determine total credit card transaction value by Customer_Segment for customers between the age group 30 - 40.

    ```sql
    SELECT
        cust.customer_segment as CUSTOMER_SEGMENT,
        SUM(trans.transcation_value) as TOTAL_CARD_TRANSACTION
    FROM customerbase_csv cust
    INNER JOIN cardbase_csv card
        ON cust.cust_id = card.cust_id
    INNER JOIN transcationbase_csv trans
        ON card.card_number = trans.card_number
    WHERE cust.AGE BETWEEN 30 AND 40
    GROUP BY ALL;
    ```

2. Write a query to determine top 5 customers who have made the most credit card transaction spend.

   ```sql
    SELECT TOP 5
        cust.cust_id AS CUSTOMER_ID,
        SUM(trans.transcation_value) as TOTAL_CARD_TRANSACTION
    FROM customerbase_csv cust
    INNER JOIN cardbase_csv card
        ON cust.cust_id = card.cust_id
    INNER JOIN transcationbase_csv trans
        ON card.card_number = trans.card_number
    GROUP BY ALL
    ORDER BY TOTAL_CARD_TRANSACTION DESC;
   ```

3. Write a query to determine list of customers who have spent more than their credit limit before June 2016.

    ```sql
    SELECT
        cust.cust_id AS CUSTOMER_ID,
        SUM(trans.transaction_value) AS TOTAL_CARD_TRANSACTION,
        SUM(card.credit_limit) AS TOTAL_CREDIT_LIMIT,
        SUM(card.card_limit - trans.transaction_value) AS REMAINING_CARD_LIMIT 
    FROM customerbase_csv cust
    INNER JOIN cardbase_csv card
        ON cust.cust_id = card.cust_id
    INNER JOIN transcationbase_csv trans
        ON card.card_number = trans.credit_card_id
    WHERE TO_DATE(trans.transaction_date, 'DD-MON-YY') < DATE('2016-06-01')
    GROUP BY ALL
    HAVING REMAINING_CARD_LIMIT < 0
    ORDER BY REMAINING_CARD_LIMIT DESC;
    ```

4. Write a query to determine minimum, maximum and average credit card transaction value by Card_Family

    ```sql
    SELECT
        card.card_family AS CARD_FAMILY,
        MIN(trans.transaction_value) AS MINIMUM_SPEND,
        MAX(trans.transaction_value) AS MAXIMUM_SPEND,
        AVG(trans.transaction_value) AS AVERAGE_SPEND
    FROM cardbase_csv card
    INNER JOIN transactionbase_csv trans
        ON cust.card_number = card.credit_card_id
    GROUP BY ALL;
    ```

5. Write a query to determine total credit card transaction value by month, percentage growth from previous month and Year to date transaction value.

    - Note:
        Year to date (YTD) transaction value = Transaction value between the beginning of the year and the current month.

    ```sql
    WITH MONTHLY_TRANS AS (
        SELECT
            YEAR(TO_DATE(trans.transaction_date, 'DD-MON-YY')) AS TRANS_YEAR,
            YEAR(TO_DATE(trans.transaction_date, 'DD-MON-YY'), 'YYYYMM') AS YEARMONTH,
            SUM(trans.transaction_value) AS MONTHLY_TRANSACTIONS,
            LAG(MONTHLY_TRANSACTIONS) OVER(ORDER BY YEARMONTH) AS PREVIOUS_MONTH_TRANS 
        FROM transactionbase_csv trans
    )
    SELECT
        YEARMONTH AS YEARMONTH,
        (MONTHLY_TRANSACTIONS - PREVIOUS_MONTH_TRANS) / PREVIOUS_MONTH_TRANS * 100 AS MONTH_GROWTH_RATE,
        SUM(MONTHLY_TRANSACTIONS) OVER(PARTITION BY TRANS_YEAR ORDER BY YEARMONTH) AS YTD_TRANSACTION
    FROM MONTHLY_TRANS
    ORDER BY YEARMONTH DESC

    ```

## Recommendations

### Data model

- It is recommend to model your data properly with correct data types as shown below.
- Our selected model is star schema
- This will help us have an easier visual of our model
- It is easier to build a query as it only requires us to do 1 level of join

![alt text](starschema.png)

#### Transform the date column into a date field

- Since the column is already transformed, our final query to build the report won't need to account for the transformation which will result for a more effecient query execution.
- Additional note, snowflake stores DATE and TIMESTAMP columns more efficiently than VARCHAR also resulting to a better query performance.

#### Create a date dimension

- Date dimension gives us another layer of attributes for the dates which will help us do less transformation for the transaction_date column.

### Apply clustering on tables

- If we don't apply clustering on our table, snowflake's query processing layer will analyze our data and apply automatic clustering to our data usually by the order of how we append to the table.
- To improve our query performance, we will apply clustering on our table in which we often use on filtering or in join conditions given by the requirements

```sql
CREATE TABLE FACT_TRANSACTIONBASE (
    TRANSACTION_ID VARCHAR(50),
    TRANSACTION_DATE DATE,
    CUST_ID VARCHAR(50),
    CREDIT_CARD_ID: VARCHAR(50),
    TRANSACTION_VALUE NUMBER,
    TRANSACTION_SEGMENT VARCHAR(20)
)
```