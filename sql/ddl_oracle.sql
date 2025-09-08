/* --------------------------------------------------------------
   Oracle DDL for the Retail Data Warehouse
   Run this script once (e.g. sqlplus or via SQLAlchemy)
   -------------------------------------------------------------- */

/* 1️⃣  Users / schema ------------------------------------------------
   For a real production deployment you would create a dedicated
   user.  For this demo we assume the user defined in the .env
   (retail_user) already exists and has the required privileges.
*/

-- Optional: create user & grant (run as SYS if you need it)
-- CREATE USER retail_user IDENTIFIED BY MyVerySecretOraPwd123;
-- GRANT CONNECT, RESOURCE, CREATE TABLE, CREATE SEQUENCE TO retail_user;
-- ALTER USER retail_user QUOTA UNLIMITED ON USERS;

/* 2️⃣  Sequences (Oracle requires a sequence for surrogate keys) ----- */
CREATE SEQUENCE dim_customer_seq START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE dim_product_seq  START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE dim_store_seq    START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE dim_date_seq     START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE fact_sales_seq   START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;

/* 3️⃣  Dimension tables ------------------------------------------------ */

CREATE TABLE dim_customer (
    customer_key    NUMBER PRIMARY KEY,
    customer_id     NUMBER NOT NULL,
    first_name      VARCHAR2(50),
    last_name       VARCHAR2(50),
    gender          VARCHAR2(20),
    age             NUMBER,
    city            VARCHAR2(100),
    state           VARCHAR2(50),
    membership_level VARCHAR2(20)
);

CREATE TABLE dim_product (
    product_key    NUMBER PRIMARY KEY,
    product_id     NUMBER NOT NULL,
    product_name   VARCHAR2(150),
    category      VARCHAR2(50),
    sub_category   VARCHAR2(50),
    brand          VARCHAR2(50),
    price          NUMBER(10,2),
    cost          NUMBER(10,2),
    color          VARCHAR2(30),
    size_           VARCHAR2(20)
);

drop table dim_product;
CREATE TABLE dim_store (
    store_key    NUMBER PRIMARY KEY,
    store_id     NUMBER NOT NULL,
    store_name   VARCHAR2(150),
    city         VARCHAR2(100),
    state        VARCHAR2(50),
    region       VARCHAR2(30),
    store_type   VARCHAR2(30)
);

CREATE TABLE dim_date (
    date_key       NUMBER PRIMARY KEY,   -- YYYYMMDD integer
    calendar_date  DATE NOT NULL,
    day            NUMBER,
    month          NUMBER,
    year           NUMBER,
    quarter        NUMBER,
    weekday        NUMBER                -- 1=Mon … 7=Sun
);

/* 4️⃣  Fact table ------------------------------------------------------ */

CREATE TABLE fact_sales (
    sales_key      NUMBER PRIMARY KEY,
    sales_id       NUMBER NOT NULL,
    customer_key   NUMBER NOT NULL REFERENCES dim_customer(customer_key),
    product_key    NUMBER NOT NULL REFERENCES dim_product(product_key),
    store_key      NUMBER NOT NULL REFERENCES dim_store(store_key),
    date_key       NUMBER NOT NULL REFERENCES dim_date(date_key),
    quantity       NUMBER,
    unit_price     NUMBER(12,2),
    discount_pct   NUMBER(5,2),
    total_amount   NUMBER(14,2)
);

/* 5️⃣  Indexes for performance (optional but recommended) */
CREATE INDEX ix_fact_sales_customer ON fact_sales(customer_key);
CREATE INDEX ix_fact_sales_product  ON fact_sales(product_key);
CREATE INDEX ix_fact_sales_store   ON fact_sales(store_key);
CREATE INDEX ix_fact_sales_date    ON fact_sales(date_key);


ALTER TABLE dim_customer MODIFY customer_key DEFAULT dim_customer_seq.NEXTVAL;
ALTER TABLE dim_product  MODIFY product_key  DEFAULT dim_product_seq.NEXTVAL;
ALTER TABLE dim_store    MODIFY store_key    DEFAULT dim_store_seq.NEXTVAL;
ALTER TABLE dim_date     MODIFY date_key     DEFAULT dim_date_seq.NEXTVAL;
ALTER TABLE fact_sales   MODIFY sales_key    DEFAULT fact_sales_seq.NEXTVAL;


select count(*) from dim_customer;

truncate table dim_customer;
truncate table dim_store  ;

truncate table dim_product  ;


truncate table fact_sales;
truncate table dim_date;

select count(*) from dim_customer;
select count(*) from dim_product;
select count(*) from dim_store;
select count(*) from fact_sales;
select count(*) from dim_date;