# Retail-Sales-Analysis-Etl-BI-Project
Got it 👍 Since you only want the **README to explain the code (not dashboards, insights, etc.)**, here’s a **clean, code-focused README.md** draft you can add to your project folder:

---

# 🛠️ Retail Sales ETL Project

## 📌 Overview

This project implements an **ETL pipeline** for retail sales data using **Python** and **Oracle SQL**. The workflow extracts raw data from CSV files, transforms it using Pandas, and loads it into a staging schema and final **Data Warehouse (DW)** schema.

---

## ⚙️ Tech Stack

* **Python 3.x**

  * `pandas` → Data extraction & transformation
  * `numpy` → Handling missing values & calculations
  * `sqlalchemy` → Database connectivity
  * `cx_Oracle` → Oracle DB driver
* **Oracle SQL** → Staging & Data Warehouse schema

---

## 📂 Project Structure

```
Retail-Sales-ETL/
│
├── data/               # Raw input CSV files
│   ├── customers.csv
│   ├── products.csv
│   ├── sales.csv
│   └── stores.csv
│
├── etl/
|  ├── .env
|  ├── config.ini
|  ├── etl.py                # Main ETL script (Python)
|  └──hybrid_settings.py            
│
├── sql/                # SQL scripts
│   ├── ddl_orcale.sql
│   ├── create_dw.sql
│   └── load_dw.sql
│
├── bi/
│   ├── bi_report.pbix
├── docs/
│   ├── sql_query.docx
│   └── project_docs.docx
├── Scripts/
|   └── generate_data.py        #generates required data for project 
├── requirements.txt    # Python dependencies
└── README.md           # Project documentation
```

---

## 🔄 ETL Script Description (`etl.py`)

The **ETL script** (`etl.py`) performs the following steps:

### 1. **Import Modules**

* `pandas`, `numpy` → Data wrangling
* `sqlalchemy` → DB connection
* `logging` → Process logging

### 2. **Database Connection**

```python
engine = create_engine(
    "oracle+cx_oracle://<username>:<password>@<host>:<port>/<service_name>"
)
```

### 3. **Extraction**

* Reads CSV files into Pandas DataFrames:

```python
df_sales = pd.read_csv("data/sales.csv")
df_products = pd.read_csv("data/products.csv")
df_customers = pd.read_csv("data/customers.csv")
df_stores = pd.read_csv("data/stores.csv")
```

### 4. **Transformation**

* Handle missing values (`fillna`, `dropna`).
* Convert columns to correct datatypes (e.g., dates, numerics).
* Add derived columns:

  * `Revenue = Quantity × Unit_Price`
  * `Profit = Revenue – Cost`

### 5. **Loading**

* Load DataFrames into **Staging Tables**:

```python
df_sales.to_sql("stg_sales", engine, if_exists="replace", index=False)
```

* Transform & insert into **DW Star Schema** (`FactSales`, `DimProduct`, `DimCustomer`, etc.).

### 6. **Logging**

* All steps are logged into a `etl.log` file for monitoring success/failure.

---

## 🚀 How to Run the Code

1. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```
2. Update **DB credentials** inside `etl.py`.
3. Run ETL script:

   ```bash
   python etl.py
   ```
4. Execute SQL scripts from `/sql/` in Oracle DB to create schema & relations.

---

## 📜 Deliverables

* `etl.py` → Python ETL script
* `/sql/` → Schema creation & load scripts
* `/data/` → Raw input CSV files
* `README.md` → Code documentation

---
