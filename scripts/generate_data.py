#!/usr/bin/env python
# --------------------------------------------------------------
# generate_data.py – Synthetic Retail data (≥ 50k rows each)
#   – Indian names, cities, states, and rupee amounts
# --------------------------------------------------------------
import argparse
import os
import random
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from faker import Faker
from tqdm import tqdm

# ----------------------------------------------------------------------
# Global seed for reproducibility
# ----------------------------------------------------------------------
SEED = 42
random.seed(SEED)
np.random.seed(SEED)

# Faker locale for India
FAKER = Faker('en_IN')
FAKER.seed_instance(SEED)

# ----------------------------------------------------------------------
# CLI – allows a member to change the base row count in one place
# ----------------------------------------------------------------------
parser = argparse.ArgumentParser(
    description="Generate synthetic Indian retail CSV files"
)
parser.add_argument(
    "--rows", type=int, default=50_000,
    help="Base row count for each dimension table (customers, products)."
)
args = parser.parse_args()
BASE = args.rows

# ----------------------------------------------------------------------
# Sizes (you can tune them if you need more/less data)
# ----------------------------------------------------------------------
N_CUSTOMERS = BASE
N_PRODUCTS   = BASE
N_STORES     = max(5_000, BASE // 10)   # keep stores realistic
N_SALES      = BASE * 5                # approx 5 sales per customer

# ----------------------------------------------------------------------
# Output folder (relative to repo root)
# ----------------------------------------------------------------------
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
OUT_DIR = os.path.join(ROOT, "data")
os.makedirs(OUT_DIR, exist_ok=True)

# ----------------------------------------------------------------------
# Helper utilities
# ----------------------------------------------------------------------
def random_date(start: datetime, end: datetime) -> datetime:
    """Uniform random datetime between start and end."""
    delta = end - start
    return start + timedelta(seconds=random.randint(0, int(delta.total_seconds())))

def price_from_category(cat: str) -> float:
    """Reasonable price ranges (in INR) per product category."""
    ranges = {
        "Electronics": (5000, 50000),           # ₹ 5 000 – ₹ 50 000
        "Home & Kitchen": (500, 15000),        # ₹ 500 – ₹ 15 000
        "Fashion": (200, 15000),               # ₹ 200 – ₹ 15 000
        "Sports": (500, 20000),                # ₹ 500 – ₹ 20 000
        "Toys": (100, 3000),                  # ₹ 100 – ₹ 3 000
        "Books": (100, 2000),                  # ₹ 100 – ₹ 2 000
        "Health": (100, 5000),                 # ₹ 100 – ₹ 5 000
        "Automotive": (1000, 30000),           # ₹ 1 000 – ₹ 30 000
    }
    lo, hi = ranges.get(cat, (500, 20000))
    return round(random.uniform(lo, hi), 2)

# ----------------------------------------------------------------------
# 1️⃣  Customers
# ----------------------------------------------------------------------
def create_customers() -> pd.DataFrame:
    """
    Generate N_CUSTOMERS synthetic Indian customers.

    • Uses Faker's dedicated providers (city, state, street_address) – no
      fragile string‑splitting.
    • Guarantees a non‑null state for every row.
    • The `city` column actually stores a readable address
      (street + city) – you can split it later if you need just the city name.
    """
    genders = ["Male", "Female", "Non‑binary", "Other"]
    memberships = ["Bronze", "Silver", "Gold", "Platinum"]
    rows = []

    for cid in tqdm(range(1, N_CUSTOMERS + 1), desc="Customers"):
        # ---- 1️⃣  Basic name & email (keep the Faker profile for realism)
        profile = FAKER.profile()
        # profile["name"] may contain middle names – keep only first & last
        name_parts = profile["name"].split()
        first_name = name_parts[0]
        last_name = name_parts[-1] if len(name_parts) > 1 else ""

        # ---- 2️⃣  Age (independent of Faker's birthdate)
        age = random.randint(18, 85)

        # ---- 3️⃣  Location data – *no* newline ever appears
        street = FAKER.street_address()
        city_name = FAKER.city()
        state_name = FAKER.state()          # full Indian state name

        # Build a readable address that will live in the CSV's `city` column.
        # (If you prefer a separate `street` column you can add one.)
        readable_address = f"{street} | {city_name}"

        # ---- 4️⃣  Membership tier (weighted distribution)
        membership = random.choices(
            memberships, weights=[0.5, 0.3, 0.15, 0.05], k=1
        )[0]

        rows.append({
            "customer_id": cid,
            "first_name": first_name,
            "last_name": last_name,
            "gender": random.choice(genders),
            "age": age,
		    "city":city_name,
            "state": state_name,         # e.g. "Tamil Nadu"
            "membership_level": membership,
            # "address": readable_address    # e.g. "75‑C‑221, Chennai"  
        })

    return pd.DataFrame(rows)

# ----------------------------------------------------------------------
# 2️⃣  Products
# ----------------------------------------------------------------------
def create_products() -> pd.DataFrame:
    categories = {
        "Electronics": ["Mobile", "Laptop", "Tablet", "Camera"],
        "Home & Kitchen": ["Cookware", "Furniture", "Appliance", "Bedding"],
        "Fashion": ["Men's Clothing", "Women's Clothing", "Shoes", "Accessories"],
        "Sports": ["Outdoor", "Gym Equipment", "Footwear", "Apparel"],
        "Toys": ["Action Figure", "Board Game", "Puzzle", "Plush"],
        "Books": ["Fiction", "Non‑Fiction", "Children", "Comics"],
        "Health": ["Supplements", "Personal Care", "Medical Device"],
        "Automotive": ["Car Accessory", "Tool", "Part"],
    }
    brands = ["Acme", "Globex", "Initech", "Umbrella",
              "Stark", "Wayne", "Soylent", "Hooli"]
    colors = ["Red", "Blue", "Green", "Black", "White",
              "Yellow", "Purple", "Orange", "Gray"]
    sizes = ["XS", "S", "M", "L", "XL", "One Size", "N/A"]

    rows = []
    cat_keys = list(categories.keys())

    for pid in tqdm(range(1, N_PRODUCTS + 1), desc="Products"):
        cat = random.choice(cat_keys)
        sub = random.choice(categories[cat])
        price = price_from_category(cat)
        cost = round(price * random.uniform(0.4, 0.9), 2)   # cost < price

        rows.append({
            "product_id": pid,
            "product_name": f"{FAKER.word().title()} {sub}",
            "category": cat,
            "sub_category": sub,
            "brand": random.choice(brands),
            "price": price,          # INR
            "cost": cost,            # INR
            "color": random.choice(colors),
            "size": random.choice(sizes),
        })
    return pd.DataFrame(rows)

# ----------------------------------------------------------------------
# 3️⃣  Stores (Indian macro‑regions + states)
# ----------------------------------------------------------------------
INDIAN_STATES = [
    # North
    "Jammu & Kashmir", "Himachal Pradesh", "Punjab", "Uttarakhand",
    "Haryana", "Delhi", "Uttar Pradesh", "Rajasthan",
    # South
    "Andhra Pradesh", "Telangana", "Karnataka", "Tamil Nadu",
    "Kerala", "Pondicherry", "Lakshadweep",
    # East
    "Bihar", "Jharkhand", "Odisha", "West Bengal", "Sikkim",
    # West
    "Gujarat", "Maharashtra", "Goa", "Madhya Pradesh",
    # Central (overlaps with North/West but good for variety)
    "Chhattisgarh", "Madhya Pradesh", "Uttar Pradesh"
]

REGIONS = {
    "North": ["Jammu & Kashmir", "Himachal Pradesh", "Punjab", "Uttarakhand",
              "Haryana", "Delhi", "Uttar Pradesh", "Rajasthan"],
    "South": ["Andhra Pradesh", "Telangana", "Karnataka", "Tamil Nadu",
              "Kerala", "Pondicherry", "Lakshadweep"],
    "East":  ["Bihar", "Jharkhand", "Odisha", "West Bengal", "Sikkim"],
    "West":  ["Gujarat", "Maharashtra", "Goa", "Madhya Pradesh"],
    "Central": ["Chhattisgarh", "Madhya Pradesh", "Uttar Pradesh"]
}
STORE_TYPES = ["Flagship", "Outlet", "Mall", "Online", "Pop‑up"]

def create_stores() -> pd.DataFrame:
    rows = []

    for sid in tqdm(range(1, N_STORES + 1), desc="Stores"):
        # pick a region first, then a state belonging to that region
        region = random.choice(list(REGIONS.keys()))
        state  = random.choice(REGIONS[region])

        rows.append({
            "store_id": sid,
            "store_name": f"{FAKER.company()} {random.choice(['Store','Outlet','Shop','Market'])}",
            "city": FAKER.city(),
            "state": state,
            "region": region,
            "store_type": random.choice(STORE_TYPES),
        })
    return pd.DataFrame(rows)

# ----------------------------------------------------------------------
# 4️⃣  Sales (fact table) – amounts are in INR
# ----------------------------------------------------------------------
def create_sales(cust_df: pd.DataFrame,
                 prod_df: pd.DataFrame,
                 store_df: pd.DataFrame) -> pd.DataFrame:
    cust_ids = cust_df["customer_id"].values
    prod_ids = prod_df["product_id"].values
    store_ids = store_df["store_id"].values
    price_map = prod_df.set_index("product_id")["price"].to_dict()

    start_ts = datetime.now() - timedelta(days=365 * 2)   # last 2 years
    end_ts   = datetime.now()

    rows = []
    for sid in tqdm(range(1, N_SALES + 1), desc="Sales"):
        c_id = int(np.random.choice(cust_ids))
        p_id = int(np.random.choice(prod_ids))
        s_id = int(np.random.choice(store_ids))

        qty = int(np.random.choice([1, 2, 3, 4, 5],
                                   p=[0.6, 0.2, 0.1, 0.07, 0.03]))
        discount = round(
            np.random.choice([0, 5, 10, 15, 20, 25, 30],
                             p=[0.65, 0.1, 0.08, 0.07, 0.05, 0.03, 0.02]), 2)

        unit_price = round(price_map[p_id] * (1 - discount / 100), 2)   # INR
        total_amount = round(unit_price * qty, 2)                      # INR

        rows.append({
            "sales_id": sid,
            "customer_id": c_id,
            "product_id": p_id,
            "store_id": s_id,
            "quantity": qty,
            "sales_date": random_date(start_ts, end_ts).strftime("%d-%m-%Y"),
            "discount_pct": discount,
            "unit_price": unit_price,
            "total_amount": total_amount,
        })
    return pd.DataFrame(rows)

# ----------------------------------------------------------------------
# 5️⃣  Main – write CSVs + a tiny validation report
# ----------------------------------------------------------------------
def main() -> None:
    print("\n=== Generating synthetic Indian retail data ===\n")
    customers = create_customers()
    products  = create_products()
    stores    = create_stores()
    sales     = create_sales(customers, products, stores)

    # ------------------------------------------------------------------
    # Write raw CSVs (no compression – easy for Power BI)
    # ------------------------------------------------------------------
    customers.to_csv(os.path.join(OUT_DIR, "customers.csv"), index=False)
    products.to_csv(os.path.join(OUT_DIR, "products.csv"), index=False)
    stores.to_csv(os.path.join(OUT_DIR, "stores.csv"), index=False)
    sales.to_csv(os.path.join(OUT_DIR, "sales.csv"), index=False)

    # ------------------------------------------------------------------
    # Small validation report (helps the reviewer)
    # ------------------------------------------------------------------
    report = [
        f"Rows – customers : {len(customers):,}",
        f"Rows – products  : {len(products):,}",
        f"Rows – stores    : {len(stores):,}",
        f"Rows – sales     : {len(sales):,}",
    ]

    # Null‑cell count
    for df, name in [(customers, 'customers'), (products, 'products'),
                     (stores, 'stores'), (sales, 'sales')]:
        report.append(f"{name}: null cells = {df.isnull().sum().sum()}")

    # Referential‑integrity check (should be zero)
    bad = sales[
        ~sales.customer_id.isin(customers.customer_id) |
        ~sales.product_id.isin(products.product_id) |
        ~sales.store_id.isin(stores.store_id)
    ]
    report.append(f"sales rows with bad foreign keys = {len(bad)}")

    with open(os.path.join(OUT_DIR, "validation_report.txt"), "w") as f:
        f.write("\n".join(report))

    print("\n".join(report))
    print("\n✅ Synthetic CSVs generated in ./data 🎉\n")


if __name__ == "__main__":
    main()
    
    
    
    
# {'job': 'Physiological scientist', 
#  'company': 'Kapoor, Yadav and Buch', 
#  'ssn': '743-48-7347', 
#  'residence': '455\nBaria Circle\nSaharanpur 623166',
#  'current_location': (Decimal('-1.179704'), Decimal('111.312139')), 
#  'blood_group': 'O-', 
#  'website': ['http://www.hayer-zachariah.org/', 'http://aggarwal-goda.com/', 'https://shenoy.org/', 'http://deshpande.info/'], 
#  'username': 'ksengupta', 
#  'name': 'Shayak Sami', 
#  'sex': 'F', 
#  'address': '80/69, Saini, Gwalior-272046',
#  'mail': 'lakshit75@yahoo.com', 
#  'birthdate': datetime.date(2013, 4, 11)}    