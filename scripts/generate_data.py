#!/usr/bin/env python3
"""
generate_data.py
Generates a large SQL seed file with 500k+ rows across customers, products, and orders.
Output: mysql/init/02-data.sql
"""

import random
import string
from datetime import datetime, timedelta

CUSTOMERS = 100_000
PRODUCTS = 10_000
ORDERS = 400_000

FIRST_NAMES = [
    "James","Mary","John","Patricia","Robert","Jennifer","Michael","Linda","William","Barbara",
    "David","Elizabeth","Richard","Susan","Joseph","Jessica","Thomas","Sarah","Charles","Karen",
    "Christopher","Lisa","Daniel","Nancy","Matthew","Betty","Anthony","Margaret","Mark","Sandra",
    "Donald","Ashley","Steven","Dorothy","Paul","Kimberly","Andrew","Emily","Joshua","Donna",
    "Kenneth","Michelle","Kevin","Carol","Brian","Amanda","George","Melissa","Timothy","Deborah"
]

LAST_NAMES = [
    "Smith","Johnson","Williams","Brown","Jones","Garcia","Miller","Davis","Rodriguez","Martinez",
    "Hernandez","Lopez","Gonzalez","Wilson","Anderson","Thomas","Taylor","Moore","Jackson","Martin",
    "Lee","Perez","Thompson","White","Harris","Sanchez","Clark","Ramirez","Lewis","Robinson",
    "Walker","Young","Allen","King","Wright","Scott","Torres","Nguyen","Hill","Flores",
    "Green","Adams","Nelson","Baker","Hall","Rivera","Campbell","Mitchell","Carter","Roberts"
]

PRODUCT_ADJECTIVES = [
    "Premium","Deluxe","Ultra","Pro","Elite","Advanced","Smart","Eco","Compact","Portable",
    "Heavy-Duty","Lightweight","High-Performance","Budget","Professional","Industrial","Home",
    "Travel","Wireless","Waterproof"
]

PRODUCT_NOUNS = [
    "Widget","Gadget","Toolkit","Device","Module","Unit","System","Pack","Kit","Set",
    "Connector","Adapter","Controller","Monitor","Sensor","Interface","Panel","Hub","Relay","Switch"
]

PRODUCT_CATEGORIES = [
    "Electronics","Tools","Home & Garden","Sports","Automotive","Office","Kitchen","Health",
    "Toys","Clothing"
]

DOMAINS = ["gmail.com","yahoo.com","hotmail.com","outlook.com","example.com","mail.com","proton.me"]

def random_date(start_year=2020, end_year=2024):
    start = datetime(start_year, 1, 1)
    end = datetime(end_year, 12, 31)
    delta = end - start
    random_days = random.randint(0, delta.days)
    return (start + timedelta(days=random_days)).strftime("%Y-%m-%d %H:%M:%S")

def escape(s):
    return s.replace("'", "''").replace("\\", "\\\\")

def main():
    output_path = "mysql/init/02-data.sql"
    print(f"Generating seed data → {output_path}")
    print(f"  Customers: {CUSTOMERS:,}")
    print(f"  Products:  {PRODUCTS:,}")
    print(f"  Orders:    {ORDERS:,}")
    print(f"  Total:     {CUSTOMERS + PRODUCTS + ORDERS:,} rows")

    used_emails = set()

    with open(output_path, "w") as f:
        f.write("USE inventory;\n\n")
        f.write("SET foreign_key_checks = 0;\n\n")

        # ── Customers ──────────────────────────────────────────────────────────
        f.write("-- Customers\n")
        BATCH = 500
        batch_rows = []
        for i in range(1, CUSTOMERS + 1):
            fn = random.choice(FIRST_NAMES)
            ln = random.choice(LAST_NAMES)
            domain = random.choice(DOMAINS)
            tag = ''.join(random.choices(string.ascii_lowercase + string.digits, k=6))
            email = f"{fn.lower()}.{ln.lower()}.{tag}@{domain}"
            while email in used_emails:
                tag = ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))
                email = f"{fn.lower()}.{ln.lower()}.{tag}@{domain}"
            used_emails.add(email)
            dt = random_date()
            batch_rows.append(
                f"('{escape(fn)}','{escape(ln)}','{escape(email)}','{dt}')"
            )
            if len(batch_rows) == BATCH or i == CUSTOMERS:
                f.write(
                    "INSERT INTO customers (first_name, last_name, email, created_at) VALUES\n"
                    + ",\n".join(batch_rows) + ";\n"
                )
                batch_rows = []
                if i % 10000 == 0:
                    print(f"  customers: {i:,}/{CUSTOMERS:,}")

        # ── Products ───────────────────────────────────────────────────────────
        f.write("\n-- Products\n")
        batch_rows = []
        for i in range(1, PRODUCTS + 1):
            adj = random.choice(PRODUCT_ADJECTIVES)
            noun = random.choice(PRODUCT_NOUNS)
            cat = random.choice(PRODUCT_CATEGORIES)
            name = f"{adj} {noun} {i}"
            description = f"High quality {adj.lower()} {noun.lower()} for {cat.lower()} use. Model #{i:05d}."
            price = round(random.uniform(1.99, 999.99), 2)
            batch_rows.append(
                f"('{escape(name)}','{escape(description)}',{price})"
            )
            if len(batch_rows) == BATCH or i == PRODUCTS:
                f.write(
                    "INSERT INTO products (name, description, price) VALUES\n"
                    + ",\n".join(batch_rows) + ";\n"
                )
                batch_rows = []

        # ── Orders ─────────────────────────────────────────────────────────────
        f.write("\n-- Orders\n")
        batch_rows = []
        for i in range(1, ORDERS + 1):
            cust_id = random.randint(1, CUSTOMERS)
            prod_id = random.randint(1, PRODUCTS)
            qty = random.randint(1, 20)
            dt = random_date()
            batch_rows.append(f"({cust_id},{prod_id},{qty},'{dt}')")
            if len(batch_rows) == BATCH or i == ORDERS:
                f.write(
                    "INSERT INTO orders (customer_id, product_id, quantity, order_date) VALUES\n"
                    + ",\n".join(batch_rows) + ";\n"
                )
                batch_rows = []
                if i % 50000 == 0:
                    print(f"  orders: {i:,}/{ORDERS:,}")

        f.write("\nSET foreign_key_checks = 1;\n")

    print("Done!")

if __name__ == "__main__":
    main()
