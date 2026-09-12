import random
from datetime import datetime, timedelta

import psycopg2


DB_NAME = "shipment_db"
DB_USER = "akash"
DB_HOST = "localhost"
DB_PORT = 5432


def connect():
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        host=DB_HOST,
        port=DB_PORT,
    )


def main():

    conn = connect()
    cur = conn.cursor()

    # Clean start
    cur.execute("""
        DROP TABLE IF EXISTS shipments;
        DROP TABLE IF EXISTS customers;
        DROP TABLE IF EXISTS warehouses;
        DROP TABLE IF EXISTS carriers;
    """)

    # -----------------------------
    # Customers
    # -----------------------------
    cur.execute("""
        CREATE TABLE customers (
            customer_id VARCHAR(20) PRIMARY KEY,
            customer_name VARCHAR(100) NOT NULL,
            email VARCHAR(150),
            city VARCHAR(100),
            region VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    # -----------------------------
    # Warehouses
    # -----------------------------
    cur.execute("""
        CREATE TABLE warehouses (
            warehouse_id VARCHAR(20) PRIMARY KEY,
            warehouse_name VARCHAR(100) NOT NULL,
            region VARCHAR(50) NOT NULL,
            capacity INTEGER NOT NULL
        );
    """)

    # -----------------------------
    # Carriers
    # -----------------------------
    cur.execute("""
        CREATE TABLE carriers (
            carrier_id SERIAL PRIMARY KEY,
            carrier_name VARCHAR(100) UNIQUE NOT NULL,
            service_level VARCHAR(50) NOT NULL
        );
    """)

    # -----------------------------
    # Shipments
    # -----------------------------
    cur.execute("""
        CREATE TABLE shipments (
            shipment_id VARCHAR(30) PRIMARY KEY,
            customer_id VARCHAR(20) NOT NULL,
            warehouse_id VARCHAR(20) NOT NULL,
            carrier_id INTEGER NOT NULL,
            shipment_date DATE NOT NULL,
            expected_delivery_date DATE NOT NULL,
            actual_delivery_date DATE,
            status VARCHAR(30) NOT NULL,
            package_weight_kg NUMERIC(10,2) NOT NULL,
            shipping_cost NUMERIC(12,2) NOT NULL,
            destination_region VARCHAR(50) NOT NULL,

            CONSTRAINT fk_customer
                FOREIGN KEY (customer_id)
                REFERENCES customers(customer_id),

            CONSTRAINT fk_warehouse
                FOREIGN KEY (warehouse_id)
                REFERENCES warehouses(warehouse_id),

            CONSTRAINT fk_carrier
                FOREIGN KEY (carrier_id)
                REFERENCES carriers(carrier_id)
        );
    """)

    # -----------------------------
    # Warehouse data
    # -----------------------------
    warehouses = [
        ("WH001", "North Distribution Center", "North", 100000),
        ("WH002", "South Distribution Center", "South", 120000),
        ("WH003", "East Distribution Center", "East", 110000),
        ("WH004", "West Distribution Center", "West", 95000),
        ("WH005", "Central Distribution Center", "Central", 150000),
    ]

    cur.executemany(
        """
        INSERT INTO warehouses
        (warehouse_id, warehouse_name, region, capacity)
        VALUES (%s, %s, %s, %s)
        """,
        warehouses,
    )

    # -----------------------------
    # Carrier data
    # -----------------------------
    carriers = [
        ("DHL", "Express"),
        ("FedEx", "Express"),
        ("UPS", "Standard"),
        ("BlueDart", "Express"),
        ("Delhivery", "Standard"),
    ]

    cur.executemany(
        """
        INSERT INTO carriers
        (carrier_name, service_level)
        VALUES (%s, %s)
        """,
        carriers,
    )

    # -----------------------------
    # Customer data
    # -----------------------------
    regions = ["North", "South", "East", "West", "Central"]

    customers = []

    for i in range(1, 50_001):

        customer_id = f"CUST{i:06d}"

        customers.append(
            (
                customer_id,
                f"Customer {i}",
                f"customer{i}@example.com",
                f"City_{random.randint(1, 100)}",
                random.choice(regions),
            )
        )

    cur.executemany(
        """
        INSERT INTO customers
        (customer_id, customer_name, email, city, region)
        VALUES (%s, %s, %s, %s, %s)
        """,
        customers,
    )

    # -----------------------------
    # Shipment data
    # -----------------------------
    statuses = [
        "Delivered",
        "In Transit",
        "Delayed",
        "Cancelled",
    ]

    warehouse_ids = [
        w[0] for w in warehouses
    ]

    carrier_ids = list(range(1, 6))

    start_date = datetime(2026, 1, 1)

    shipments = []

    for i in range(1, 500_001):

        shipment_date = (
            start_date
            + timedelta(
                days=random.randint(0, 180)
            )
        )

        expected_days = random.randint(2, 7)

        expected_delivery = (
            shipment_date
            + timedelta(days=expected_days)
        )

        status = random.choices(
            statuses,
            weights=[70, 15, 10, 5],
            k=1,
        )[0]

        if status in ["Cancelled", "In Transit"]:
            actual_delivery = None

        else:

            delay = (
                random.randint(1, 5)
                if status == "Delayed"
                else random.randint(-1, 1)
            )

            actual_delivery = (
                expected_delivery
                + timedelta(days=delay)
            )

        weight = round(
            random.uniform(0.5, 50),
            2,
        )

        cost = round(
            50
            + weight * random.uniform(8, 20)
            + random.uniform(0, 100),
            2,
        )

        shipments.append(
            (
                f"SHP{i:08d}",
                random.choice(
                    customers
                )[0],
                random.choice(
                    warehouse_ids
                ),
                random.choice(
                    carrier_ids
                ),
                shipment_date.date(),
                expected_delivery.date(),
                (
                    actual_delivery.date()
                    if actual_delivery
                    else None
                ),
                status,
                weight,
                cost,
                random.choice(regions),
            )
        )

        # Insert in batches
        if len(shipments) >= 10_000:

            cur.executemany(
                """
                INSERT INTO shipments (
                    shipment_id,
                    customer_id,
                    warehouse_id,
                    carrier_id,
                    shipment_date,
                    expected_delivery_date,
                    actual_delivery_date,
                    status,
                    package_weight_kg,
                    shipping_cost,
                    destination_region
                )
                VALUES (
                    %s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,%s
                )
                """,
                shipments,
            )

            conn.commit()

            print(
                f"Inserted {i:,} shipments"
            )

            shipments = []

    # Remaining records
    if shipments:

        cur.executemany(
            """
            INSERT INTO shipments (
                shipment_id,
                customer_id,
                warehouse_id,
                carrier_id,
                shipment_date,
                expected_delivery_date,
                actual_delivery_date,
                status,
                package_weight_kg,
                shipping_cost,
                destination_region
            )
            VALUES (
                %s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,%s
            )
            """,
            shipments,
        )

    conn.commit()

    print("\nDatabase setup completed successfully.")

    cur.execute(
        "SELECT COUNT(*) FROM customers"
    )
    print(
        "Customers:",
        cur.fetchone()[0]
    )

    cur.execute(
        "SELECT COUNT(*) FROM shipments"
    )
    print(
        "Shipments:",
        cur.fetchone()[0]
    )

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
