import random
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd


ROWS = 500_000
OUTPUT = Path("data/raw/shipments.csv")

random.seed(42)

customers = [f"CUST{n:06d}" for n in range(1, 50_001)]

warehouses = {
    "WH001": "North",
    "WH002": "South",
    "WH003": "East",
    "WH004": "West",
    "WH005": "Central",
}

carriers = [
    "DHL",
    "FedEx",
    "UPS",
    "BlueDart",
    "Delhivery",
]

statuses = [
    "Delivered",
    "In Transit",
    "Delayed",
    "Cancelled",
]

start_date = datetime(2026, 1, 1)

rows = []

for i in range(1, ROWS + 1):

    shipment_date = start_date + timedelta(
        days=random.randint(0, 180)
    )

    expected_days = random.randint(2, 7)

    expected_delivery = shipment_date + timedelta(
        days=expected_days
    )

    status = random.choices(
        statuses,
        weights=[70, 15, 10, 5],
        k=1,
    )[0]

    if status == "Cancelled":
        actual_delivery = None

    elif status == "In Transit":
        actual_delivery = None

    else:
        delay_days = (
            random.randint(1, 5)
            if status == "Delayed"
            else random.randint(-1, 1)
        )

        actual_delivery = expected_delivery + timedelta(
            days=delay_days
        )

    weight_kg = round(
        random.uniform(0.5, 50.0),
        2,
    )

    shipping_cost = round(
        50
        + (weight_kg * random.uniform(8, 20))
        + random.uniform(0, 100),
        2,
    )

    rows.append(
        {
            "shipment_id": f"SHP{i:08d}",
            "customer_id": random.choice(customers),
            "warehouse_id": random.choice(
                list(warehouses.keys())
            ),
            "carrier": random.choice(carriers),
            "shipment_date": shipment_date.date(),
            "expected_delivery_date": expected_delivery.date(),
            "actual_delivery_date": (
                actual_delivery.date()
                if actual_delivery
                else None
            ),
            "status": status,
            "package_weight_kg": weight_kg,
            "shipping_cost": shipping_cost,
            "destination_region": random.choice(
                list(warehouses.values())
            ),
        }
    )

df = pd.DataFrame(rows)

OUTPUT.parent.mkdir(
    parents=True,
    exist_ok=True,
)

df.to_csv(
    OUTPUT,
    index=False,
)

print(f"Created {len(df):,} shipment records")
print(f"Saved to: {OUTPUT}")
print(df.head())
