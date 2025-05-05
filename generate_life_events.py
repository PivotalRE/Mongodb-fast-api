import csv
import random
from faker import Faker
from datetime import datetime, timedelta, timezone

fake = Faker()
RECORD_COUNT = 2000
EDGE_CASE_RATE = 0.15
OUTPUT_FILE = "life_events_stress_test.csv"

EVENT_TYPES = [
    "marriage", "death", "trust_creation", "divorce", "ownership_transfer",
    "bankruptcy", "lien", "judgment", "probate", "quit_claim_deed"
]

def generate_life_event_record(is_edge_case=False):
    record = {
        "apn": f"{random.randint(1000000000, 9999999999)}",  # 10-digit APN
        "event_type": random.choice(EVENT_TYPES),
        "event_date": (datetime.now(timezone.utc) - timedelta(days=random.randint(0, 3650))).strftime('%Y-%m-%d'),
        "description": fake.sentence(nb_words=10),
        "documents": "|".join([fake.file_name(extension='pdf') for _ in range(random.randint(1, 3))]),
        "last_updated": datetime.now(timezone.utc).isoformat()
    }

    if is_edge_case:
        case = random.choice([1, 2, 3, 4, 5])
        if case == 1:
            # Missing required field & invalid date
            record.pop("event_type", None)
            record["event_date"] = "not_a_date"
        elif case == 2:
            # Null and empty values
            record["event_date"] = ""
            record["documents"] = None
        elif case == 3:
            # Corrupt documents field
            record["documents"] = "|||"
        elif case == 4:
            # Very large description
            record["description"] = fake.text(max_nb_chars=2000)
        elif case == 5:
            # Wrong types
            record["event_type"] = 123
            record["apn"] = True

    return record

def generate_csv():
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as csvfile:
        fieldnames = ["apn", "event_type", "event_date", "description", "documents", "last_updated"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for i in range(RECORD_COUNT):
            is_edge = random.random() < EDGE_CASE_RATE
            record = generate_life_event_record(is_edge_case=is_edge)
            writer.writerow(record)

    print(f"Generated {RECORD_COUNT} life event records in {OUTPUT_FILE}")

if __name__ == "__main__":
    generate_csv()
