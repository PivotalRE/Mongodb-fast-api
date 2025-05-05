import csv
import random
from faker import Faker
from datetime import datetime, timedelta

fake = Faker()
RECORD_COUNT = 2000
EDGE_CASE_RATE = 0.15
OUTPUT_FILE = "phones_stress_test.csv"
FORCED_DUPLICATE_NUMBER = "+12065551234"

SOURCES = ["Tracers", "Versium", "TLO", "Whitepages", "Manual Entry"]
STATUSES = ["valid", "invalid", "pending", "unverified"]

TAGS = ["skip", "high_priority", "needs_review", "ai_flagged"]

def generate_clean_number():
    """Generate clean 10/11-digit phone numbers, optionally with +1 prefix"""
    digits = f"{random.randint(200,999)}{random.randint(200,999)}{random.randint(1000,9999)}"
    return f"+1{digits}"

def generate_owner_apn():
    """Generate 10-digit APN"""
    return f"{random.randint(0, 9999999999):010}"

def generate_phone_record(is_edge_case=False):
    record = {
        "number": generate_clean_number(),
        "owner_apn": generate_owner_apn(),
        "source": random.choice(SOURCES),
        "status": random.choice(STATUSES),
        "verified": random.choice([True, False]),
        "tags": "|".join(random.sample(TAGS, random.randint(0, 2))),
        "last_updated": datetime.now().isoformat()
    }

    if is_edge_case:
        case = random.choice([1, 2, 3, 4, 5])
        if case == 1:  # Missing required fields
            record["number"] = ""
        elif case == 2:  # Wrong types
            record["verified"] = random.choice(["true", "nope", 1])
        elif case == 3:  # Bad number format
            record["number"] = random.choice(["abc1234567", "(999)123-ABCD", "123-45-6789"])
        elif case == 4:  # Forced duplicate
            record["number"] = FORCED_DUPLICATE_NUMBER
        elif case == 5:  # Invalid tags array
            record["tags"] = "|||"
    
    return record

# Generate dataset
records = []
seen_numbers = set()
edge_case_count = int(RECORD_COUNT * EDGE_CASE_RATE)

while len(records) < RECORD_COUNT:
    is_edge = len(records) < edge_case_count
    rec = generate_phone_record(is_edge)

    if not is_edge:
        if rec["number"] not in seen_numbers and rec["number"]:
            seen_numbers.add(rec["number"])
            records.append(rec)
    else:
        records.append(rec)

# Write to CSV
with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=records[0].keys())
    writer.writeheader()
    writer.writerows(records)

print(f"""
 Generated {len(records)} phone records
- Edge cases: {edge_case_count}
- Normal records: {RECORD_COUNT - edge_case_count}
- Forced duplicate: {FORCED_DUPLICATE_NUMBER}
""")
