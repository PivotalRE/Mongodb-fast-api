import csv
import random
from faker import Faker

fake = Faker()
RECORD_COUNT = 2000
OUTPUT_FILE = "owners_stress_test.csv"
EDGE_CASE_RATE = 0.15
FORCED_DUPLICATE_APN = "1234567890"

UPLOAD_SOURCES = ["REISift", "Stewart Title", "County Records", "Manual Entry"]
LISTS = ["List A", "List B", "List C", "VIP Clients", "High Risk", "Pre-foreclosure"]
DOMAINS = ["example.com", "test.com", "fake.org"]

def generate_apn():
    return f"{random.randint(1000000000, 9999999999)}"

def inject_edge_cases(record):
    case = random.choice([1, 2, 3, 4, 5])
    is_forced_duplicate = False

    if case == 1:  # Missing key fields
        record[random.choice(["apn", "full_name", "mailing_street"])] = ""

    elif case == 2:  # Invalid state or zip
        record["mailing_state"] = random.choice(["XX", "Washington", "WA-WA"])
        record["mailing_zip"] = random.choice(["00000", "ABCDE", "98001-12345"])

    elif case == 3:  # Special characters in name/address
        record["full_name"] = random.choice([
            "María Doñe-Smith (CEO)", "张伟", "O'Connor–Johnson"
        ])
        record["mailing_street"] = "123 Ümlaut Ln #" + str(random.randint(1, 100))

    elif case == 4:  # Complex valid address
        record["mailing_street"] = f"{random.randint(1,99999)}th Ave NE Apt {random.randint(1,5000)}"
        record["mailing_zip"] = f"{random.randint(98001, 99403)}-{random.randint(1000,9999)}"

    elif case == 5:  # Force duplicate
        record.update({
            "apn": FORCED_DUPLICATE_APN,
            "full_name": "FORCED DUPLICATE OWNER",
            "mailing_street": "123 Duplicate Ln",
            "mailing_city": "Seattle",
            "mailing_state": "WA",
            "mailing_zip": "98001"
        })
        is_forced_duplicate = True

    return record, is_forced_duplicate

def generate_record(is_edge_case):
    first = fake.first_name()
    last = fake.last_name()

    record = {
        "apn": generate_apn(),
        "full_name": f"{first} {last}",
        "first_name": first,
        "last_name": last,
        "mailing_street": fake.street_address(),
        "mailing_city": fake.city(),
        "mailing_state": "WA",
        "mailing_zip": fake.postcode_in_state("WA"),
        "emails": "|".join([
            f"{first.lower()}.{last.lower()}@{random.choice(DOMAINS)}",
            f"{last.lower()}{random.randint(10,99)}@{random.choice(DOMAINS)}"
        ][:random.randint(1,2)]),
        "upload_sources": "|".join(random.sample(UPLOAD_SOURCES, random.randint(1,2))),
        "lists": "|".join(random.sample(LISTS, random.randint(1,3))),
        "score": random.randint(50, 100)
    }

    if random.random() < 0.05:
        middle = fake.first_name()
        record["full_name"] = f"{first} {middle[0]}. {last}"
        record["first_name"] = f"{first} {middle[0]}."

    if is_edge_case:
        return inject_edge_cases(record)
    return record, False

# Main generation
records = []
hashes = set()
edge_case_count = int(RECORD_COUNT * EDGE_CASE_RATE)
forced_duplicates = 0

while len(records) < RECORD_COUNT:
    is_edge = len(records) < edge_case_count
    record, is_dup = generate_record(is_edge)

    dedup_hash = hash(frozenset({
        k: v for k, v in record.items()
        if k in ["apn", "full_name", "mailing_street", "mailing_city", "mailing_state", "mailing_zip"]
    }.items()))

    if is_dup:
        records.append(record)
        forced_duplicates += 1
    elif dedup_hash not in hashes:
        hashes.add(dedup_hash)
        records.append(record)

random.shuffle(records)
records = records[:RECORD_COUNT]

# Final write
with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=records[0].keys())
    writer.writeheader()
    writer.writerows(records)

print(f"""
Generated {len(records)} owner records
- Normal: {RECORD_COUNT - edge_case_count}
- Edge cases: {edge_case_count}
  - Forced duplicates: {forced_duplicates}
""")
