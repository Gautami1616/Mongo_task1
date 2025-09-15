from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.errors import DuplicateKeyError
from bson import ObjectId
from datetime import datetime
import re
from pprint import pprint

import warnings
warnings.filterwarnings("ignore")

#Task-1 : Connection and prep of data

#conditions to check if the email is in correct format
email = re.compile(r"^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$", re.I)
#condition to verify if the user is inserting the dept from the below mentioned departents
depts = {"Engineering", "AI/ML", "Finance", "HR", "Sales"}

#connection to mongodb
client = MongoClient("mongodb://localhost:27017/")
db = client["training_db"]
employees = db["employees"]

employees.drop()

# inserting documents in the form of dictionary
docs = [
        {
            "name": "Gautami Kadam",
            "email": "gautami.kadam@gamil.com",
            "department": "Engineering",
            "salary": 85000.0,
            "join_date": datetime(2023, 8, 1),
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow(),
        },
        {
            "name": "Raksha Pathak",
            "email": "raksha.pathak@gmail.com",
            "department": "AI/ML",
            "salary": 120000.0,
            "join_date": datetime(2024, 1, 15),
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow(),
        },
        {
            "name": "Mohit Jain",
            "email": "mohit.jain@gmail.com",
            "department": "Finance",
            "salary": 70000.0,
            "join_date": datetime(2022, 11, 10),
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow(),
        },
        {
            "name": "Neha Sharma",
            "email": "neha.sharma@gmail.com",
            "department": "HR",
            "salary": 60000.0,
            "join_date": datetime(2021, 6, 25),
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow(),
        },
        {
            "name": "Sanjay Gupta",
            "email": "sanjay.gupta@gmail.com",
            "department": "Sales",
            "salary": 90000.0,
            "join_date": datetime(2023, 3, 9),
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow(),
        },
    ]
employees.insert_many(docs)

#Task-2 : Search

# Task -2 1.->List All Records
for doc in employees.find({}):
    pprint(doc)

# Task 2- 2.->search the documents by department
for doc in employees.find({"department": "Engineering"}):
    pprint(doc)

#Task-2 3.-> search by salary range
for doc in employees.find({"salary": {"$gte": 65000, "$lte": 100000}}):
    pprint(doc)

#Task-2 4.->Search by Name Pattern
for doc in employees.find({"name": {"$regex": "sha", "$options": "i"}}):
    pprint(doc)

#Task-2 5.->Advanced Search with multiple criteria
query = {"department": "Finance", "salary": {"$gte": 60000, "$lte": 80000}}
for doc in employees.find(query):
    pprint(doc)

#Task-2 6.->sorted result
for doc in employees.find({}).sort("salary", ASCENDING).limit(3):
    pprint(doc)

#Task-2 7.->pagination (limit and skip)
page, page_size = 1, 2
skip = (page - 1) * page_size
for doc in employees.find({}).sort("salary", DESCENDING).skip(skip).limit(page_size):
    pprint(doc)

#Task-3 : Insert

#Task-3 1.-> single Insert
d1 = {
    "name": "Sayali Bhoir",
    "email": "sayali.bhoir@company.com",
    "department": "AI/ML",
    "salary": 110000,
    "join_date": "2024-07-01",
}

#Task-3 1.-> single Insert
name = (d1.get("name") or "").strip()
new_email = (d1.get("email") or "").strip().lower()
department = (d1.get("department") or "").strip()
salary = float(d1.get("salary"))
join_date = d1.get("join_date")

if not name:
    raise ValueError("name is required")
if not email.match(new_email):
    raise ValueError("invalid email format")
if department not in depts:
    raise ValueError(f"department must be one of {sorted(depts)}")
if salary <= 0:
    raise ValueError("salary must be > 0")
if isinstance(join_date, str):
    join_date = datetime.strptime(join_date, "%Y-%m-%d")
if not isinstance(join_date, datetime):
    raise ValueError("join_date must be datetime or 'YYYY-MM-DD'")

payload = {
    "name": name,
    "email": email,
    "department": department,
    "salary": salary,
    "join_date": join_date,
    "created_at": datetime.utcnow(),
    "updated_at": datetime.utcnow(),
}
try:
    res = employees.insert_one(payload)
    print("Inserted _id:", res.inserted_id)
except DuplicateKeyError:
    print("Duplicate")
except Exception as e:
    print("Insert error:", e)

#Task-3 2.-> multiple docs Insert
bulk_docs = [
    {
        "name": "abc",
        "email": "gautami.kadam@company.com",
        "department": "Sales",
        "salary": 50000,
        "join_date": "2024-07-02",
    },
    {
        "name": "xyz",
        "email": "xyz@company.com",
        "department": "Ops",
        "salary": 45000,
        "join_date": "2024-07-02",
    },
    {
        "name": "ghi",
        "email": "ghi@company.com",
        "department": "Engineering",
        "salary": 95000,
        "join_date": "2022-04-05",
    },
    {
        "name": "jkl",
        "email": "jkl@company.com",
        "department": "HR",
        "salary": 62000,
        "join_date": datetime(2021, 12, 30),
    },
]

#Task-3 2.-> multiple doc Insert
cleaned_docs = []
seen_emails = set()
for d in bulk_docs:
    try:
        nm = (d.get("name") or "").strip()
        em = (d.get("email") or "").strip().lower()
        dept = (d.get("department") or "").strip()
        sal = float(d.get("salary"))
        jd = d.get("join_date")

        if not nm:
            raise ValueError("name is required")
        if not email.match(em):
            raise ValueError("invalid email format")
        if dept not in depts:
            raise ValueError("department must be one of {sorted(depts)}")
        if sal <= 0:
            raise ValueError("salary must be > 0")
        if isinstance(jd, str):
            jd = datetime.strptime(jd, "%Y-%m-%d")
        if not isinstance(jd, datetime):
            raise ValueError("join_date must be datetime or 'YYYY-MM-DD'")

        if em in seen_emails:
            print("Duplicate")
            continue

        cleaned_docs.append({
            "name": nm,
            "email": em,
            "department": dept,
            "salary": sal,
            "join_date": jd,
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow(),
        })
        seen_emails.add(em)
    except Exception as e:
        print("Skipping", e)

#Task-3 2.-> multiple doc Insert
if cleaned_docs:
    try:
        employees.insert_many(cleaned_docs, ordered=False)
    except DuplicateKeyError:
        print("duplicates exist.")
else:
    print("No docs to insert.")

for doc in employees.find({}).sort("name", ASCENDING):
    pprint(doc)

#Task-4 : Update

# Task-4 1.-> Update Single Field
res = employees.update_one(
    {"_id": ObjectId('68c7c8762bff4fd3161358c9')},
    {"$set": {"department": "HR", "updated_at": datetime.utcnow()}}
    )
print("done")
pprint(employees.find_one({"_id": ObjectId('68c7c8762bff4fd3161358c9')}))

# Task-4 2.-> Update Multiple Fields
updates = {"salary": 88000, "name": "Rahul K. Verma", "updated_at": datetime.utcnow()}
res = employees.update_one(
    {"_id": ObjectId('68c7c8762bff4fd3161358c7')},
    {"$set": updates})
print("done")
pprint(employees.find_one({"_id": ObjectId('68c7c8762bff4fd3161358c7')}))

# Task-4 3.-> Update by ID
res = employees.update_one(
    {"_id": ObjectId('68c7c8762bff4fd3161358c5')},
    {"$set": {"salary": 90000, "updated_at": datetime.utcnow()}}
    )
print("done")
pprint(employees.find_one({"_id": ObjectId('68c7c8762bff4fd3161358c5')}))

#Task-4 4.-> update by criteria
res = employees.update_many(
    {"department": "HR"},
    {"$set": {"salary": 65000, "updated_at": datetime.utcnow()}})
print("done")
for doc in employees.find({"department": "HR"}):
    pprint(doc)

#Task-4 5.-> conditional update
res = employees.update_many(
    {"salary": {"$lt": 70000}},
    {"$set": {"needs_raise": True, "updated_at": datetime.utcnow()}})
print("done")
for doc in employees.find({"salary": {"$lt": 70000}}):
    pprint(doc)

