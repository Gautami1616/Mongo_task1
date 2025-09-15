from datetime import datetime
import re
import os
from dotenv import load_dotenv
from pprint import pprint

load_dotenv()
MONGO_URI = os.getenv('MONGO_URI', 'mongodb://localhost:27017/')
DB_NAME = os.getenv('DB_NAME', 'training_db')
COLLECTION_NAME = os.getenv('COLLECTION_NAME', 'employees')

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]


# Code for single insertion
def single_insert():
    try:
        collection.create_index([(" email", 1)], unique=True)
    except Exception as e:
        print("couldn't create index")
    try:
        emp_id = int(input("emp_id: "))
        name = input("name: ")
        email = input("email: ")
        department = input("department: ")
        salary = float(input("salary: "))
        join_date = datetime.datetime.strptime(input("join_date (YYYY-MM-DD): "), "%Y-%m-%d")

        if collection.count_documents({"email": email}, limit=1):
            print("duplicate email")
        else:
            collection.insert_one({
                "emp_id": emp_id,
                "name": name,
                "email": email,
                "department": department,
                "salary": salary,
                "join_date": join_date,
                "created_at": datetime.datetime.utcnow(),
                "updated_at": datetime.datetime.utcnow()
            })
            print("Inserted")
    except Exception as e:
        print("Error inserting document:", e)

# multiple insertion
def multiple_insert():
    n = int(input("how many employees to insert: "))
    docs = []
    seen = set()
    for i in range(n):
        emp_id = int(input("emp_id: "))
        name = input("name: ")
        email = input("email: ")
        department = input("department: ")
        salary = float(input("salary: "))
        join_date = datetime.datetime.strptime(input("join_date (YYYY-MM-DD): "), "%Y-%m-%d")

        if email in seen:
            print("duplicate email")
            continue

        docs.append({
            "emp_id": emp_id,
            "name": name,
            "email": email,
            "department": department,
            "salary": salary,
            "join_date": join_date,
            "created_at": datetime.datetime.utcnow(),
            "updated_at": datetime.datetime.utcnow()
        })
        seen.add(email)

    if docs:
        collection.insert_many(docs)
        print("Inserted")
    else:
        print("no documents to insert")

# code for reading/printing the documents
def read_doc():
    try:
        for doc in collection.find():
            print(doc)
    except Exception as e1:
        print("Error printing document:", e1)

# code for searching documents by departments
def search_by_dept():
    try:
        dept = input("enter the department: ")
        for doc in collection.find({"department": dept}):
            print(doc)
    except Exception as e:
        print("Error fetching the document:", e)

# code for searching documents by salary range
def search_by_salary_range():
    try:
        lower = float(input("enter the lower salary range: "))
        upper = float(input("enter the upper salary range: "))
        for doc in collection.find({"salary": {"$gte": lower, "$lte": upper}}):
            print(doc)
    except Exception as e:
        print("Error fetching the document:", e)

# code for searching documents by name pattern
def search_by_name_pattern():
    try:
        pat = input("enter the name to search: ")
        for doc in collection.find({"name": {"$regex": pat}}):
            print(doc)
    except Exception as e:
        print("Error fetching the document:", e)

# code for searching documents by department, salary
def advanced_search():
    try:
        salary = float(input("enter the salary to search: "))
        dept = input("enter the dept to search: ")
        for doc in collection.find({
            "$and": [{"salary": {"$eq": salary}}, 
                     {"department": {"$eq": dept}}]}):
            print(doc)
    except Exception as e:
        print("Error fetching the document:", e)

# pagination and sorting code
def read_by_page():
    try:
        page = int(input("page number: "))
        size = int(input("page size: "))
        field = input("sort field: ")
        order = int(input("for asc put 1 and for desc put -1: "))

        skip = (page - 1) * size

        cur = (collection.find()
                  .sort(field, order)
                  .skip(skip)
                  .limit(size))

        for doc in cur:
            print(doc)
    except Exception as e:
        print("Error fetching the document:", e)

# code for updating the field
def update_single_field():
    try:
        emp_id = int(input("enter the id to be updated: "))
        update_field = input("enter the field name to be updated: ")
        to_be_updated = input("enter the new update: ")

        result = collection.update_one(
            {"emp_id": emp_id}, 
            {"$set": {update_field: to_be_updated, "updated_at": datetime.datetime.utcnow()}}
            )
        
        print("matched and updated")
    except Exception as e:
        print("Error updating document:", e)

# code for updating multiple fields
def update_multiple_field():
    try:
        emp_id = int(input("enter the id to be updated: "))
        n = int(input("enter the number of updates to be done: "))
        updates = {}
        for i in range(n):
            update_field = input("enter the field to be updated: ")
            to_be_updated = input("enter the value to be updated: ")
            updates[update_field] = to_be_updated
        updates["updated_at"] = datetime.datetime.utcnow()
        result = collection.update_many(
            {"emp_id": emp_id}, 
            {"$set": updates}
            )
        
        print("matched and updated")

    except Exception as e:
        print("Error updating document:", e)


# code for updating document using id
def update_by_id():
    try:
        emp_id = int(input("enter the id to be updated: "))
        update_field = input("enter the field name to be updated: ")
        to_be_updated = input("enter the new update: ")

        result = collection.update_one(
            {"emp_id": emp_id}, 
            {"$set": {update_field: to_be_updated, "updated_at": datetime.datetime.utcnow()}}
            )
        
        print("matched and updated")
    except Exception as e:
        print("Error updating document:", e)

# code for updating document by criteria
def update_by_criteria():
    try:
        field = input("enter the field name to be updated: ")
        value = input("enter the new value: ")
        criteria_field = input("enter the criteria field: ")
        criteria_value = input("enter the criteria value: ")

        result = collection.update_many(
            {criteria_field: criteria_value}, 
            {"$set": {field: value, "updated_at": datetime.datetime.utcnow()}})
        
        print("matched and modified")
    except Exception as e:
        print("Error updating document:", e)

# code for updating document based on condition
def conditional_update():
    res = collection.update_many(
    {"salary": {"$lt": 70000}}, 
    {"$set": {"needs_raise": True, "updated_at": datetime.datetime.utcnow()}})
    print("done")
    for doc in collection.find({"salary": {"$lt": 70000}}):
        pprint(doc)

# menu to select from
if __name__ == "__main__":
    while True:
        print("\nMenu:")
        print("1. Insert Employees")
        print("2. List All Employees")
        print("3. Paginate Employees")
        print("4. Search Employees")
        print("5. Update Employees")
        print("6. Exit")
        choice = int(input("Enter your choice: "))
        if choice == 1:
            print("1. Insert Single Employees")
            print("2. Insert Multiple Employees")
            print("3. Exit")
            ch1 = int(input("Enter your choice: "))
            if ch1==1:
                single_insert()
            elif ch1==2:
                multiple_insert()
            elif ch1==3:
                break
        elif choice == 2:
            read_doc()
        elif choice == 3:
            read_by_page()
        elif choice == 4:
            print("1. Search by dept")
            print("2. Search by salary range")
            print("3. Search by name pattern")
            print("4. Advanced Search")
            print("5. Exit")
            ch2 = int(input("Enter your choice: "))
            if ch2==1:
                search_by_dept()
            elif ch2==2:
                search_by_salary_range()
            elif ch2==3:
                search_by_name_pattern()
            elif ch2==4:
                advanced_search()
            elif ch2==5:
                break
        elif choice == 5:
            print("1. Update single field")
            print("2. update multiple fields")
            print("3. Update by id")
            print("4. Update by criteria")
            print("5. Update based on condition")
            print("6. Exit")
            ch2 = int(input("Enter your choice: "))
            if ch2==1:
                update_single_field()
            elif ch2==2:
                update_multiple_field()
            elif ch2==3:
                update_by_id()
            elif ch2==4:
                update_by_criteria()
            elif ch2==5:
                conditional_update()
            elif ch2==6:
                break
        elif choice == 6:
            print("Exiting...")
            break
        else:
            print("Invalid choice")