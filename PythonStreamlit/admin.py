from auth import create_user
from elasticsearch import Elasticsearch

es = Elasticsearch("http://localhost:9200")

# Change these as you want
username = "Admin"
password = "Huawei2025"  
role = "admin"

if not es.indices.exists(index="users"):
    es.indices.create(index="users")
    print("Created 'users' index.")
else:
    print("'users' index already exists.")

success = create_user(username, password, role)
if success:
        print(f"✅ Admin user '{username}' created successfully!")
else:
        print(f"⚠️ User '{username}' already exists.")
