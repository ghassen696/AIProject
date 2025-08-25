from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConnectionError, TransportError, NotFoundError, RequestError
import bcrypt
import re
import logging
from datetime import datetime, timedelta,timezone
from config import ELASTICSEARCH_URL

# Initialize Elasticsearch and logger
es = Elasticsearch(ELASTICSEARCH_URL)
logger = logging.getLogger(__name__)

# ------------------------- Password Hashing -------------------------

def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

def check_password(password: str, hashed: str) -> bool:
    return bcrypt.checkpw(password.encode('utf-8'), hashed.encode('utf-8'))

# ------------------------- Helpers -------------------------

def get_user_doc_by_username(username: str):
    query = {"query": {"term": {"username.keyword": username}}}
    try:
        res = es.search(index="users", body=query)
        hits = res.get('hits', {}).get('hits', [])
        return hits[0] if hits else None
    except (ConnectionError, TransportError, NotFoundError, RequestError) as e:
        logger.error(f"Error retrieving user by username {username}: {e}")
        return None

def get_user_doc_by_email(email: str):
    query = {"query": {"term": {"email.keyword": email}}}
    try:
        res = es.search(index="users", body=query)
        hits = res.get('hits', {}).get('hits', [])
        return hits[0] if hits else None
    except (ConnectionError, TransportError, NotFoundError, RequestError) as e:
        logger.error(f"Error retrieving user by email {email}: {e}")
        return None

def user_exists(username: str) -> bool:
    return get_user_doc_by_username(username) is not None

def email_exists(email: str) -> bool:
    return get_user_doc_by_email(email) is not None

# ------------------------- CRUD Operations -------------------------

def create_user(username: str, password: str, role: str = "user", employee_id: str = None, email: str = None, token: str = None, token_expiry_minutes: int = 120):
    if user_exists(username) or email_exists(email):
        return False  # Username or email already exists

    token_expiry = datetime.now(timezone.utc) + timedelta(minutes=token_expiry_minutes)
    print("Token expirsy:", token_expiry)


    user_doc = {
        "username": username,
        "password_hash": hash_password(password),
        "role": role,
        "email": email,
        "status": "active" if not token else "pending",
        "employee_id": employee_id,
        "token": token,
        "token_expiry": token_expiry.isoformat() if token else None
    }
    if token:
        user_doc["token"] = token
        user_doc["token_expiry"] = token_expiry.isoformat()

    try:
        es.index(index="users", document=user_doc)
        es.indices.refresh(index="users")
        return True
    except (ConnectionError, TransportError, NotFoundError, RequestError) as e:

        logger.error(f"Error creating user {username}: {e}")
        return False

def get_user(username: str):
    doc = get_user_doc_by_username(username)
    if doc:
        user = doc['_source']
        user['_id'] = doc['_id'] 
        return user
    return None


def get_all_users():
    try:
        res = es.search(index="users", size=1000, body={"query": {"match_all": {}}})
        users = []
        for hit in res['hits']['hits']: 
            src = hit['_source']
            src['_id'] = hit['_id']  # Add this line

            users.append({
                "_id": hit['_id'], 

                "username": src.get("username"),
                "role": src.get("role"),
                "employee_id": src.get("employee_id", ""),
                "email": src.get("email", "N/A"),
                "status": src.get("status", "Unknown")  # ✅ Added status

            })
        return users
    except (ConnectionError, TransportError, NotFoundError, RequestError) as e:

        logger.error(f"Error fetching users: {e}")
        return []

def update_user(username, new_password=None, new_role=None, new_employee_id=None, email=None):
    doc = get_user_doc_by_username(username)
    if not doc:
        return False

    doc_id = doc['_id']
    update_fields = {}

    if new_password:
        update_fields["password_hash"] = hash_password(new_password)
    if new_role:
        update_fields["role"] = new_role
    if new_employee_id is not None:
        update_fields["employee_id"] = new_employee_id
    if email is not None and email != doc['_source'].get("email"):
        if email_exists(email):
            logger.warning(f"Email {email} already used.")
            return False
        update_fields["email"] = email

    if not update_fields:
        return True  # Nothing to update

    try:
        es.update(index="users", id=doc_id, body={"doc": update_fields})
        return True
    except (ConnectionError, TransportError, NotFoundError, RequestError) as e:

        logger.error(f"Error updating user {username}: {e}")
        return False

def delete_user(username):
    doc = get_user_doc_by_username(username)
    if not doc:
        return False

    doc_id = doc['_id']
    try:
        es.delete(index="users", id=doc_id)
        es.indices.refresh(index="users")

        return True
    except (ConnectionError, TransportError, NotFoundError, RequestError) as e:

        logger.error(f"Error deleting user {username}: {e}")
        return False
def get_employee_logs(employee_id, date, index="employee-activity-logs"):
    """
    Fetch logs for a given employee on a specific date.
    """
    query = {
        "query": {
            "bool": {
                "must": [
                    {"match": {"employee_id": employee_id}},
                    {"range": {"timestamp": {"gte": f"{date}T00:00:00", "lte": f"{date}T23:59:59"}}}
                ]
            }
        },
        "size": 10000,  # adjust if you have >10000 logs
        "sort": [{"timestamp": {"order": "asc"}}]
    }
    res = es.search(index=index, body=query)
    return [hit["_source"] for hit in res["hits"]["hits"]]

# ------------------------- Token Validation -------------------------

def is_token_valid(token: str) -> bool:
    query = {"query": {"term": {"token.keyword": token}}}
    try:
        res = es.search(index="users", body=query)
        hits = res.get("hits", {}).get("hits", [])
        if not hits:
            return False
        doc = hits[0]['_source']
        expiry_str = doc.get("token_expiry")
        if not expiry_str:
            return False
        expiry = datetime.fromisoformat(expiry_str)
        return datetime.now(timezone.utc) < expiry
    except Exception as e:
        logger.error(f"Error checking token: {e}")
        return False

# ------------------------- Password Strength -------------------------

def check_password_strength(password: str) -> list:
    errors = []
    if len(password) < 8:
        errors.append("Password must be at least 8 characters long.")
    if not re.search(r"[A-Z]", password):
        errors.append("Must contain at least one uppercase letter.")
    if not re.search(r"[a-z]", password):
        errors.append("Must contain at least one lowercase letter.")
    if not re.search(r"\d", password):
        errors.append("Must contain at least one digit.")
    if not re.search(r"[@$!%*?&]", password):
        errors.append("Must contain at least one special character (@$!%*?&).")
    return errors
