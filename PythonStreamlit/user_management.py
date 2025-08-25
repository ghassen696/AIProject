import uuid
from datetime import datetime
from elasticsearch import Elasticsearch
import smtplib
from email.message import EmailMessage
import urllib.parse #for incoding the token
from datetime import datetime, timedelta, timezone


es = Elasticsearch("http://localhost:9200")

token_expiry = datetime.now(timezone.utc) + timedelta(hours=2)

def generate_token():
    return str(uuid.uuid4())

def invite_user(email, employee_id, role,username):
    query = {"query": {"term": {"email.keyword": email}}}
    res = es.search(index="users", body=query)
    if res["hits"]["total"]["value"] > 0:
        return False  
    
    token = generate_token()
    user_doc = {
        "username":username,
        "email": email,
        "employee_id": employee_id,
        "role": role,
        "status": "pending",
        "token": token,
        "created_at": datetime.utcnow().isoformat(),
        "token_expiry": token_expiry.isoformat(),

    }
    es.index(index="users", body=user_doc)
    return token  


def send_invite_email(to_email, token):
    link = f"http://localhost:8501/?token={urllib.parse.quote(token)}"
    msg = EmailMessage()
    msg['Subject'] = "You're invited to set your password"
    msg['From'] = "huaweitesttest1@gmail.com"
    msg['To'] = to_email
    msg.set_content(f"Click the link to set your password:\n{link}")

    with smtplib.SMTP('smtp.gmail.com', 587) as server:
        server.starttls()
        server.login("huaweitesttest1@gmail.com", "ordf ylso cjkm koiq")
        server.send_message(msg)
