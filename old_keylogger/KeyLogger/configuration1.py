# config.py

import socket




REASONS = [
    "Personal break",
    "Lunch",
    "Meeting",
    "Phone call",
    "Technical issue",
    "Bathroom",
]

FLUSH_INTERVAL = 10  # seconds
INACTIVITY_TIMEOUT = 5  # seconds
HOSTNAME = socket.gethostname()
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC = 'keylogger-logs'
ENABLE_KAFKA = True  


VK_NUMPAD_MAP = {
    96: '0', 97: '1', 98: '2', 99: '3', 100: '4', 101: '5',
    102: '6', 103: '7', 104: '8', 105: '9', 110: '.', 111: '/',
    106: '*', 109: '-', 107: '+',
}
