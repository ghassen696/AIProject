"""
config.py: Configuration constants for Kafka and keylogger settings.
"""
import socket

# Kafka configuration
#KAFKA_BROKERS = ['11.247.150.75:9092'] #Kafka broker addresses
KAFKA_BROKERS = ['localhost:9092'] #Kafka broker addresses
KAFKA_TOPIC = 'logs_event'
HOSTNAME = socket.gethostname()

MAX_BUFFER_SIZE = 500                # Flush buffer when this many keystrokes are collected
IDLE_FLUSH_SECONDS = 60
# config.pys 
PAUSE_DURATION_LIMITS = {
    "lunch": 60 * 60,        # 1 hour
    "bathroom": 15 * 60,     # 15 minutes
    "phone_call": 10 * 60,   # 10 minutes
    "meeting": 2 * 60 * 60,  # 2 hours
    "personal_break": 15 * 60      # 15 minutes
}

REASONS = [
    "Personal break",
    "Lunch",
    "Meeting",
    "Phone call",
    "other",
    "Bathroom",
]
VK_NUMPAD_MAP  = {
    96: '0',    
    97: '1',
    98: '2',
    99: '3',
    100: '4',
    101: '5',
    102: '6',
    103: '7',
    104: '8',
    105: '9',
    110: '.', 
    111: '/', 
    106: '*',  
    109: '-',  
    107: '+',  
}
# Pause/resume dialog settings
PAUSE_DIALOG_TITLE = "Pause Logging"
PAUSE_REASON_PROMPT = "Enter reason for pausing:"
PAUSE_DURATION_PROMPT = "Enter duration of pause (minutes):"
# System tray icon settings
TRAY_ICON_TITLE = "Employee Activity Monitor"

SENSITIVE_KEYWORDS = ["password", "login", "signin", "auth","mot de passe"]
