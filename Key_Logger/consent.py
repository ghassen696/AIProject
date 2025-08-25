# consent.py
import tkinter as tk
from tkinter import messagebox
from datetime import datetime
import logger
import config
def get_user_consent(keylogger):
    root = tk.Tk()
    root.withdraw()  # hide the main window
    consent = messagebox.askyesno(
        "Consent Required",
        "This application will log your keystrokes, active windows, and idle periods for performance monitoring.\n"
        "Data is securely sent to a Kafka pipeline for analysis.\n"
        "Do you agree to proceed?"
    )
    root.destroy()
    keylogger.seq_num += 1
    # Log consent decision to Kafka
    event = {
        "timestamp": datetime.now().isoformat(),
        "event": "consent_given" if consent else "consent_declined",
        "employee_id": config.HOSTNAME,
        "session_id": keylogger.session_id,
        "seq_num": keylogger.seq_num
    }
    logger.log_event(event)

    return consent
