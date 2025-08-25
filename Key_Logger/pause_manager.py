import threading
import tkinter as tk
from tkinter import messagebox, ttk
from datetime import datetime
import logger
import config

# Internal state
_paused = False
_pause_reason = None
_resume_timer = None
_keylogger_ref = None
# Per-reason duration limits (in minutes)
PAUSE_DURATION_LIMITS = {
    "Lunch": 60,
    "Personal": 15,
    "Meeting": 30,
    "Break": 10,
    # Add more as needed
}
DEFAULT_MAX_DURATION = 15  

def is_paused() -> bool:
    return _paused

def pause(duration_minutes: float, reason: str,keylogger=None):
    global _paused, _resume_timer, _pause_reason,_keylogger_ref
    if _paused:
        return  # Already paused

    _paused = True
    _pause_reason = reason
    _keylogger_ref = keylogger

    session_id = None
    seq_num = None
    if keylogger:
        keylogger.seq_num += 1
        seq_num = keylogger.seq_num
        session_id = keylogger.session_id    # Send pause event
    event = {
        "timestamp": datetime.now().isoformat(),
        "event": "pause",
        "reason": reason,
        "duration_minutes": duration_minutes,
        "employee_id": config.HOSTNAME,
        "session_id": session_id,
        "seq_num": seq_num
 
    }
    logger.log_event(event)
    logger.logger.info(f"Logging paused for {duration_minutes} minutes. Reason: {reason}")

    # Schedule automatic resume
    seconds = max(0, float(duration_minutes) * 60)
    if seconds > 0:
        _resume_timer = threading.Timer(seconds, resume)
        _resume_timer.start()

def resume(keylogger=None):
    global _paused, _resume_timer, _pause_reason,_keylogger_ref
    if not _paused:
        return

    _paused = False
    if _resume_timer:
        _resume_timer.cancel()
    keylogger = keylogger or _keylogger_ref

    session_id = None
    seq_num = None
    if keylogger:
        keylogger.seq_num += 1
        seq_num = keylogger.seq_num
        session_id = keylogger.session_id

    event = {
        "timestamp": datetime.now().isoformat(),
        "event": "resume",
        "employee_id": config.HOSTNAME,
        "session_id": session_id,
        "seq_num": seq_num


    }
    logger.log_event(event)
    logger.logger.info("Logging resumed.")
    _keylogger_ref = None


def show_pause_dialog(keylogger=None):
    """
    Display dialog to enter pause reason and duration, then pause logging.
    """
    def on_submit():
        reason = reason_entry.get()
        duration_text = duration_entry.get()

        if not reason:
            messagebox.showerror("Error", "Please select a reason.")
            return
        try:
            duration = float(duration_text)
        except ValueError:
            messagebox.showerror("Error", "Duration must be a number.")
            return

        if duration <= 0:
            messagebox.showerror("Error", "Duration must be greater than 0.")
            return

        # Enforce max duration
        max_allowed = PAUSE_DURATION_LIMITS.get(reason, DEFAULT_MAX_DURATION)
        if duration > max_allowed:
            messagebox.showerror("Limit Exceeded",
                                 f"The maximum allowed duration for '{reason}' is {max_allowed} minutes.")
            return

        pause(duration, reason ,keylogger)
        dialog.destroy()

    dialog = tk.Tk()
    dialog.title(config.PAUSE_DIALOG_TITLE)
    tk.Label(dialog, text="Reason:").grid(row=0, column=0, padx=10, pady=10)

    reason_entry = tk.StringVar()
    reason_dropdown = ttk.Combobox(dialog, textvariable=reason_entry, values=config.REASONS, state="readonly")
    reason_dropdown.grid(row=0, column=1, padx=10, pady=10)

    tk.Label(dialog, text="Duration (minutes):").grid(row=1, column=0, padx=10, pady=10)
    duration_entry = tk.Entry(dialog)
    duration_entry.grid(row=1, column=1, padx=10, pady=10)

    submit_btn = tk.Button(dialog, text="Pause", command=on_submit)
    submit_btn.grid(row=2, column=0, columnspan=2, pady=10)

    dialog.mainloop()
