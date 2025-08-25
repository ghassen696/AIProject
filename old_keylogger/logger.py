import threading
import time
import datetime
import json
from pynput import keyboard
try:
    import win32gui
    def get_active_window_title():
        hwnd = win32gui.GetForegroundWindow()
        return win32gui.GetWindowText(hwnd) if hwnd else None
except ImportError:
    import pywinctl as pw
    def get_active_window_title():
        w = pw.getActiveWindow()
        return w.title if w else None
    
import uiautomation as auto

def is_sensitive_input():
    try:
        ctrl = auto.GetFocusedControl()

        # Heuristic 1: Native password control
        if getattr(ctrl, 'IsPasswordControl', False):
            return True

        # Heuristic 2: Name contains password-related terms (English + French)
        name = (ctrl.Name or '').lower()
        if any(word in name for word in ['password', 'mot de passe', 'mdp']):
            return True

        # Heuristic 3: Class name, Automation ID, ControlType
        automation_id = (ctrl.AutomationId or '').lower()
        control_type = (ctrl.ControlTypeName or '').lower()
        class_name = (ctrl.ClassName or '').lower()
        if any(word in val for val in [automation_id, control_type, class_name]
               for word in ['password', 'motdepasse', 'mdp']):
            return True

        return False
    except Exception:
        return False

    
def is_known_sensitive_app():
    title = get_active_window_title().lower()
    return any(keyword in title for keyword in ['sign in','inscription','connexion'])

import tkinter as tk
from tkinter import messagebox
import pystray
from PIL import Image, ImageDraw


LOG_FILE = 'Logs/activity_logs.txt'
stop_event = threading.Event()
paused = threading.Event()
pause_info = {}
tray_icon = None

# Prompt for employee identifier
def get_employee_id():
    emp = input("Enter employee ID (e.g., emp001): ").strip()
    return emp or 'unknown'

text_buffer = []
buffer_lock = threading.Lock()
sensitive_mode = False

# Logging helper
def log_event(event_type, text=None, window_name=None, employee_id=None, extra=None):
    if paused.is_set() and event_type not in ('pause_start','pause_end'):
        return
    event = {'timestamp': datetime.datetime.utcnow().isoformat()+'Z',
             'event_type': event_type,
             'text': text, 'window_name': window_name,
             'employee_id': employee_id}
    if extra: event.update(extra)
    with open(LOG_FILE,'a',encoding='utf-8') as f:
        f.write(json.dumps(event)+"\n")

# Flush buffer
def flush_buffer(employee_id, window_name):
    global text_buffer
    with buffer_lock:
        if text_buffer:
            combined=''.join(text_buffer)
            log_event('text_input', text=combined, window_name=window_name, employee_id=employee_id)
            text_buffer=[]

# Updated pause and tray icon logic
from tkinter import ttk

# Advanced pause window
def show_pause_dialog(icon):
    def submit():
        reason = reason_entry.get().strip()
        try:
            minutes = int(duration_entry.get())
        except ValueError:
            messagebox.showerror("Invalid Input", "Duration must be a number")
            return
        if not reason:
            messagebox.showerror("Missing Reason", "Please enter a reason for pause.")
            return
        top.destroy()
        start_pause(reason, minutes, icon)

    top = tk.Tk()
    top.title("Pause Logging")
    top.geometry("300x150")
    tk.Label(top, text="Reason for Pause:").pack(pady=(10, 0))
    reason_entry = ttk.Entry(top, width=40)
    reason_entry.pack()
    tk.Label(top, text="Duration (minutes):").pack(pady=(10, 0))
    duration_entry = ttk.Entry(top, width=10)
    duration_entry.pack()
    ttk.Button(top, text="Pause", command=submit).pack(pady=10)
    top.mainloop()

def start_pause(reason, duration, icon):
    if paused.is_set():
        return
    paused.set()
    start = datetime.datetime.utcnow()
    pause_info['start'] = start.isoformat() + 'Z'
    pause_info['reason'] = reason
    log_event('pause_start', window_name=get_active_window_title(), employee_id=eid, extra={'pause_info': pause_info})
    icon.menu = pystray.Menu(pystray.MenuItem('Resume Logging', lambda: resume_logging(icon)))
    icon.update_menu()

    def auto_resume():
        if paused.is_set():
            resume_logging(icon)
    threading.Timer(duration * 60, auto_resume).start()

def resume_logging(icon):
    if not paused.is_set():
        return
    paused.clear()
    end = datetime.datetime.utcnow()
    pause_info['end'] = end.isoformat() + 'Z'
    log_event('pause_end', window_name=get_active_window_title(), employee_id=eid, extra={'pause_info': pause_info})
    icon.menu = pystray.Menu(pystray.MenuItem('Pause Logging', lambda: show_pause_dialog(icon)))
    icon.update_menu()

# Create dynamic tray icon
def create_icon():
    global tray_icon
    image = Image.new('RGB', (64, 64), 'white')
    d = ImageDraw.Draw(image)
    d.rectangle((16, 16, 48, 48), fill='black')

    icon = pystray.Icon('ActivityLogger', image, 'Logger')
    icon.menu = pystray.Menu(pystray.MenuItem('Pause Logging', lambda: show_pause_dialog(icon)))
    tray_icon = icon

    threading.Thread(target=tray_icon.run, daemon=True).start()



# Keyboard handlers
def on_press(key, employee_id):
    global sensitive_mode
    win=get_active_window_title()
    if is_sensitive_input() or is_known_sensitive_app():

        if not sensitive_mode:
            sensitive_mode=True
            flush_buffer(employee_id,win)
            log_event('sensitive_field_start', window_name=win, employee_id=employee_id)
        return
    else:
        if sensitive_mode:
            sensitive_mode=False
            log_event('sensitive_field_end', window_name=win, employee_id=employee_id)
    try: char=key.char
    except: char=None
    if char:
        with buffer_lock: text_buffer.append(char)
    if key in (keyboard.Key.space,keyboard.Key.enter):
        with buffer_lock: text_buffer.append(' ' if key==keyboard.Key.space else '\n')
        flush_buffer(employee_id,win)

def on_release(key, employee_id):
    if key==keyboard.Key.esc:
        stop_event.set(); flush_buffer(employee_id,get_active_window_title()); return False

# Window and flush threads
def track_active_window(employee_id,interval):
    last=None
    while not stop_event.is_set():
        cur=get_active_window_title()
        if cur!=last:
            flush_buffer(employee_id,cur)
            log_event('window_change', window_name=cur, employee_id=employee_id)
            last=cur
        time.sleep(interval)

def periodic_flush(employee_id,interval):
    while not stop_event.is_set():
        time.sleep(interval)
        flush_buffer(employee_id,get_active_window_title())

# Main
if __name__=='__main__':
    eid=get_employee_id(); print(f"Logging to {LOG_FILE} for '{eid}'. ESC to stop.")
    create_icon()
    threading.Thread(target=track_active_window,args=(eid,3),daemon=True).start()
    threading.Thread(target=periodic_flush,args=(eid,5),daemon=True).start()
    from pynput import keyboard as kb
    with kb.Listener(on_press=lambda k: on_press(k,eid), on_release=lambda k: on_release(k,eid)) as listener:
        listener.join()
    stop_event.set(); time.sleep(0.1)
    if tray_icon:
        tray_icon.stop()
    print("Exited.")
