import threading
import time
import datetime
import json
from pynput import keyboard
import tkinter as tk
from tkinter import messagebox, ttk
import pystray
from PIL import Image, ImageDraw
import win32gui
import uiautomation as auto

LOG_FILE = 'Logs/activity_logs.txt'
stop_event = threading.Event()
paused = threading.Event()
pause_info = {}
tray_icon = None
REASONS = [
    "Personal break",
    "Lunch",
    "Meeting",
    "Phone call",
    "Technical issue",
    "Bathroom",
]


def get_active_window_title():
        hwnd = win32gui.GetForegroundWindow()
        return win32gui.GetWindowText(hwnd) if hwnd else None

def is_sensitive_input():
    try:
        ctrl = auto.GetFocusedControl()
        if getattr(ctrl, 'IsPasswordControl', False):
            return True
        name = (ctrl.Name or '').lower()
        if any(word in name for word in ['password', 'mot de passe', 'mdp']):
            return True
        automation_id = (ctrl.AutomationId or '').lower()
        control_type = (ctrl.ControlTypeName or '').lower()
        class_name = (ctrl.ClassName or '').lower()
        if any(word in val for val in [automation_id, control_type, class_name] for word in ['password', 'motdepasse', 'mdp']):
            return True
        return False
    except Exception:
        return False

def is_known_sensitive_app():
    title = get_active_window_title().lower()
    return any(keyword in title for keyword in ['sign in', 'inscription', 'connexion'])

def get_employee_id():
    emp = input("Enter employee ID (e.g., emp001): ").strip()
    return emp +" "+'hostname:'+ socket.gethostname() or 'unknown'

text_buffer = []
buffer_lock = threading.Lock()
sensitive_mode = False
last_keypress_time = time.monotonic()
flush_interval = 10  
buffer_limit = 30  

def log_event(event_type, text=None, window_name=None, employee_id=None, extra=None):
    if paused.is_set() and event_type not in ('pause_start', 'pause_end'):
        return
    event = {
        'timestamp': datetime.datetime.now().isoformat() ,
        'event_type': event_type,
        'text': text,
        'window_name': window_name,
        'employee_id': employee_id
    }
    if extra:
        event.update(extra)
    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(json.dumps(event) + "\n")

def flush_buffer(employee_id, window_name):
    global text_buffer
    with buffer_lock:
        if text_buffer:
            combined = ''.join(text_buffer)
            log_event('text_input', text=combined, window_name=window_name, employee_id=employee_id)
            text_buffer = []

def show_pause_dialog(icon):
    def on_submit():
        reason = reason_var.get()
        try:
            duration = int(duration_entry.get())
        except ValueError:
            messagebox.showerror("Error", "Duration must be a number.")
            return

        if not reason:
            messagebox.showerror("Error", "Please select a reason.")
            return
        if duration <= 0:
            messagebox.showerror("Error", "Duration must be greater than 0.")
            return

        start_pause(reason, duration, icon)
        dialog.destroy()

    dialog = tk.Tk()
    dialog.title("Pause Logging")

    tk.Label(dialog, text="Reason:").grid(row=0, column=0, padx=10, pady=10)
    reason_var = tk.StringVar()
    reason_dropdown = ttk.Combobox(dialog, textvariable=reason_var, values=REASONS, state="readonly")
    reason_dropdown.grid(row=0, column=1, padx=10, pady=10)

    tk.Label(dialog, text="Duration (minutes):").grid(row=1, column=0, padx=10, pady=10)
    duration_entry = tk.Entry(dialog)
    duration_entry.grid(row=1, column=1, padx=10, pady=10)

    submit_btn = tk.Button(dialog, text="Pause", command=on_submit)
    submit_btn.grid(row=2, column=0, columnspan=2, pady=10)

    dialog.mainloop()

def start_pause(reason, duration, icon):
    if paused.is_set():
        return
    paused.set()
    start = datetime.datetime.now()
    pause_info['start'] = start.isoformat() 
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
    end = datetime.datetime.now()
    pause_info['end'] = end.isoformat() 
    log_event('pause_end', window_name=get_active_window_title(), employee_id=eid, extra={'pause_info': pause_info})
    icon.menu = pystray.Menu(pystray.MenuItem('Pause Logging', lambda: show_pause_dialog(icon)))
    icon.update_menu()

def create_icon():
    global tray_icon
    image = Image.new('RGB', (64, 64), 'white')
    d = ImageDraw.Draw(image)
    d.rectangle((16, 16, 48, 48), fill='black')
    icon = pystray.Icon('ActivityLogger', image, 'Logger')
    icon.menu = pystray.Menu(pystray.MenuItem('Pause Logging', lambda: show_pause_dialog(icon)))
    tray_icon = icon
    threading.Thread(target=tray_icon.run, daemon=True).start()


from pynput.keyboard import Key, KeyCode

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

def on_press(key, employee_id):
    global sensitive_mode, last_keypress_time
    win = get_active_window_title()
    last_keypress_time = time.monotonic()  

    if is_sensitive_input() or is_known_sensitive_app():
        if not sensitive_mode:
            sensitive_mode = True
            flush_buffer(employee_id, win)
            log_event('sensitive_field_start', window_name=win, employee_id=employee_id)
        return
    else:
        if sensitive_mode:
            sensitive_mode = False
            log_event('sensitive_field_end', window_name=win, employee_id=employee_id)

    char = None
    if isinstance(key, KeyCode):
        if key.char:
            char = key.char
        elif key.vk in VK_NUMPAD_MAP:
            char = VK_NUMPAD_MAP[key.vk]

    with buffer_lock:
        if char is not None and char.isprintable():
            text_buffer.append(char)
        elif key == Key.space:
            text_buffer.append(' ')
        elif key == Key.enter:
            text_buffer.append('\n')

        if len(text_buffer) >= buffer_limit:
            flush_buffer(employee_id, win)

def on_release(key, employee_id):
    if key == keyboard.Key.esc:
        stop_event.set()
        flush_buffer(employee_id, get_active_window_title())
        return False

def track_active_window(employee_id, interval):
    last = None
    while not stop_event.is_set():
        cur = get_active_window_title()
        if cur != last:
            flush_buffer(employee_id, cur)
            log_event('window_change', window_name=cur, employee_id=employee_id)
            last = cur
        time.sleep(interval)


def flush_if_inactive_loop(employee_id, window_name_getter, timeout=5):
    global last_keypress_time
    while not stop_event.is_set():
        time.sleep(1)
        if time.monotonic() - last_keypress_time > timeout:
            flush_buffer(employee_id, window_name_getter())
import socket



if __name__ == '__main__':
    eid = get_employee_id()
    print(f"Logging to {LOG_FILE} for '{eid}'. ESC to stop.")
    create_icon()
    threading.Thread(target=track_active_window, args=(eid, 3), daemon=True).start()
    threading.Thread(target=flush_if_inactive_loop, args=(eid, get_active_window_title), daemon=True).start()
    with keyboard.Listener(on_press=lambda k: on_press(k, eid), on_release=lambda k: on_release(k, eid)) as listener:
        listener.join()
    stop_event.set()
    time.sleep(0.1)
    if tray_icon:
        tray_icon.stop()
    print("Exited.")
