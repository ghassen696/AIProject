import threading
import time
import uuid
from datetime import datetime
from pynput import keyboard
from pynput.keyboard import Key
import pyperclip
import config
import logger
import tracker
import pause_manager


class Keylogger:
    def __init__(self):
        self.buffer = []   
        self.last_clipboard = None
        self.buffer_lock = threading.Lock()  
        self.last_window = None    
        self.last_hwnd  =None      
        self.last_keypress_time = time.time()
        self.running = False
        self.flush_thread = None
        self.session_id = str(uuid.uuid4())  # Unique session id per run
        self.seq_num = 0                     # Incremental event sequence
        self.idle_state = False              # Track if user is idle
        self.current_idle_id = None      # Track the unique idle session
        self.idle_start_time = None      # Track when idle started

    def start(self):
        self.running = True
        listener = keyboard.Listener(on_press=self.on_press)
        listener.daemon = True
        listener.start()
        logger.logger.info(f"Keylogger started. Session: {self.session_id}")

        self.flush_thread = threading.Thread(target=self._periodic_flush, daemon=True)
        self.flush_thread.start()

        self.clipboard_thread = threading.Thread(target=self._clipboard_watcher, daemon=True)
        self.clipboard_thread.start()

    def stop(self):
        self.running = False
        logger.logger.info("Keylogger stopped.")

    """def on_press(self, key):
        if not self.running or pause_manager.is_paused():
            return

        if tracker.is_sensitive_field():
            return

        with self.buffer_lock:
            # Detect idle state exit
            if self.idle_state:
                self._send_idle_event(idle_end=True)
                self.idle_state = False

            if key == Key.space:
                char = ' '
                self.buffer.append(char)
            elif key == Key.enter:
                char = '\n'
                self.buffer.append(char)
            else:
                try:
                    if hasattr(key, 'char') and key.char is not None:
                        char = key.char
                        if char.isprintable():
                            self.buffer.append(char)
                    elif key.vk in config.VK_NUMPAD_MAP:
                        char = config.VK_NUMPAD_MAP[key.vk]
                        self.buffer.append(char)
                    else:
                        char = f"<{key.name}>"
                except AttributeError:
                    char = f"<{key}>"

            hwnd, window, app_name = tracker.get_active_window_info()

            # Flush if window changed
            if self.last_hwnd and hwnd != self.last_hwnd:
                old_window = self.last_window
                old_app = self.last_app

                self._flush_buffer()
                self._send_window_switch_event(old_window, old_app)

            self.last_hwnd = hwnd
            self.last_window = window
            self.last_app = app_name   # <-- NEW, cache app name
            self.last_keypress_time = time.time()

            # Smart flush on punctuation/newline
            if char in {'.', '!', '?', '\n', ';'}:
                self._flush_buffer()

            if len(self.buffer) >= config.MAX_BUFFER_SIZE:
                self._flush_buffer()"""
    
    def on_press(self, key):
        if not self.running or pause_manager.is_paused():
            return


        with self.buffer_lock:
            # Detect idle state exit
            if self.idle_state:
                idle_duration = time.time() - self.idle_start_time
                self._send_idle_event(idle_end=True, idle_id=self.current_idle_id, idle_duration_sec=idle_duration)
                self.idle_state = False
                self.idle_start_time=None
                self.current_idle_id=None

            # Get current window info
            hwnd, window, app_name = tracker.get_active_window_info()
            control_name = tracker.get_focused_control_name()
            if window and "Pause Logging" in window:
                return
            # Flush if window changed BEFORE adding the new character
            if self.last_hwnd and hwnd != self.last_hwnd:
                self._flush_buffer()  # flush buffer from previous window
                self._send_window_switch_event(self.last_window, self.last_app)

            # Update to current window/app
            self.last_hwnd = hwnd
            self.last_window = window
            self.last_app = app_name
            self.last_keypress_time = time.time()

            # Determine the character pressed
            char = ""
            if key == Key.space:
                char = " "
            elif key == Key.enter:
                char = "\n"
            else:
                try:
                    if hasattr(key, "char") and key.char is not None:
                        char = key.char if key.char.isprintable() else ""
                    elif key.vk in config.VK_NUMPAD_MAP:
                        char = config.VK_NUMPAD_MAP[key.vk]
                    else:
                        char = f"<{key.name}>"
                except AttributeError:
                    char = f"<{key}>"

            # 🔒 Sensitive field check → replace with masked "*"
            if tracker.is_sensitive_field(window, control_name, app_name):
                char = "*"

            if char:
                self.buffer.append(char)

            # Smart flush on punctuation/newline
            if char in {".", "!", "?", "\n", ";"}:
                self._flush_buffer()

            # Flush if buffer exceeds max size
            if len(self.buffer) >= config.MAX_BUFFER_SIZE:
                self._flush_buffer()

    def _clipboard_watcher(self):
        while self.running:
            try:
                text = pyperclip.paste()
                if text and text != self.last_clipboard:
                    self.last_clipboard = text
                    self._log_clipboard_event(text)
            except Exception:
                pass
            time.sleep(0.5)  # check twice per second

    def _periodic_flush(self):
        while self.running:
            time.sleep(1)
            with self.buffer_lock:
                idle_time = time.time() - self.last_keypress_time
                if self.buffer and idle_time > config.IDLE_FLUSH_SECONDS:
                    self._flush_buffer()

                # Detect idle start event
                if idle_time > config.IDLE_FLUSH_SECONDS and not self.idle_state:
                    self.current_idle_id = str(uuid.uuid4())  # generate unique idle id
                    self.idle_start_time =time.time()
                    self._send_idle_event(idle_start=True)
                    self.idle_state = True

    def _flush_buffer(self):
        if not self.buffer:
            return

        self.seq_num += 1
        event = {
            "timestamp": datetime.now().isoformat(),
            "event": "keystrokes",
            "window": self.last_window,
            "application": self.last_app,  
            "control": tracker.get_focused_control_name(),
            "text": "".join(self.buffer).strip(),
            "employee_id": config.HOSTNAME,
            "session_id": self.session_id,
            "seq_num": self.seq_num
        }
        logger.log_event(event)
        logger.logger.info(f"Flushed {len(self.buffer)} chars from window: {event['window']}")
        self.buffer.clear()

    def _send_window_switch_event(self, new_window, new_app):
        self.seq_num += 1
        event = {
            "timestamp": datetime.now().isoformat(),
            "event": "window_switch",
            "window": new_window,
            "application": new_app,   # use cached app name
            "control": "",
            "text": "",
            "employee_id": config.HOSTNAME,
            "session_id": self.session_id,
            "seq_num": self.seq_num
        }
        logger.log_event(event)
        logger.logger.info(f"Window switched from: {new_window} to {self.last_window}")

    def _send_idle_event(self, idle_start=False, idle_end=False, idle_id=None,idle_duration_sec=None):
        self.seq_num += 1
        event_type = "idle_start" if idle_start else "idle_end" if idle_end else "idle"
        event = {
            "timestamp": datetime.now().isoformat(),
            "event": event_type,
            "window": self.last_window or "",
            "application": tracker.get_active_application_name(),
            "control": "",
            "text": "",
            "employee_id": config.HOSTNAME,
            "session_id": self.session_id,
            "seq_num": self.seq_num,
            "idle_id": idle_id,
            "idle_duration_sec": idle_duration_sec
        }

        logger.log_event(event)
        logger.logger.info(f"Idle event: {event_type}, duration: {idle_duration_sec}")

        # Reset idle tracking on idle_end
        if idle_end:
            self.current_idle_id = None
            self.idle_start_time = None

    def _log_clipboard_event(self, text):
        hwnd, window, app_name = tracker.get_active_window_info()
        control_name = tracker.get_focused_control_name()

        # Mask sensitive paste
        if tracker.is_sensitive_field(window, control_name, app_name):
            safe_text = "*" * len(text)
        else:
            safe_text = text
        self.seq_num += 1
        event = {
            "timestamp": datetime.now().isoformat(),
            "event": "clipboard_paste",
            "window": self.last_window or "",
            "application": self.last_app or "",
            "control": tracker.get_focused_control_name(),
            "text": safe_text.strip(),
            "employee_id": config.HOSTNAME,
            "session_id": self.session_id,
            "seq_num": self.seq_num
        }
        logger.log_event(event)
        logger.logger.info(f"Clipboard paste captured: {len(text)} chars")


