import win32gui
import win32process
import psutil
import uiautomation as auto

def get_active_window_title() -> str:
    try:
        hwnd = win32gui.GetForegroundWindow()
        title = win32gui.GetWindowText(hwnd)
        return title
    except Exception:
        return "no title"

def get_active_application_name() -> str:
    """
    Get the executable name of the currently active window's process.
    """
    try:
        hwnd = win32gui.GetForegroundWindow()
        _, pid = win32process.GetWindowThreadProcessId(hwnd)
        proc = psutil.Process(pid)
        return proc.name()
    except Exception:
        return "unknown_app"
def get_active_window_handle() -> int:
    """
    Return the handle of the currently active window.
    """
    try:
        return win32gui.GetForegroundWindow()
    except Exception:
        return None

def get_focused_control_name() -> str:
    """
    Get the name of the currently focused UI control.
    """
    try:
        control = auto.GetFocusedControl()
        if control:
            return control.Name or ""
    except Exception:
        pass
    return ""

SENSITIVE_KEYWORDS = ["password", "login", "signin", "auth", "secure", "pin", "passcode"]

def is_sensitive_field(window: str = "", control_name: str = "", app_name: str = "") -> bool:
    """
    Detect if the currently focused control is a sensitive input field (password/login).
    Works for English and French in desktop apps and browsers.
    """
    sensitive_keywords = ["password", "passwd", "login", "signin", "connexion", "mot de passe","pin","secure","credentials"]

    try:
        control = auto.GetFocusedControl()
        if control:
            ctrl_type = (getattr(control, 'ControlTypeName', "") or "").lower()
            name = (control.Name or "").lower()
            localized = (getattr(control, 'LocalizedControlType', "") or "").lower()
            automation_id = (getattr(control, 'AutomationId', "") or "").lower()

            combined = " ".join([ctrl_type, name, localized, automation_id])

            if any(word in combined for word in sensitive_keywords):
                return True

            # Extra heuristic: browser context
            if app_name.lower() in {"chrome.exe", "msedge.exe", "firefox.exe"}:
                window_lower = (window or "").lower()
                if any(word in window_lower for word in sensitive_keywords):
                    return True

    except Exception:
        pass

    return False

def get_active_window_info():
    try:
        hwnd = win32gui.GetForegroundWindow()
        title = win32gui.GetWindowText(hwnd)
        _, pid = win32process.GetWindowThreadProcessId(hwnd)
        proc = psutil.Process(pid)
        app_name = proc.name()
        return hwnd, title, app_name
    except Exception:
        return None, "no title", "unknown_app"
