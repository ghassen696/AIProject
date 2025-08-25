"""
main.py: Entry point for the employee monitoring keylogger system.
"""
"""
from keylogger import Keylogger
from tray import SystemTrayIcon

def main():
    keylogger = Keylogger()
    keylogger.start()

    tray_icon = SystemTrayIcon(keylogger)
    tray_icon.run()

if __name__ == "__main__":
    main()
"""
from keylogger import Keylogger
from tray import SystemTrayIcon
from consent import get_user_consent
import logger

def main():
    keylogger = Keylogger()

    if not get_user_consent(keylogger):
        logger.logger.info("User declined consent. Exiting application.")
        return

    keylogger.start()

    tray_icon = SystemTrayIcon(keylogger)
    tray_icon.run()

if __name__ == "__main__":
    main()
