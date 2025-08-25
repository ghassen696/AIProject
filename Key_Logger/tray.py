"""
tray.py: System tray icon with menu to pause/resume logging.
"""
from PIL import Image, ImageDraw
import pystray
import pause_manager
import config

from PIL import Image, ImageDraw, ImageFont

def create_image():
    # Create a blank image
    width, height = 64, 64  # Adjust the size based on your needs
    image = Image.new('RGBA', (width, height), (255, 255, 255, 0))  # Transparent background
    dc = ImageDraw.Draw(image)

    # Font settings (make sure this path points to a valid font on your system)
    font = ImageFont.load_default()

    text = 'Logging'  # Text to display on the tray icon
    # Use textbbox instead of textsize
    bbox = dc.textbbox((0, 0), text, font=font)
    text_width, text_height = bbox[2] - bbox[0], bbox[3] - bbox[1]

    # Draw the text in the center of the image
    dc.text(((width - text_width) / 2, (height - text_height) / 2), text, font=font, fill="black")

    return image

class SystemTrayIcon:
    def __init__(self, keylogger):
        self.keylogger = keylogger
        menu = pystray.Menu(
            pystray.MenuItem("Pause Logging", self.on_pause),
            pystray.MenuItem("Resume Logging", self.on_resume),
            pystray.MenuItem("Exit", self.on_exit)
        )
        self.icon = pystray.Icon(config.TRAY_ICON_TITLE, create_image(), config.TRAY_ICON_TITLE, menu)

    def on_pause(self, icon, item):
        pause_manager.show_pause_dialog(self.keylogger)

    def on_resume(self, icon, item):
        pause_manager.resume(self.keylogger)

    def on_exit(self, icon, item):
        self.keylogger.stop()
        icon.stop()

    def run(self):
        """
        Run the system tray icon (this call blocks until icon.stop() is invoked).
        """
        self.icon.run()
