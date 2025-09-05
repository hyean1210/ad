import webview
from app import app

"""
This script launches the Flask web application in a dedicated desktop window.

To create a single .exe file from this:
1. Make sure you have PyInstaller:
   pip install pyinstaller

2. Run the PyInstaller command in your terminal:
   pyinstaller --onefile --windowed --add-data "templates;templates" run_desktop.py

   - `--onefile`: Bundles everything into a single executable.
   - `--windowed`: Prevents a console window from appearing.
   - `--add-data "templates;templates"`: This is crucial. It tells PyInstaller to
     include the 'templates' folder (which contains your index.html) in the package.
     The format is 'source;destination'.

3. The .exe file will be created in a 'dist' folder.
"""

if __name__ == '__main__':
    # Create a pywebview window.
    # The first argument is the window title.
    # The second argument is the URL to open, which is our local Flask app.
    # The 'app' object is the Flask app instance we imported.
    webview.create_window('데이터 가공 자동화', app)
    
    # Start the event loop. This will display the window and run the app.
    webview.start()
