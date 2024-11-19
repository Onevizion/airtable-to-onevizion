import subprocess
import sys
import json

from run_module import run_module

installed_dependencies = subprocess.check_output([sys.executable, '-m', 'pip', 'install', '-r', 'python_dependencies.ini']).decode().strip()
if 'Successfully installed' in installed_dependencies:
    raise Exception('Some required dependent libraries were installed. ' \
                    'Module execution has to be terminated now to use installed libraries on the next scheduled launch.')

with open('settings.json', 'rb') as settings_file:
    settings_data = json.loads(settings_file.read().decode('utf-8'))

run_module(settings_data)
