import subprocess
import sys
import json

from run_module import run_module






with open('settings_test.json', 'rb') as settings_file:
    settings_data = json.loads(settings_file.read().decode('utf-8'))

run_module(settings_data)