from os import getenv


view_by_name = {}
printer_by_name = {}


shell_name = getenv('SHELL_NAME', 'bash')
