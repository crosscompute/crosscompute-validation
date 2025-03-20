import re

from crosscompute_macros.abstract import (
    Bag)


C = Bag()
# D = Bag()
E = Bag()


C.protocol_version = __version__ = '0.9.5'


C.error_configuration_not_found = -100


C.configuration_name = 'automate.yaml'
C.tool_name = 'Tool X'
C.kit_name = 'Kit X'
C.tool_version = '0.0.0'


C.step_names = 'input', 'log', 'output', 'debug', 'print'


C.copyright_uri_and_image_text = '''\
[<img src="{image_uri}" alt="{name}" loading="lazy">]({owner_uri}) © {year}'''
C.copyright_uri_text = '''\
[{name}]({owner_uri}) © {year}'''
C.copyright_text = '''\
{name} © {year}'''


C.variable_id_pattern = re.compile(r'[a-zA-Z0-9_]+$')
C.variable_id_template_pattern = re.compile(r'{ *([a-zA-Z0-9_| ]+?) *}')


C.raw_data_byte_count = 1024
C.raw_data_cache_length = 256


C.script_language = 'python'
C.engine_name = 'podman'
C.image_name = 'python'


C.printer_names = 'pdf',


C.support_email = 'support@crosscompute.com'


C.step_input = 'i'
C.step_output = 'o'
C.step_log = 'l'
C.step_debug = 'd'
C.step_print = 'p'


C.data_value = 'v'
C.data_path = 'p'
C.data_uri = 'u'
C.data_configuration = 'c'


E.view_by_name = {}
E.printer_by_name = {}
