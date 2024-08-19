import csv
from collections import Counter
from logging import getLogger
from os import environ
from os.path import basename
from pathlib import Path

from aiofiles import open
from crosscompute_macros.disk import (
    assert_path_is_in_folder,
    is_existing_path,
    is_file_path,
    is_folder_path,
    is_link_path,
    list_paths)
from crosscompute_macros.error import (
    DiskError)
from crosscompute_macros.iterable import (
    apply_functions,
    find_item)
from crosscompute_macros.log import (
    redact_path)
from crosscompute_macros.text import (
    format_slug)
from ruamel.yaml import YAML
from ruamel.yaml.error import YAMLError

from ..constants import (
    ATTRIBUTION_TEXT,
    ATTRIBUTION_URI_AND_IMAGE_TEXT,
    ATTRIBUTION_URI_TEXT,
    CONFIGURATION_NAME,
    D_VALUE,
    ENGINE_NAME,
    ERROR_CONFIGURATION_NOT_FOUND,
    IMAGE_NAME,
    PACKAGE_MANAGER_NAMES,
    SCRIPT_LANGUAGE,
    STEP_NAMES,
    SUPPORT_EMAIL,
    S_INPUT,
    TOOLKIT_NAME,
    TOOL_NAME,
    TOOL_VERSION,
    VARIABLE_ID_TEMPLATE_PATTERN)
from ..errors import (
    CrossComputeConfigurationError,
    CrossComputeDataError,
    CrossComputeError,
    CrossComputeFormatError)
from ..settings import (
    shell_name)
from .variable import (
    LoadableVariableView,
    load_variable_data_by_id)


class Definition(dict):

    def __init__(self, d):
        super().__init__(d)
        self._validation_functions = []

    @classmethod
    async def load(Class, d, **kwargs):
        instance = Class(d)
        await instance._initialize(**kwargs)
        await instance._validate()
        return instance

    async def _initialize(self, **kwargs):
        pass

    async def _validate(self):
        d = self.__dict__
        for f in self._validation_functions:
            d.update(await f(self))
        for k in list(d.keys()):
            if k.startswith('__'):
                del d[k]


class ToolDefinition(Definition):

    async def _initialize(self, **kwargs):
        self.absolute_path = path = kwargs['path'].absolute()
        self.absolute_folder = path.parent
        self.locus = kwargs['locus']
        self._validation_functions.extend([
            validate_paths,
            validate_tool_identifiers,
            validate_copyright,
            validate_tools,
            validate_steps,
            validate_presets,
            validate_datasets,
            validate_scripts,
            validate_environment])

    async def load_data_by_id(self, result_folder, step_name):
        variable_definitions = self.get_variable_definitions(step_name)
        tool_folder = self.absolute_folder
        step_folder = tool_folder / result_folder / step_name
        data_by_id = await load_variable_data_by_id(
            step_folder, variable_definitions)
        return data_by_id

    def get_variable_definitions(self, step_name):
        d = self.step_definition_by_name
        if step_name not in d:
            return []
        return d[step_name].variable_definitions


class CopyrightDefinition(Definition):

    async def _initialize(self, **kwargs):
        self._validation_functions.extend([
            validate_copyright_identifiers])


class StepDefinition(Definition):

    async def _initialize(self, **kwargs):
        self.name = kwargs['name']
        self._validation_functions.extend([
            validate_step_variables,
            validate_step_templates])


class PresetDefinition(Definition):

    async def _initialize(self, **kwargs):
        self.tool_definition = kwargs['tool_definition']
        self.data = kwargs['data']
        self._validation_functions.extend([
            validate_preset_identifiers,
            validate_preset_reference,
            validate_preset_configuration])


class DatasetDefinition(Definition):

    async def _initialize(self, **kwargs):
        self.tool_definition = kwargs['tool_definition']
        self._validation_functions.extend([
            validate_dataset_identifiers,
            validate_dataset_reference,
            # validate_dataset_script,
        ])


class ScriptDefinition(Definition):

    async def _initialize(self, **kwargs):
        self.tool_definition = kwargs['tool_definition']
        self._validation_functions.extend([
            validate_script_identifiers])


class EnvironmentDefinition(Definition):

    async def _initialize(self, **kwargs):
        self.tool_definition = kwargs['tool_definition']
        self._validation_functions.extend([
            validate_engine,
            validate_packages,
            validate_ports,
            validate_environment_variables])


class VariableDefinition(Definition):

    async def _initialize(self, **kwargs):
        self._validation_functions.extend([
            validate_variable_identifiers])


class PackageDefinition(Definition):

    async def _initialize(self, **kwargs):
        self._validation_functions.extend([
            validate_package_identifiers])


class PortDefinition(Definition):

    async def _initialize(self, **kwargs):
        self._validation_functions.extend([
            validate_port_identifiers])


async def load_configuration(path_or_folder, locus='0'):
    path_or_folder = Path(path_or_folder)
    if await is_file_path(path_or_folder):
        configuration = await load_configuration_from_path(
            path_or_folder, locus)
    elif await is_folder_path(path_or_folder):
        configuration = await load_configuration_from_folder(
            path_or_folder, locus)
    elif not await is_existing_path(path_or_folder):
        raise CrossComputeConfigurationError(
            f'"{path_or_folder}" does not exist')
    else:
        raise CrossComputeFormatError(
            f'"{path_or_folder}" must be a path or folder')
    return configuration


async def load_configuration_from_path(path, locus):
    path = Path(path).absolute()
    L.debug('"%s" is loading', redact_path(path))
    try:
        c = await load_raw_configuration(path)
        c = await ToolDefinition.load(c, path=path, locus=locus)
    except CrossComputeConfigurationError as e:
        if not hasattr(e, 'path'):
            e.path = path
        raise
    L.debug('"%s" loaded', redact_path(path))
    return c


async def load_configuration_from_folder(folder, locus):
    relative_paths = await list_paths(folder)
    default_name = CONFIGURATION_NAME
    if default_name in relative_paths:
        relative_paths.remove(default_name)
        relative_paths.insert(0, default_name)
    for relative_path in relative_paths:
        path = folder / relative_path
        if await is_folder_path(path):
            continue
        try:
            configuration = await load_configuration_from_path(path, locus)
        except CrossComputeConfigurationError:
            raise
        except CrossComputeFormatError:
            continue
        break
    else:
        raise CrossComputeError(
            'configuration was not found', code=ERROR_CONFIGURATION_NOT_FOUND)
    return configuration


async def load_raw_configuration(configuration_path, with_comments=False):
    configuration_format = get_configuration_format(configuration_path)
    load = {
        'yaml': load_raw_configuration_yaml,
    }[configuration_format]
    return await load(configuration_path, with_comments)


def get_configuration_format(path):
    suffix = path.suffix
    try:
        configuration_format = {
            '.yaml': 'yaml',
            '.yml': 'yaml',
        }[suffix]
    except KeyError:
        raise CrossComputeFormatError(
            f'file suffix "{suffix}" is not supported')
    return configuration_format


async def load_raw_configuration_yaml(configuration_path, with_comments=False):
    yaml = YAML(typ='rt' if with_comments else 'safe')
    try:
        async with open(configuration_path, mode='rt') as f:
            configuration = yaml.load(await f.read())
    except (OSError, YAMLError) as e:
        raise CrossComputeConfigurationError(e)
    return configuration or {}


async def validate_paths(d):
    packs = list(d.items())
    folder = Path(d.absolute_folder)
    while packs:
        k, v = packs.pop()
        if k in ['path', 'folder']:
            try:
                path = folder / v
            except TypeError:
                raise CrossComputeConfigurationError(f'"{k}" must be a string')
            try:
                await assert_path_is_in_folder(path, folder)
            except DiskError as e:
                raise CrossComputeConfigurationError(
                    f'path "{v}" must be within '
                    f'folder "{redact_path(folder)}"; {e}')
        elif isinstance(v, dict):
            packs.extend(v.items())
        elif isinstance(v, list):
            for x in v:
                try:
                    packs.extend(x.items())
                except AttributeError:
                    pass
    return {}


async def validate_tool_identifiers(d):
    name = d.get('name', (
        TOOL_NAME if 'output' in d else TOOLKIT_NAME
    ).replace('X', d.locus)).strip()
    slug = d.get('slug', format_slug(name)).strip()
    version = d.get('version', TOOL_VERSION).strip()
    return {'name': name, 'slug': slug, 'version': version}


async def validate_copyright(d):
    copyright_dictionary = get_dictionary(d, 'copyright')
    copyright_definition = await CopyrightDefinition.load(copyright_dictionary)
    return {'copyright_definition': copyright_definition}


async def validate_tools(d):
    tool_definitions = [d] if 'output' in d else []
    tool_dictionaries = get_dictionaries(d, 'tools')
    tool_folder = d.absolute_folder
    for i, tool_dictionary in enumerate(tool_dictionaries):
        if 'path' in tool_dictionary:
            path = tool_folder / tool_dictionary['path']
        else:
            raise CrossComputeConfigurationError(
                'tool path or uri is required')
        try:
            tool_configuration = await load_configuration(
                path, f'{d.locus}-{i}')
        except CrossComputeFormatError as e:
            raise CrossComputeConfigurationError(e)
        tool_definitions.extend(tool_configuration.tool_definitions)
    assert_unique_values([_.name for _ in tool_definitions], 'tool name "{x}"')
    assert_unique_values([_.slug for _ in tool_definitions], 'tool slug "{x}"')
    return {'tool_definitions': tool_definitions}


async def validate_steps(d):
    step_definition_by_name = {}
    tool_variable_ids = []
    for step_name in STEP_NAMES:
        if step_name not in d:
            continue
        step_dictionary = d[step_name]
        step_definition = await StepDefinition.load(
            step_dictionary, name=step_name)
        step_definition_by_name[step_name] = step_definition
        variable_ids = [_.id for _ in step_definition.variable_definitions]
        assert_unique_values(variable_ids, 'variable id "{x}"')
        tool_variable_ids.extend(variable_ids)
    if 'return_code' in tool_variable_ids:
        raise CrossComputeConfigurationError(
            '"return_code" is a reserved variable')
    return {'step_definition_by_name': step_definition_by_name}


async def validate_presets(d):
    preset_definitions = []
    for preset_dictionary in get_dictionaries(d, 'presets'):
        preset_definition = await PresetDefinition.load(
            preset_dictionary, tool_definition=d, data={})
        preset_definitions.extend(preset_definition.preset_definitions)
    if 'output' in d and not preset_definitions:
        raise CrossComputeConfigurationError(
            'no presets found; define at least one preset')
    assert_unique_values([
        _.folder_name for _ in preset_definitions], 'preset folder "{x}"')
    assert_unique_values([
        _.name for _ in preset_definitions], 'preset name "{x}"')
    assert_unique_values([
        _.slug for _ in preset_definitions], 'preset slug "{x}"')
    return {'preset_definitions': preset_definitions}


async def validate_datasets(d):
    dataset_dictionaries = get_dictionaries(d, 'datasets')
    dataset_definitions = [await DatasetDefinition.load(
        _, tool_definition=d) for _ in dataset_dictionaries]
    return {'dataset_definitions': dataset_definitions}


async def validate_scripts(d):
    script_dictionaries = get_dictionaries(d, 'scripts')
    script_definitions = [await ScriptDefinition.load(
        _, tool_definition=d) for _ in script_dictionaries]
    return {'script_definitions': script_definitions}


async def validate_environment(d):
    environment_dictionary = get_dictionary(d, 'environment')
    environment_definition = await EnvironmentDefinition.load(
        environment_dictionary, tool_definition=d)
    return {'environment_definition': environment_definition}


async def validate_copyright_identifiers(d):
    copyright_name = d.get('name')
    copyright_year = d.get('year')
    copyright_image_uri = d.get('image_uri')
    copyright_owner_uri = d.get('owner_uri')
    if 'text' in d:
        attribution_text = d.get('text')
    elif copyright_name and copyright_year:
        if copyright_owner_uri:
            if copyright_image_uri:
                attribution_text = ATTRIBUTION_URI_AND_IMAGE_TEXT
            else:
                attribution_text = ATTRIBUTION_URI_TEXT
        else:
            attribution_text = ATTRIBUTION_TEXT
    else:
        attribution_text = ''
    try:
        attribution_text = attribution_text.format(**d)
    except KeyError as e:
        raise CrossComputeConfigurationError(
            f'copyright "{e}" is specified in text but undefined')
    return {'text': attribution_text}


async def validate_step_variables(d):
    variable_dictionaries = get_dictionaries(d, 'variables')
    variable_definitions = [await VariableDefinition.load(
        _) for _ in variable_dictionaries]
    return {'variable_definitions': variable_definitions}


async def validate_step_templates(d):
    return {'template_definitions': []}


async def validate_preset_identifiers(d):
    folder = get_text(d, 'folder')
    name = get_text(d, 'name', basename(folder))
    slug = get_text(d, 'slug', name)
    data_by_id = d.data.get(S_INPUT, {})
    try:
        folder = format_text(folder, data_by_id)
        name = format_text(name, data_by_id)
        slug = format_text(slug, data_by_id)
    except CrossComputeConfigurationError as e:
        if hasattr(e, 'variable_id'):
            preset_configuration = get_dictionary(d, 'configuration')
            if 'path' in preset_configuration:
                e.path = preset_configuration['path']
        raise
    if 'slug' not in d:
        slug = format_slug(slug)
    return {
        'folder_name': folder,
        'name': name,
        'slug': slug}


async def validate_preset_reference(d):
    preset_reference = get_dictionary(d, 'reference')
    if 'folder' in preset_reference:
        reference_data_by_id = await d.tool_definition.load_data_by_id(
            preset_reference['folder'], 'input')
    else:
        reference_data_by_id = {}
    return {'__reference_data_by_id': reference_data_by_id}


async def validate_preset_configuration(d):
    preset_definitions = []
    preset_dictionary = d.copy()
    preset_configuration = preset_dictionary.pop('configuration', {})
    reference_data_by_id = d.__reference_data_by_id
    tool_definition = d.tool_definition
    tool_folder = tool_definition.absolute_folder
    if 'path' in preset_configuration:
        path = tool_folder / preset_configuration.pop('path')
        suffix = path.suffix
        try:
            yield_data_by_id = YIELD_DATA_BY_ID_BY_SUFFIX[suffix]
        except KeyError:
            raise CrossComputeConfigurationError(
                f'preset configuration suffix "{suffix}" is not supported')
        input_variable_definitions = tool_definition.get_variable_definitions(
            'input')
        async for _ in yield_data_by_id(path, input_variable_definitions):
            data = {S_INPUT: reference_data_by_id | _ | preset_configuration}
            preset_definition = await PresetDefinition.load(
                d, tool_definition=tool_definition, data=data)
            preset_definitions.extend(preset_definition.preset_definitions)
    else:
        data_by_id = await tool_definition.load_data_by_id(
            d.folder_name, 'input')
        d.data[S_INPUT] = d.data.get(S_INPUT, {
        }) | reference_data_by_id | preset_configuration | data_by_id
        preset_definitions.append(d)
    return {'preset_definitions': preset_definitions}


async def validate_dataset_identifiers(d):
    dataset_path = get_path(d)
    if not dataset_path:
        raise CrossComputeConfigurationError(
            'path is required for each dataset')
    return {'path': dataset_path}


async def validate_dataset_reference(d):
    dataset_reference = get_dictionary(d, 'reference')
    if 'path' in dataset_reference:
        reference_path = get_path(dataset_reference)
        if reference_path:
            tool_folder = d.tool_definition.absolute_folder
            source_path = tool_folder / reference_path
            if not await is_existing_path(source_path):
                if await is_link_path(source_path):
                    raise CrossComputeConfigurationError(
                        f'dataset reference link "{reference_path}" is '
                        'invalid')
                elif source_path.name == 'results':
                    source_path.mkdir(parents=True)
                else:
                    raise CrossComputeConfigurationError(
                        f'dataset reference path "{reference_path}" was not '
                        'found')
            target_path = d.path
            if await is_existing_path(
                    target_path) and not await is_link_path(target_path):
                raise CrossComputeConfigurationError(
                    'dataset path conflicts with existing file: please '
                    f'relocate "{target_path}" from the disk to continue')
    elif 'uri' in dataset_reference:
        pass
    return {'reference': dataset_reference}


async def validate_script_identifiers(d):
    method_names = []
    if 'command' in d:
        command_string = d['command']
        preparation_dictionary = {}
        method_names.append('command')
    if 'path' in d:
        command_string, preparation_dictionary = prepare_script_path(
            d['path'])
        method_names.append('path')
    if 'function' in d:
        command_string, preparation_dictionary = prepare_script_function(
            d.get('language', SCRIPT_LANGUAGE), d['function'])
        method_names.append('function')
    if not method_names:
        raise CrossComputeConfigurationError(
            'script command or path or function is required')
    elif len(method_names) > 1:
        method_string = ' and '.join(method_names)
        raise CrossComputeConfigurationError(
            f'script {method_string} conflict; choose one')
    return {
        'folder': Path(d.get('folder', '.')),
        'command_string': command_string,
        'preparation_dictionary': preparation_dictionary}


async def validate_engine(d):
    return {
        'engine_name': d.get('engine', ENGINE_NAME),
        'parent_image_name': d.get('image', IMAGE_NAME)}


async def validate_packages(d):
    package_dictionaries = get_dictionaries(d, 'packages')
    package_definitions = [await PackageDefinition.load(
        _) for _ in package_dictionaries]
    return {
        'package_definitions': package_definitions}


async def validate_ports(d):
    port_definitions = []
    f = d.tool_definition.get_variable_definitions
    variable_definitions = f('log') + f('debug')
    for port_dictionary in get_dictionaries(d, 'ports'):
        port_definition = await PortDefinition.load(port_dictionary)
        port_id = port_definition.id
        try:
            variable_definition = find_item(
                variable_definitions, 'id', port_id)
        except StopIteration:
            raise CrossComputeConfigurationError(
                f'port "{port_id}" must correspond to a log or debug variable')
        port_definition.step_name = variable_definition.step_name
        port_definitions.append(port_definition)
    return {
        'port_definitions': port_definitions}


async def validate_environment_variables(d):
    variable_dictionaries = get_dictionaries(d, 'variables')
    variable_definitions = []
    for variable_dictionary in variable_dictionaries:
        variable_id = variable_dictionary['id']
        if variable_id not in environ:
            L.error('tool environment is missing variable "%s"', variable_id)
        variable_definition = VariableDefinition(variable_dictionary)
        variable_definition.id = variable_id
        variable_definitions.append(variable_definition)
    assert_unique_values([
        _.id for _ in variable_definitions], 'environment variable id "{x}"')
    return {'variable_definitions': variable_definitions}


async def validate_variable_identifiers(d):
    try:
        variable_id = d['id'].strip()
        view_name = d['view'].strip()
        variable_path = d['path'].strip()
    except KeyError as e:
        raise CrossComputeConfigurationError(
            f'{e} is required for each variable')
    return {
        'id': variable_id,
        'view_name': view_name,
        'path_name': variable_path}


async def validate_package_identifiers(d):
    try:
        package_id = d['id']
        manager_name = d['manager']
    except KeyError as e:
        raise CrossComputeConfigurationError(
            f'{e} is required for each package')
    if manager_name not in PACKAGE_MANAGER_NAMES:
        raise CrossComputeConfigurationError(
            f'manager "{manager_name}" is not supported')
    return {
        'id': package_id,
        'manager_name': manager_name}


async def validate_port_identifiers(d):
    try:
        port_id = d['id']
        port_number = d['number']
    except KeyError as e:
        raise CrossComputeConfigurationError(
            f'{e} is required for each port')
    try:
        port_number = int(port_number)
    except ValueError:
        raise CrossComputeConfigurationError(
            f'port number "{port_number}" must be an integer')
    return {
        'id': port_id,
        'number': port_number}


async def yield_data_by_id_from_csv(path, variable_definitions):
    try:
        async with open(path, mode='rt') as f:
            lines = await f.readlines()
            csv_reader = csv.reader(lines)
            keys = [_.strip() for _ in next(csv_reader)]
            for values in csv_reader:
                data_by_id = {k: {D_VALUE: v} for k, v in zip(keys, values)}
                data_by_id = await parse_data_by_id(
                    data_by_id, variable_definitions)
                if data_by_id.get('#') == '#':
                    continue
                yield data_by_id
    except OSError as e:
        raise CrossComputeConfigurationError(e)
    except StopIteration:
        pass


async def yield_data_by_id_from_txt(path, variable_definitions):
    if len(variable_definitions) > 1:
        raise CrossComputeConfigurationError(
            'use preset configuration suffix ".csv" to configure multiple '
            'variables')
    try:
        variable_id = variable_definitions[0].id
    except IndexError:
        raise CrossComputeConfigurationError(
            'define at least one input variable when using preset '
            'configuration suffix ".txt"')
    try:
        async with open(path, mode='rt') as f:
            lines = await f.readlines()
            for line in lines:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                data_by_id = {variable_id: {D_VALUE: line}}
                yield parse_data_by_id(data_by_id, variable_definitions)
    except OSError as e:
        raise CrossComputeConfigurationError(e)


async def parse_data_by_id(data_by_id, variable_definitions):
    for variable_definition in variable_definitions:
        variable_id = variable_definition.id
        try:
            variable_data = data_by_id[variable_id]
        except KeyError:
            continue
        if D_VALUE not in variable_data:
            continue
        variable_value = variable_data[D_VALUE]
        variable_view = LoadableVariableView.get_from(variable_definition)
        try:
            variable_value = await variable_view.parse(variable_value)
        except CrossComputeDataError as e:
            e.variable_id = variable_id
            raise
        variable_data[D_VALUE] = variable_value
    return data_by_id


def prepare_script_path(script_path):
    path = Path(script_path)
    match path.suffix:
        case '.py':
            command_string = f'python "{path}"'
            preparation_dictionary = {}
        case '.ipynb':
            new_path = '.' + str(path.with_suffix('.ipynb.py'))
            command_string = f'python "{new_path}"'
            preparation_dictionary = {
                'target_path': new_path,
                'notebook_path': path}
        case '.sh':
            command_string = f'{shell_name} "{path}"'
            preparation_dictionary = {}
        case _:
            suffixes_string = ' '.join(['.py', '.ipynb'])
            raise CrossComputeConfigurationError(
                f'script path suffix can be one of {suffixes_string}; '
                f'message {SUPPORT_EMAIL} to request support for '
                'more suffixes')
    return command_string, preparation_dictionary


def prepare_script_function(script_language, function_string):
    match script_language:
        case 'python':
            path = '.run.py'
            command_string = f'python "{path}"'
            preparation_dictionary = {
                'target_path': path,
                'function_string': function_string}
        case _:
            languages_string = ' '.join(['python'])
            raise CrossComputeConfigurationError(
                f'script language can be one of {languages_string}; '
                f'message {SUPPORT_EMAIL} to request support for '
                'more languages')
    return command_string, preparation_dictionary


def get_dictionaries(d, k):
    values = get_list(d, k)
    for v in values:
        if not isinstance(v, dict):
            raise CrossComputeConfigurationError(
                f'"{k}" must be a list of dictionaries')
    return values


def get_dictionary(d, k):
    value = d.get(k, {})
    if not isinstance(value, dict):
        raise CrossComputeConfigurationError(f'"{k}" must be a dictionary')
    return value


def get_list(d, k):
    value = d.get(k, [])
    if not isinstance(value, list):
        raise CrossComputeConfigurationError(f'"{k}" must be a list')
    return value


def get_text(d, k, default=None):
    value = d.get(k) or default
    if isinstance(value, dict):
        raise CrossComputeConfigurationError(
            f'"{k}" must be surrounded with quotes when it begins with a {{')
    return value


def get_path(d):
    path = d.get('path', '').strip()
    if not path:
        return
    return Path(path)


def format_text(text, data_by_id):
    if not data_by_id:
        return text

    def f(match):
        matching_inner_text = match.group(1)
        terms = matching_inner_text.split('|')
        variable_id = terms[0].strip()
        try:
            variable_data = data_by_id[variable_id]
        except KeyError:
            raise CrossComputeConfigurationError(
                f'preset "{text}" missing value',
                variable_id=variable_id)
        value = variable_data.get(D_VALUE, '')
        try:
            value = apply_functions(value, terms[1:], {
                'slug': format_slug,
                'title': str.title})
        except KeyError as e:
            raise CrossComputeConfigurationError(
                f'function "{e.args[0]}" is not supported in '
                f'"{matching_inner_text}"')
        return str(value)

    return VARIABLE_ID_TEMPLATE_PATTERN.sub(f, text)


def assert_unique_values(values, description):
    for x, count in Counter(values).items():
        if count > 1:
            raise CrossComputeConfigurationError(
                description.format(x=x) + ' is not unique')


YIELD_DATA_BY_ID_BY_SUFFIX = {
    '.csv': yield_data_by_id_from_csv,
    '.txt': yield_data_by_id_from_txt}
L = getLogger(__name__)
