from collections import Counter
from logging import getLogger
from pathlib import Path

from aiofiles import open
from ruamel.yaml import YAML
from ruamel.yaml.error import YAMLError

from ..constants import (
    CONFIGURATION_NAME,
    ERROR_CONFIGURATION_NOT_FOUND,
    STEP_NAMES,
    TOOLKIT_NAME,
    TOOL_NAME,
    TOOL_VERSION)
from ..errors import (
    CrossComputeConfigurationError,
    CrossComputeError,
    CrossComputeFormatError)
from ..macros.disk import (
    is_existing_path,
    is_file_path,
    is_folder_path,
    list_paths)
from ..macros.log import (
    redact_path)
from ..macros.text import (
    format_slug)


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
            validate_tool_identifiers,
            validate_tools,
        ])


class StepDefinition(Definition):

    def __init__(self, d, **kwargs):
        super().__init__(d, **kwargs)
        self.name = kwargs['name']
        self._validation_functions.extend([
            validate_step_variables,
            validate_step_templates])


class VariableDefinition(Definition):

    def __init__(self, d, **kwargs):
        super().__init__(d, **kwargs)
        self._validation_functions.extend([
            validate_variable_identifiers])


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


async def validate_tool_identifiers(d):
    name = d.get('name', (
        TOOL_NAME if 'output' in d else TOOLKIT_NAME
    ).replace('X', d.locus)).strip()
    slug = d.get('slug', format_slug(name)).strip()
    version = d.get('version', TOOL_VERSION).strip()
    return {'name': name, 'slug': slug, 'version': version}


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
    for step_name in STEP_NAMES:
        if step_name not in d:
            continue
        step_dictionary = d[step_name]
        step_definition = await StepDefinition.load(
            step_dictionary, name=step_name)
        step_definition_by_name[step_name] = step_definition
    return {'step_definition_by_name': step_definition_by_name}


async def validate_step_variables(d):
    variable_dictionaries = get_dictionaries(d, 'variables')
    variable_definitions = [await VariableDefinition.load(
        _) for _ in variable_dictionaries]
    return {'variable_definitions': variable_definitions}


async def validate_step_templates(d):
    return {'template_definitions': []}


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
        'path_string': variable_path}


def get_dictionaries(d, k):
    values = get_list(d, k)
    for v in values:
        if not isinstance(v, dict):
            raise CrossComputeConfigurationError(
                f'"{k}" must be a list of dictionaries')
    return values


def get_list(d, k):
    value = d.get(k, [])
    if not isinstance(value, list):
        raise CrossComputeConfigurationError(f'"{k}" must be a list')
    return value


def assert_unique_values(values, description):
    for x, count in Counter(values).items():
        if count > 1:
            raise CrossComputeConfigurationError(
                description.format(x=x) + ' is not unique')


L = getLogger(__name__)
