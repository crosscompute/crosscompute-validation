from logging import getLogger
from pathlib import Path

from aiofiles import open
from ruamel.yaml import YAML
from ruamel.yaml.error import YAMLError

from ..constants import (
    CONFIGURATION_NAME,
    ERROR_CONFIGURATION_NOT_FOUND)
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
        print(kwargs)

    async def _validate(self):
        d = self.__dict__
        for f in self._validation_functions:
            d.update(await f(self))
        for k in list(d.keys()):
            if k.startswith('__'):
                del d[k]


class ToolDefinition(Definition):

    async def _initialize(self, **kwargs):
        pass


async def load_configuration(path_or_folder):
    path_or_folder = Path(path_or_folder)
    if await is_file_path(path_or_folder):
        configuration = await load_configuration_from_path(path_or_folder)
    elif await is_folder_path(path_or_folder):
        configuration = await load_configuration_from_folder(path_or_folder)
    elif not await is_existing_path(path_or_folder):
        raise CrossComputeConfigurationError(
            f'"{path_or_folder}" does not exist')
    else:
        raise CrossComputeFormatError(
            f'"{path_or_folder}" must be a path or folder')
    return configuration


async def load_configuration_from_path(path, position='0'):
    path = Path(path).absolute()
    L.debug('loading "%s"', redact_path(path))
    try:
        c = await load_raw_configuration(path)
        c = await ToolDefinition.load(c, path=path, position=position)
    except CrossComputeConfigurationError as e:
        if not hasattr(e, 'path'):
            e.path = path
        raise
    return c


async def load_configuration_from_folder(folder):
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
            configuration = await load_configuration_from_path(path)
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


L = getLogger(__name__)
