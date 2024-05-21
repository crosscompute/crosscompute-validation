from logging import getLogger
from pathlib import Path

from aiofiles import open
from ruamel.yaml import YAML
from ruamel.yaml.error import YAMLError

from ..errors import (
    CrossComputeConfigurationError,
    CrossComputeFormatError)
from ..macros.log import (
    redact_path)


class Definition(dict):

    def __init__(self, d, **kwargs):
        super().__init__(d)
        self._validation_functions = []

    @classmethod
    async def load(Class, d, **kwargs):
        instance = Class(d, **kwargs)
        await instance._initialize()
        await instance._validate()
        return instance

    async def _initialize(self):
        pass

    async def _validate(self):
        d = self.__dict__
        for f in self._validation_functions:
            d.update(await f(self))
        for k in list(d.keys()):
            if k.startswith('__'):
                del d[k]


class ToolDefinition(Definition):

    async def _initialize(self):
        pass


async def load_configuration(path, id='0'):
    path = Path(path).absolute()
    L.debug('loading "%s"', redact_path(path))
    try:
        c = await load_raw_configuration(path)
        c = await ToolDefinition.load(c, path=path, id=id)
    except CrossComputeConfigurationError as e:
        if not hasattr(e, 'path'):
            e.path = path
        raise
    return c


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
