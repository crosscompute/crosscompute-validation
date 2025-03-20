import json
from importlib.metadata import entry_points
from logging import getLogger
from os.path import join, splitext

from crosscompute_macros.disk import (
    FileCache,
    get_byte_count,
    is_existing_path,
    load_raw_json,
    load_raw_text)
from crosscompute_macros.log import (
    redact_path)
from crosscompute_macros.package import (
    import_attribute)

from ..constants import (
    DATA_CONFIGURATION,
    DATA_PATH,
    DATA_VALUE,
    RAW_DATA_BYTE_COUNT,
    RAW_DATA_CACHE_LENGTH)
from ..errors import (
    CrossComputeDataError)
from ..settings import (
    view_by_name)
from .disk import (
    get_matching_paths)


class LoadableVariableView:

    name = 'variable'

    @classmethod
    def get_from(Class, variable):
        view_name = variable.view_name
        try:
            View = view_by_name[view_name]
        except KeyError:
            L.error(
                'view "%s" is not installed and is needed by variable "%s"',
                view_name, variable.id)
            View = Class
        return View(variable)

    def __init__(self, variable):
        self.variable = variable

    async def parse(self, data):
        return data


class LoadableNumberView(LoadableVariableView):

    async def parse(self, value):
        try:
            value = float(value)
        except ValueError:
            raise CrossComputeDataError(
                f'value "{value}" is not a number',
                variable_id=self.variable.id)
        if value.is_integer():
            value = int(value)
        return value


def initialize_view_by_name(d=None, with_entry_points=False):
    if with_entry_points:
        for entry_point in entry_points().select(group='crosscompute.views'):
            view_by_name[entry_point.name] = import_attribute(
                entry_point.value)
    if d:
        view_by_name.update(d)


async def load_variable_data_by_id(folder, variables):
    data_by_id = {}
    for variable in variables:
        path_name = variable.path_name
        if path_name == 'ENVIRONMENT':
            continue
        try:
            variable_data = await load_variable_data(folder, variable)
        except CrossComputeDataError as e:
            L.debug(e)
            continue
        data_by_id[variable.id] = variable_data
    return data_by_id


async def load_variable_data(folder, variable, with_configuration_path=True):
    variable_path = variable.path_name
    path = join(folder, variable_path)
    if '{index}' in variable_path:
        return {DATA_PATH: path}
    variable_id = variable.id
    try:
        raw_data = await raw_data_cache.get(path)
    except CrossComputeDataError as e:
        e.variable_id = variable_id
        raise
    if path.endswith('.dictionary'):
        variable_value_by_id = raw_data[DATA_VALUE]
        variable_data = load_variable_data_from(
            variable_value_by_id, variable_id)
        await restore_data_configuration(
            variable_data, folder, variable, variable_value_by_id,
            with_configuration_path)
    elif with_configuration_path:
        variable_data = raw_data
        await restore_data_configuration(
            variable_data, folder, variable, {}, with_configuration_path)
    else:
        variable_data = raw_data
    if DATA_VALUE in variable_data:
        variable_data[DATA_VALUE] = await LoadableVariableView.get_from(
            variable).parse(variable_data[DATA_VALUE])
    return variable_data


def load_variable_data_from(variable_value_by_id, variable_id):
    try:
        variable_value = variable_value_by_id[variable_id]
    except KeyError:
        raise CrossComputeDataError(
            'value was not found', variable_id=variable_id)
    return {DATA_VALUE: variable_value}


async def restore_data_configuration(
        variable_data, folder, variable, variable_value_by_id,
        with_configuration_path):
    variable_configuration = variable.configuration
    data_configuration = {}
    default_path = join(folder, variable.path_name + '.configuration')
    if variable_value_by_id:
        variable_id = variable.id
        v = variable_value_by_id.get(variable_id + '.configuration', {})
        if isinstance(v, dict):
            data_configuration.update(v)
        else:
            L.error(f'data configuration must be a dictionary; {variable_id=}')
    elif with_configuration_path and await is_existing_path(default_path):
        await update_data_configuration(data_configuration, default_path)
    if 'path' in variable_configuration:
        custom_path = join(folder, variable_configuration['path'])
        await update_data_configuration(data_configuration, custom_path)
    if data_configuration:
        variable_data[DATA_CONFIGURATION] = data_configuration


async def update_data_configuration(data_configuration, path):
    try:
        data_configuration.update(await load_raw_json(path))
    except OSError as e:
        L.error(f'path "{redact_path(path)}" is not accessible; {e}')
    except json.JSONDecodeError as e:
        L.error(f'path "{redact_path(path)}" is not valid json; {e}')


async def load_raw_data(path):
    try:
        matching_paths = await get_matching_paths(path)
    except OSError:
        raise CrossComputeDataError('path does not exist', path=path)
    match len(matching_paths):
        case 0:
            raise CrossComputeDataError('path does not exist', path=path)
        case 1:
            path = matching_paths[0]
        case _:
            return {DATA_PATH: path}
    suffix = splitext(path)[1]
    if suffix == '.dictionary':
        return await load_dictionary_data(path)
    if suffix in ['.md', '.txt']:
        return await load_file_data(path, load_raw_text)
    if suffix in ['.geojson', '.json']:
        return await load_file_data(path, load_raw_json)
    return {DATA_PATH: path}


async def load_dictionary_data(path):
    try:
        value = await load_raw_json(path)
    except OSError as e:
        raise CrossComputeDataError(f'file does not load; {e}', path=path)
    except json.JSONDecodeError as e:
        raise CrossComputeDataError(f'json expected; {e}', path=path)
    if not isinstance(value, dict):
        raise CrossComputeDataError('dictionary expected', path=path)
    return {DATA_VALUE: value}


async def load_file_data(path, load):
    try:
        byte_count = await get_byte_count(path)
        if byte_count > RAW_DATA_BYTE_COUNT:
            return {DATA_PATH: path}
        value = await load(path)
    except OSError as e:
        raise CrossComputeDataError(e, path=path)
    return {DATA_VALUE: value}


raw_data_cache = FileCache(
    load=load_raw_data,
    length=RAW_DATA_CACHE_LENGTH)
L = getLogger(__name__)
