import json
from importlib.metadata import entry_points
from logging import getLogger
from os.path import splitext

from crosscompute_macros.disk import (
    FileCache,
    get_byte_count,
    load_raw_json,
    load_raw_text)
from crosscompute_macros.package import import_attribute

from ..constants import (
    D_PATH,
    D_VALUE,
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
        data_by_id[variable.id] = await load_variable_data(
            folder / path_name, variable)
    return data_by_id


async def load_variable_data(path, variable):
    # TODO: Load variable_configuration
    variable_id = variable.id
    try:
        raw_data = await raw_data_cache.get(path)
    except CrossComputeDataError as e:
        e.variable_id = variable_id
        raise
    suffix = splitext(path)[1]
    if suffix == '.dictionary':
        raw_value = raw_data[D_VALUE]
        try:
            variable_value = raw_value[variable_id]
        except KeyError:
            raise CrossComputeDataError(
                'value was not found', variable_id=variable_id, path=path)
        variable_data = {D_VALUE: variable_value}
    else:
        variable_data = raw_data
    if D_VALUE in variable_data:
        variable_view = LoadableVariableView.get_from(variable)
        variable_data[D_VALUE] = await variable_view.parse(variable_data[
            D_VALUE])
    return variable_data


async def load_raw_data(path):
    matching_paths = await get_matching_paths(path)
    match len(matching_paths):
        case 0:
            raise CrossComputeDataError('path does not exist', path=path)
        case 1:
            path = matching_paths[0]
        case _:
            return {D_PATH: path}
    suffix = splitext(path)[1]
    if suffix == '.dictionary':
        return await load_dictionary_data(path)
    if suffix in ['.md', '.txt']:
        return await load_text_data(path)
    return {D_PATH: path}


async def load_dictionary_data(path):
    try:
        value = await load_raw_json(path)
    except OSError as e:
        raise CrossComputeDataError(f'file does not load; {e}', path=path)
    except json.JSONDecodeError as e:
        raise CrossComputeDataError(f'json expected; {e}', path=path)
    if not isinstance(value, dict):
        raise CrossComputeDataError('dictionary expected', path=path)
    return {D_VALUE: value}


async def load_text_data(path):
    try:
        byte_count = await get_byte_count(path)
        if byte_count > RAW_DATA_BYTE_COUNT:
            return {D_PATH: path}
        value = await load_raw_text(path)
    except OSError as e:
        raise CrossComputeDataError(e, path=path)
    return {D_VALUE: value}


raw_data_cache = FileCache(
    load=load_raw_data,
    length=RAW_DATA_CACHE_LENGTH)
L = getLogger(__name__)
