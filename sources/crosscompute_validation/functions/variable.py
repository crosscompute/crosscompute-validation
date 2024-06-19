import json
from logging import getLogger

from crosscompute_macros.disk import (
    FileCache,
    get_byte_count,
    is_existing_path,
    load_raw_json,
    load_raw_text)
from crosscompute_macros.package import import_attribute
from importlib_metadata import entry_points

from ..constants import (
    MAXIMUM_RAW_DATA_BYTE_COUNT,
    MAXIMUM_RAW_DATA_CACHE_LENGTH)
from ..errors import (
    CrossComputeDataError)
from ..settings import (
    view_by_name)


class VariableView:

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
        data_by_id[variable.id] = await load_variable_data(
            folder / variable.path_string, variable)
    return data_by_id


async def load_variable_data(path, variable):
    variable_id = variable.id
    try:
        raw_data = await raw_data_cache.get(path)
    except CrossComputeDataError as e:
        e.variable_id = variable_id
        raise
    if path.suffix == '.dictionary':
        raw_value = raw_data['value']
        try:
            variable_value = raw_value[variable_id]
        except KeyError:
            raise CrossComputeDataError(
                'value was not found', variable_id=variable_id, path=path)
        variable_data = {'value': variable_value}
    else:
        variable_data = raw_data
    if 'value' in variable_data:
        variable_view = VariableView.get_from(variable)
        variable_data['value'] = await variable_view.parse(variable_data[
            'value'])
    return variable_data


async def load_raw_data(path):
    if not await is_existing_path(path):
        raise CrossComputeDataError(f'path "{path}" does not exist')
    suffix = path.suffix
    if suffix == '.dictionary':
        return await load_dictionary_data(path)
    if suffix in ['.md', '.txt']:
        return await load_text_data(path)
    return {'path': path}


async def load_dictionary_data(path):
    try:
        value = await load_raw_json(path)
    except (json.JSONDecodeError, OSError) as e:
        raise CrossComputeDataError(e, path=path)
    if not isinstance(value, dict):
        raise CrossComputeDataError('dictionary was expected', path=path)
    return {'value': value}


async def load_text_data(path):
    try:
        byte_count = await get_byte_count(path)
        if byte_count > MAXIMUM_RAW_DATA_BYTE_COUNT:
            return {'path': path}
        value = await load_raw_text(path)
    except OSError as e:
        raise CrossComputeDataError(e, path=path)
    return {'value': value}


raw_data_cache = FileCache(
    load_data=load_raw_data,
    maximum_length=MAXIMUM_RAW_DATA_CACHE_LENGTH)
L = getLogger(__name__)
