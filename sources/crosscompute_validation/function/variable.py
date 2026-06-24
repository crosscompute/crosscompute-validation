from logging import getLogger
from os.path import join, splitext

from crosscompute_macros.disk import (
    FileCache,
    get_byte_count,
    is_existing_path,
    load_raw_json,
    load_raw_text)
from crosscompute_macros.error import (
    DiskError,
    ParsingError)
from crosscompute_views.base import (
    LoadableVariableView)

from ..constant import (
    DATA_CONFIGURATION,
    DATA_PATH,
    DATA_VALUE,
    RAW_DATA_BYTE_COUNT,
    RAW_DATA_CACHE_LENGTH)
from ..error import (
    CrossComputeDataError)
from .disk import (
    get_matching_paths)


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


async def load_variable_data(
        folder, variable, *, with_configuration_path=True):
    variable_path = variable.path_name
    path = join(folder, variable_path)  # noqa: PTH118
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
    except KeyError as e:
        x = 'value was not found'
        raise CrossComputeDataError(
            x, variable_id=variable_id) from e
    return {DATA_VALUE: variable_value}


async def restore_data_configuration(
        variable_data, folder, variable, variable_value_by_id,
        with_configuration_path):
    variable_configuration = variable.configuration
    data_configuration = {}
    default_path = join(  # noqa: PTH118
        folder, variable.path_name + '.configuration')
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
        custom_path = join(  # noqa: PTH118
            folder, variable_configuration['path'])
        await update_data_configuration(data_configuration, custom_path)
    if data_configuration:
        variable_data[DATA_CONFIGURATION] = data_configuration


async def update_data_configuration(data_configuration, path):
    try:
        data_configuration.update(await load_raw_json(path))
    except (DiskError, ParsingError) as e:
        L.error(e)


async def load_raw_data(path):
    try:
        matching_paths = await get_matching_paths(path)
    except OSError as e:
        x = 'path does not exist'
        raise CrossComputeDataError(x, path=path) from e
    match len(matching_paths):
        case 0:
            x = 'path does not exist'
            raise CrossComputeDataError(x, path=path)
        case 1:
            path = matching_paths[0]
        case _:
            return {DATA_PATH: path}
    suffix = splitext(path)[1]  # noqa: PTH122
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
    except (DiskError, ParsingError) as e:
        raise CrossComputeDataError(e) from e
    if not isinstance(value, dict):
        x = 'dictionary expected'
        raise CrossComputeDataError(x, path=path)
    return {DATA_VALUE: value}


async def load_file_data(path, load):
    try:
        byte_count = await get_byte_count(path)
        if byte_count > RAW_DATA_BYTE_COUNT:
            return {DATA_PATH: path}
        value = await load(path)
    except (DiskError, ParsingError) as e:
        raise CrossComputeDataError(e) from e
    return {DATA_VALUE: value}


raw_data_cache = FileCache(
    load=load_raw_data,
    length=RAW_DATA_CACHE_LENGTH)
L = getLogger(__name__)
