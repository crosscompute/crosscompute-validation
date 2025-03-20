# TODO: Check string lengths
import csv
from collections import Counter
from logging import getLogger
from os import getenv
from os.path import basename
from pathlib import Path

from aiofiles import open
from crosscompute_macros.disk import (
    is_contained_path,
    is_existing_path,
    is_file_path,
    is_folder_path,
    is_link_path,
    is_path_in_folder,
    list_paths)
from crosscompute_macros.iterable import (
    apply_functions,
    find_item)
from crosscompute_macros.log import (
    redact_path)
from crosscompute_macros.package import (
    is_equivalent_version)
from crosscompute_macros.text import (
    format_name, format_slug)
from ruamel.yaml import YAML
from ruamel.yaml.error import YAMLError

from ..errors import (
    CrossComputeConfigurationError,
    CrossComputeDataError,
    CrossComputeError,
    CrossComputeFormatError)
from ..settings import C, E
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
            validate_protocol,
            validate_paths,
            validate_tool_identifiers,
            validate_copyright,
            validate_tools,
            validate_steps,
            validate_prints,
            validate_presets,
            validate_datasets,
            validate_scripts,
            validate_environment,
            validate_display])

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
        self.tool_definition = kwargs['tool_definition']
        self._validation_functions.extend([
            validate_copyright_identifiers])


class StepDefinition(Definition):

    async def _initialize(self, **kwargs):
        self.name = kwargs['name']
        self.tool_definition = kwargs['tool_definition']
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
            validate_dataset_reference])


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


class DisplayDefinition(Definition):

    async def _initialize(self, **kwargs):
        self.tool_definition = kwargs['tool_definition']
        self._validation_functions.extend([
            validate_styles,
            # validate_templates,
            validate_pages])


class VariableDefinition(Definition):

    async def _initialize(self, **kwargs):
        self.step_name = kwargs.get('step_name')
        self._validation_functions.extend([
            validate_variable_identifiers,
            validate_variable_configuration])


class TemplateDefinition(Definition):

    async def _initialize(self, **kwargs):
        self.tool_definition = kwargs['tool_definition']
        self._validation_functions.extend([
            validate_template_identifiers])


class PackageDefinition(Definition):

    async def _initialize(self, **kwargs):
        self._validation_functions.extend([
            validate_package_identifiers])


class PortDefinition(Definition):

    async def _initialize(self, **kwargs):
        self._validation_functions.extend([
            validate_port_identifiers])


class StyleDefinition(Definition):

    async def _initialize(self, **kwargs):
        self.tool_definition = kwargs['tool_definition']
        self._validation_functions.extend([
            validate_style_identifiers])


class PageDefinition(Definition):

    async def _initialize(self, **kwargs):
        self._validation_functions.extend([
            validate_page_identifiers])


class ButtonDefinition(Definition):

    async def _initialize(self, **kwargs):
        self._validation_functions.extend([
            validate_button_identifiers])


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
    default_name = C.configuration_name
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
            'configuration was not found',
            code=C.error_configuration_not_found)
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


async def validate_protocol(d):
    if 'crosscompute' not in d:
        raise CrossComputeError('crosscompute protocol version is missing')
    protocol_version = d['crosscompute'].strip()
    PROTOCOL_VERSION = C.protocol_version
    if not protocol_version:
        raise CrossComputeConfigurationError(
            'crosscompute protocol version is required')
    elif not is_equivalent_version(
            protocol_version, PROTOCOL_VERSION, version_depth=3):
        raise CrossComputeConfigurationError(
            f'crosscompute protocol {PROTOCOL_VERSION} is not compatible with '
            f'crosscompute {PROTOCOL_VERSION}, which is currently installed; '
            f'pip install crosscompute=={PROTOCOL_VERSION}')
    return {
        'protocol_version': protocol_version}


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
            if not await is_path_in_folder(path, folder):
                raise CrossComputeConfigurationError(
                    f'path "{v}" must be in folder "{redact_path(folder)}"')
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
        C.tool_name if 'output' in d else C.kit_name
    ).replace('X', d.locus)).strip()
    slug = d.get('slug', format_slug(name)).strip()
    version = d.get('version', C.tool_version).strip()
    return {'name': name, 'slug': slug, 'version': version}


async def validate_copyright(d):
    copyright_map = get_map(d, 'copyright')
    copyright_definition = await CopyrightDefinition.load(
        copyright_map, tool_definition=d)
    return {'copyright_definition': copyright_definition}


async def validate_tools(d):
    tool_definitions = [d] if 'output' in d else []
    tool_maps = get_maps(d, 'tools')
    tool_folder = d.absolute_folder
    for i, tool_map in enumerate(tool_maps):
        if 'path' in tool_map:
            path = tool_folder / tool_map['path']
        else:
            raise CrossComputeConfigurationError(
                'tool path is required')
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
    for step_name in C.step_names:
        if step_name not in d:
            continue
        step_map = d[step_name]
        if not step_map:
            continue
        step_definition = await StepDefinition.load(
            step_map, name=step_name, tool_definition=d)
        step_definition_by_name[step_name] = step_definition
        variable_ids = [_.id for _ in step_definition.variable_definitions]
        assert_unique_values(variable_ids, 'variable id "{x}"')
        tool_variable_ids.extend(variable_ids)
    if 'return_code' in tool_variable_ids:
        raise CrossComputeConfigurationError(
            '"return_code" is a reserved variable')
    return {'step_definition_by_name': step_definition_by_name}


async def validate_prints(d):
    print_definition = d.step_definition_by_name.get('print')
    if print_definition:
        for variable_definition in print_definition.variable_definitions:
            view_name = variable_definition.view_name
            if view_name in ['link']:
                continue
            elif view_name not in C.printer_names:
                raise CrossComputeConfigurationError(
                    f'printer "{view_name}" is not supported')
            elif view_name not in E.printer_by_name:
                L.error(
                    f'printer "{view_name}" is missing; '
                    f'pip install crosscompute-printers-{view_name}')
            variable_id = variable_definition.id
            variable_configuration = variable_definition.configuration
            process_header_footer_options(variable_id, variable_configuration)
            process_page_number_options(variable_id, variable_configuration)
    return {}


async def validate_presets(d):
    preset_definitions = []
    for preset_map in get_maps(d, 'presets'):
        preset_definition = await PresetDefinition.load(
            preset_map, tool_definition=d, data={})
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
    dataset_maps = get_maps(d, 'datasets')
    dataset_definitions = [await DatasetDefinition.load(
        _, tool_definition=d) for _ in dataset_maps]
    assert_unique_values([
        _.path_name for _ in dataset_definitions], 'dataset path "{x}"')
    return {'dataset_definitions': dataset_definitions}


async def validate_scripts(d):
    script_maps = get_maps(d, 'scripts')
    script_definitions = [await ScriptDefinition.load(
        _, tool_definition=d) for _ in script_maps]
    return {'script_definitions': script_definitions}


async def validate_environment(d):
    environment_map = get_map(d, 'environment')
    environment_definition = await EnvironmentDefinition.load(
        environment_map, tool_definition=d)
    return {'environment_definition': environment_definition}


async def validate_display(d):
    display_map = get_map(d, 'display')
    display_definition = await DisplayDefinition.load(
        display_map, tool_definition=d)
    return {'display_definition': display_definition}


async def validate_copyright_identifiers(d):
    # TODO: Support multiple copyright owners
    copyright_name = d.get('name')
    copyright_year = d.get('year')
    copyright_image_uri = d.get('image_uri')
    copyright_owner_uri = d.get('owner_uri')
    if 'text' in d:
        copyright_text = d.get('text')
    elif copyright_name and copyright_year:
        if copyright_owner_uri:
            if copyright_image_uri:
                copyright_text = C.copyright_uri_and_image_text
            else:
                copyright_text = C.copyright_uri_text
        else:
            copyright_text = C.copyright_text
    else:
        copyright_text = ''
    try:
        copyright_text = copyright_text.format(**d)
    except KeyError as e:
        raise CrossComputeConfigurationError(
            f'copyright "{e}" is specified in text but undefined')
    copyright_text = copyright_text.strip()
    if not copyright_text and 'output' in d.tool_definition:
        raise CrossComputeConfigurationError(
            'copyright is required, either as text or name and year')
    return {'text': copyright_text}


async def validate_step_variables(d):
    variable_maps = get_maps(d, 'variables')
    variable_definitions = [await VariableDefinition.load(
        _, step_name=d.name) for _ in variable_maps]
    return {'variable_definitions': variable_definitions}


async def validate_step_templates(d):
    template_maps = get_maps(d, 'templates')
    template_definitions = [await TemplateDefinition.load(
        _, tool_definition=d.tool_definition) for _ in template_maps]
    return {'template_definitions': template_definitions}


async def validate_preset_identifiers(d):
    folder = get_text(d, 'folder')
    name = get_text(d, 'name', basename(folder))
    slug = get_text(d, 'slug', name)
    data_by_id = d.data.get(C.step_input, {})
    try:
        folder = format_text(folder, data_by_id)
        name = format_text(name, data_by_id)
        slug = format_text(slug, data_by_id)
    except CrossComputeConfigurationError as e:
        if hasattr(e, 'variable_id'):
            preset_configuration = get_map(d, 'configuration')
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
    preset_reference = get_map(d, 'reference')
    if 'folder' in preset_reference:
        reference_data_by_id = await d.tool_definition.load_data_by_id(
            preset_reference['folder'], 'input')
    else:
        reference_data_by_id = {}
    return {'__reference_data_by_id': reference_data_by_id}


async def validate_preset_configuration(d):
    preset_definitions = []
    preset_map = d.copy()
    preset_configuration = preset_map.pop('configuration', {})
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
            data = {
                C.step_input: reference_data_by_id | _ | preset_configuration}
            preset_definition = await PresetDefinition.load(
                d, tool_definition=tool_definition, data=data)
            preset_definitions.extend(preset_definition.preset_definitions)
    else:
        data_by_id = await tool_definition.load_data_by_id(
            d.folder_name, 'input')
        d.data[C.step_input] = d.data.get(C.step_input, {
        }) | reference_data_by_id | preset_configuration | data_by_id
        preset_definitions.append(d)
    return {'preset_definitions': preset_definitions}


async def validate_dataset_identifiers(d):
    path_name = get_required_string(d, 'path', 'dataset')
    if not is_contained_path(path_name):
        raise CrossComputeConfigurationError(
            f'dataset path "{path_name}" is invalid')
    input_mode = d.get('input', 'none')
    if input_mode not in ['none', 'replace']:
        raise CrossComputeConfigurationError(
            f'dataset input "{input_mode}" is not supported')
    output_mode = d.get('output', 'none')
    if output_mode not in ['none', 'append', 'replace']:
        raise CrossComputeConfigurationError(
            f'dataset output "{output_mode}" is not supported')
    return {
        'path_name': path_name,
        'input_mode': input_mode,
        'output_mode': output_mode}


async def validate_dataset_reference(d):
    dataset_reference = get_map(d, 'reference')
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
    elif 'uri' in dataset_reference:
        pass
    return {'reference': dataset_reference}


async def validate_script_identifiers(d):
    method_names = []
    if 'command' in d:
        command_string = d['command']
        preparation_map = {}
        method_names.append('command')
    if 'path' in d:
        command_string, preparation_map = prepare_script_path(
            d['path'])
        method_names.append('path')
    if 'function' in d:
        command_string, preparation_map = prepare_script_function(
            d.get('language', C.script_language), d['function'])
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
        'preparation_map': preparation_map}


async def validate_engine(d):
    return {
        'engine_name': d.get('engine', C.engine_name),
        'parent_image_name': d.get('image', C.image_name)}


async def validate_packages(d):
    package_maps = get_maps(d, 'packages')
    package_definitions = [await PackageDefinition.load(
        _) for _ in package_maps]
    return {
        'package_definitions': package_definitions}


async def validate_ports(d):
    port_definitions = []
    f = d.tool_definition.get_variable_definitions
    variable_definitions = f('log') + f('debug')
    for port_map in get_maps(d, 'ports'):
        port_definition = await PortDefinition.load(port_map)
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
    variable_maps = get_maps(d, 'variables')
    variable_definitions = [VariableDefinition(
        _) for _ in variable_maps]
    for variable_definition in variable_definitions:
        variable_id = variable_definition['id']
        if getenv(variable_id) is None:
            L.error('tool environment is missing variable "%s"', variable_id)
    assert_unique_values(
        [_['id'] for _ in variable_definitions],
        'environment variable id "{x}"')
    return {'variable_definitions': variable_definitions}


async def validate_styles(d):
    style_maps = get_maps(d, 'styles')
    style_definitions = [await StyleDefinition.load(
        _, tool_definition=d.tool_definition) for _ in style_maps]
    return {'style_definitions': style_definitions}


async def validate_pages(d):
    page_maps = get_maps(d, 'pages')
    page_definitions = [await PageDefinition.load(_) for _ in page_maps]
    return {'page_definitions': page_definitions}


async def validate_variable_identifiers(d):
    variable_id = get_required_string(d, 'id', 'variable')
    view_name = get_required_string(d, 'view', 'variable')
    path_name = get_required_string(d, 'path', 'variable')
    mode_name = d.get('mode', '').strip()
    label_text = d.get('label', format_name(variable_id)).strip()
    if not C.variable_id_pattern.match(variable_id):
        raise CrossComputeConfigurationError(
            f'variable "{variable_id}" is not a valid variable id; please use '
            'only lowercase, uppercase, numbers and underscores')
    if view_name not in E.view_by_name:
        raise CrossComputeConfigurationError(
            f'variable "{variable_id}" view "{view_name}" is not installed or '
            'not supported')
    if path_name.startswith('/') or path_name.startswith('..'):
        raise CrossComputeConfigurationError(
            f'variable "{variable_id}" path "{path_name}" must be within the '
            'folder')
    if mode_name and mode_name not in ['input']:
        raise CrossComputeConfigurationError(
            f'variable "{variable_id}" mode must be "input" if specified')
    return {
        'id': variable_id,
        'view_name': view_name,
        'path_name': path_name,
        'mode_name': mode_name,
        'label_text': label_text}


async def validate_variable_configuration(d):
    # TODO: Validate configuration according to variable view
    c = get_map(d, 'configuration')
    if 'path' in c:
        p = c['path']
        if not p.endswith('.json'):
            raise CrossComputeConfigurationError(
                f'variable configuration path "{p}" suffix must be ".json"')
    return {'configuration': c}


async def validate_template_identifiers(d):
    path_name = get_required_string(d, 'path', 'template')
    expression_text = d.get('expression')
    return {
        'path_name': path_name,
        'expression_text': expression_text}


async def validate_package_identifiers(d):
    package_id = get_required_string(d, 'id', 'package')
    manager_name = get_required_string(d, 'manager', 'package')
    if manager_name not in ['dnf', 'apt', 'pip', 'npm']:
        raise CrossComputeConfigurationError(
            f'manager "{manager_name}" is not supported')
    return {
        'id': package_id,
        'manager_name': manager_name}


async def validate_port_identifiers(d):
    port_id = get_required_string(d, 'id', 'port')
    port_number = get_required_string(d, 'number', 'port')
    try:
        port_number = int(port_number)
    except ValueError:
        raise CrossComputeConfigurationError(
            f'port number "{port_number}" must be an integer')
    return {
        'id': port_id,
        'number': port_number}


async def validate_style_identifiers(d):
    path_name = d.get('path', '').strip()
    uri = d.get('uri', '').strip()
    if not path_name and not uri:
        raise CrossComputeConfigurationError(
            'style path or uri is required')
    return {
        'path_name': path_name,
        'uri': uri}


async def validate_page_identifiers(d):
    page_id = get_required_string(d, 'id', 'page')
    design_name = d.get('design')
    if page_id == 'tool':
        if not design_name:
            design_name = 'input'
        if design_name not in ['input', 'output', 'none']:
            raise CrossComputeConfigurationError(
                f'tool design "{design_name}" is not supported')
    elif page_id in ['input', 'output', 'log', 'debug']:
        if not design_name:
            design_name = 'flex'
        if design_name not in ['flex', 'flat', 'none']:
            raise CrossComputeConfigurationError(
                f'tool design "{design_name}" is not supported')
    else:
        raise CrossComputeConfigurationError(
            f'page id "{page_id}" is not supported')
    button_maps = get_maps(d, 'buttons')
    button_definitions = [await ButtonDefinition.load(_) for _ in button_maps]
    return {
        'id': page_id,
        'design_name': design_name,
        'button_definitions': button_definitions}


async def validate_button_identifiers(d):
    button_id = get_required_string(d, 'id', 'button')
    if button_id not in ['continue', 'back']:
        raise CrossComputeConfigurationError(
            f'button id "{button_id}" is not supported')
    button_text = get_required_string(d, 'text', 'button')
    return {'id': button_id, 'text': button_text}


async def yield_data_by_id_from_csv(path, variable_definitions):
    try:
        async with open(path, mode='rt') as f:
            lines = await f.readlines()
            csv_reader = csv.reader(lines)
            keys = [_.strip() for _ in next(csv_reader)]
            for values in csv_reader:
                data_by_id = await parse_data_by_id({
                    k: {C.data_value: v} for k, v in zip(keys, values)
                }, variable_definitions)
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
                data_by_id = {variable_id: {C.data_value: line}}
                data_by_id = await parse_data_by_id(
                    data_by_id, variable_definitions)
                yield data_by_id
    except OSError as e:
        raise CrossComputeConfigurationError(e)


async def parse_data_by_id(data_by_id, variable_definitions):
    for variable_definition in variable_definitions:
        variable_id = variable_definition.id
        try:
            variable_data = data_by_id[variable_id]
        except KeyError:
            continue
        if C.data_valuenot in variable_data:
            continue
        variable_value = variable_data[C.data_value]
        variable_view = LoadableVariableView.get_from(variable_definition)
        try:
            variable_value = await variable_view.parse(variable_value)
        except CrossComputeDataError as e:
            e.variable_id = variable_id
            raise
        variable_data[C.data_value] = variable_value
    return data_by_id


def prepare_script_path(script_path):
    path = Path(script_path)
    match path.suffix:
        case '.py':
            command_string = f'python "{path}"'
            preparation_map = {}
        case '.ipynb':
            new_path = '.' + str(path.with_suffix('.ipynb.py'))
            command_string = f'python "{new_path}"'
            preparation_map = {
                'target_path': new_path,
                'notebook_path': path}
        case '.sh':
            command_string = f'bash "{path}"'
            preparation_map = {}
        case _:
            suffixes_string = ' '.join(['.py', '.ipynb'])
            raise CrossComputeConfigurationError(
                f'script path suffix can be one of {suffixes_string}; '
                f'message {C.support_email} to request support for '
                'more suffixes')
    return command_string, preparation_map


def prepare_script_function(script_language, function_string):
    match script_language:
        case 'python':
            path = '.run.py'
            command_string = f'python "{path}"'
            preparation_map = {
                'target_path': path,
                'function_string': function_string}
        case _:
            languages_string = ' '.join(['python'])
            raise CrossComputeConfigurationError(
                f'script language can be one of {languages_string}; '
                f'message {C.support_email} to request support for '
                'more languages')
    return command_string, preparation_map


def process_header_footer_options(variable_id, print_configuration):
    k = 'header-footer'
    d = get_map(print_configuration, k)
    d['skip-first'] = bool(d.get('skip-first'))


def process_page_number_options(variable_id, print_configuration):
    k = 'page-number'
    d = get_map(print_configuration, k)
    location = d.get('location')
    if location and location not in ['header', 'footer']:
        raise CrossComputeConfigurationError(
            f'print variable "{variable_id}" configuration "{k}" '
            f'location "{location}" is not supported')
    alignment = d.get('alignment')
    if alignment and alignment not in ['left', 'center', 'right']:
        raise CrossComputeConfigurationError(
            f'print variable "{variable_id}" configuration "{k}" '
            f'alignment "{alignment}" is not supported')


def get_required_string(d, k, x):
    try:
        value = d[k].strip()
    except KeyError:
        raise CrossComputeConfigurationError(
            f'"{k}" is required for each {x}')
    except AttributeError:
        raise CrossComputeConfigurationError(
            f'"{k}" must be a string')
    if not value:
        raise CrossComputeConfigurationError(
            f'"{k}" cannot be empty')
    return value


def get_maps(d, k):
    values = get_list(d, k)
    for v in values:
        if not isinstance(v, dict):
            raise CrossComputeConfigurationError(
                f'"{k}" must be a list of maps')
    return values


def get_map(d, k):
    value = d.get(k, {})
    if not isinstance(value, dict):
        raise CrossComputeConfigurationError(f'"{k}" must be a map')
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
        value = variable_data.get(C.data_value, '')
        try:
            value = apply_functions(value, terms[1:], {
                'slug': format_slug,
                'title': str.title})
        except KeyError as e:
            raise CrossComputeConfigurationError(
                f'function "{e.args[0]}" is not supported in '
                f'"{matching_inner_text}"')
        return str(value)

    return C.variable_id_template_pattern.sub(f, text)


def assert_unique_values(values, description):
    for x, count in Counter(values).items():
        if count > 1:
            raise CrossComputeConfigurationError(
                description.format(x=x) + ' is not unique')


YIELD_DATA_BY_ID_BY_SUFFIX = {
    '.csv': yield_data_by_id_from_csv,
    '.txt': yield_data_by_id_from_txt}
L = getLogger(__name__)
