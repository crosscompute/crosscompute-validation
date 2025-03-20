from aiofiles import open
from pytest import mark, raises

from crosscompute_macros.disk import (
    make_link,
    remove_path)

from crosscompute_validation.constants import (
    COPYRIGHT_TEXT,
    COPYRIGHT_URI_AND_IMAGE_TEXT,
    COPYRIGHT_URI_TEXT)
from crosscompute_validation.errors import (
    CrossComputeConfigurationError)
from crosscompute_validation.functions.configuration import (
    CopyrightDefinition,
    Definition,
    validate_copyright_identifiers,
    validate_paths,
    validate_steps)


@mark.asyncio
async def test_validate_paths(tmpdir):
    definition = Definition({
        'xs': ['a', 'b']})
    definition.absolute_folder = tmpdir
    await validate_paths(definition)
    definition['xs'] = [{'path': []}]
    with raises(CrossComputeConfigurationError):
        await validate_paths(definition)
    definition['xs'][0]['path'] = str(tmpdir / 'c')
    await make_link(tmpdir / 'c', 'b')
    await make_link(tmpdir / 'b', 'a')
    await make_link(tmpdir / 'a', '/')
    with raises(CrossComputeConfigurationError):
        await validate_paths(definition)
    await remove_path(tmpdir / 'a')
    await make_link(tmpdir / 'a', 'c')
    with raises(CrossComputeConfigurationError):
        await validate_paths(definition)
    await remove_path(tmpdir / 'a')
    async with open(tmpdir / 'a', 'wt') as f:
        await f.write('A')
    await validate_paths(definition)


@mark.asyncio
async def test_validate_copyright_identifiers():
    async def f(copyright_map, attribution_text):
        d = await validate_copyright_identifiers(copyright_map)
        assert d['text'] == attribution_text.format(**copyright_map)
    copyright_text = '{name} {year} {owner_uri} {image_uri}'
    with raises(CrossComputeConfigurationError):
        await validate_copyright_identifiers({'text': copyright_text})
    copyright_definition = CopyrightDefinition({
        'name': 'X', 'year': 1, 'owner_uri': 'https://example.com',
        'image_uri': 'https://example.com/image.svg', 'text': copyright_text})
    copyright_definition.tool_definition = {}
    await f(copyright_definition, copyright_text)
    del copyright_definition['text']
    await f(copyright_definition, COPYRIGHT_URI_AND_IMAGE_TEXT)
    del copyright_definition['image_uri']
    await f(copyright_definition, COPYRIGHT_URI_TEXT)
    del copyright_definition['owner_uri']
    await f(copyright_definition, COPYRIGHT_TEXT)
    del copyright_definition['year']
    await f(copyright_definition, '')


@mark.asyncio
async def test_validate_steps():
    with raises(CrossComputeConfigurationError):
        await validate_steps({
            'input': {
                'variables': [
                    {'id': 'a', 'view': 'string', 'path': 'a.txt'},
                ],
            }, 'output': {
                'variables': [
                    {'id': 'a', 'view': 'string', 'path': 'a.txt'},
                ],
            },
        })
