from aiofiles import open
from pytest import mark, raises

from crosscompute_macros.disk import (
    make_link,
    remove_path)

from crosscompute_validation.constants import (
    ATTRIBUTION_TEXT,
    ATTRIBUTION_URI_AND_IMAGE_TEXT,
    ATTRIBUTION_URI_TEXT)
from crosscompute_validation.errors import (
    CrossComputeConfigurationError)
from crosscompute_validation.functions.configuration import (
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
    async def f(copyright_dictionary, attribution_text):
        d = await validate_copyright_identifiers(copyright_dictionary)
        assert d['text'] == attribution_text.format(**copyright_dictionary)
    copyright_text = '{name} {year} {owner_uri} {image_uri}'
    with raises(CrossComputeConfigurationError):
        await validate_copyright_identifiers({'text': copyright_text})
    copyright_dictionary = {
        'name': 'X', 'year': 1, 'owner_uri': 'https://example.com',
        'image_uri': 'https://example.com/image.svg', 'text': copyright_text}
    await f(copyright_dictionary, copyright_text)
    del copyright_dictionary['text']
    await f(copyright_dictionary, ATTRIBUTION_URI_AND_IMAGE_TEXT)
    del copyright_dictionary['image_uri']
    await f(copyright_dictionary, ATTRIBUTION_URI_TEXT)
    del copyright_dictionary['owner_uri']
    await f(copyright_dictionary, ATTRIBUTION_TEXT)
    del copyright_dictionary['year']
    await f(copyright_dictionary, '')


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
