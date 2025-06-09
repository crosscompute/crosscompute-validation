from aiofiles import open
from pytest import mark, raises

from crosscompute_macros.disk import (
    make_soft_link,
    remove_path)

from crosscompute_validation.errors import (
    CrossComputeConfigurationError)
from crosscompute_validation.functions.configuration import (
    Definition,
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
    await make_soft_link(tmpdir / 'c', 'b')
    await make_soft_link(tmpdir / 'b', 'a')
    await make_soft_link(tmpdir / 'a', '/')
    with raises(CrossComputeConfigurationError):
        await validate_paths(definition)
    await remove_path(tmpdir / 'a')
    await make_soft_link(tmpdir / 'a', 'c')
    with raises(CrossComputeConfigurationError):
        await validate_paths(definition)
    await remove_path(tmpdir / 'a')
    async with open(tmpdir / 'a', 'wt') as f:
        await f.write('A')
    await validate_paths(definition)


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
