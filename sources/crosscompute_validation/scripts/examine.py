import asyncio
from argparse import ArgumentParser
from logging import getLogger

from crosscompute_validation.errors import (
    CrossComputeError)
from crosscompute_validation.functions.configuration import (
    load_configuration)
from crosscompute_validation.macros.log import (
    configure_argument_parser_for_logging,
    configure_logging_from)


async def start(arguments=None):
    a = ArgumentParser()
    configure_argument_parser_for_logging(a)
    configure_argument_parser_for_examining(a)
    args = a.parse_args(arguments)
    try:
        configure_logging_from(args)
    except CrossComputeError as e:
        L.error(e)
        return
    configuration = await examine_with(args)
    for tool_definition in configuration.tool_definitions:
        print(tool_definition.slug)


def configure_argument_parser_for_examining(a):
    a.add_argument(
        'path_or_folder', nargs='?',
        default='.',
        help='configuration path or folder')


async def examine_with(args):
    return await examine(args.path_or_folder)


async def examine(path_or_folder):
    return await load_configuration(path_or_folder)


L = getLogger(__name__)


if __name__ == '__main__':
    asyncio.run(start())
