import json

from aiofiles.os import (
    listdir, path, stat)

from .iterable import LRUDict


class FileCache(LRUDict):

    def __init__(self, *args, load_data, maximum_length: int, **kwargs):
        super().__init__(*args, maximum_length=maximum_length, **kwargs)
        self._load_data = load_data

    async def set(self, path, d):
        t = await get_modification_time(path)
        value = t, d
        super().__setitem__(path, value)

    async def get(self, path):
        if path in self:
            old_t, d = super().__getitem__(path)
            new_t = await get_modification_time(path)
            if old_t == new_t:
                return d
        data = await self._load_data(path)
        await self.set(path, data)
        return data


async def get_byte_count(path):
    s = await stat(path)
    return s.st_size


async def load_raw_text(path):
    async with open(path, mode='rt') as f:
        text = f.read()
    return text.rstrip()


async def load_raw_json(source_path):
    async with open(source_path, mode='rt') as f:
        text = await f.read()
        dictionary = json.loads(text)
    return dictionary


is_existing_path = path.exists
is_file_path = path.isfile
is_folder_path = path.isdir
list_paths = listdir
get_modification_time = path.getmtime
