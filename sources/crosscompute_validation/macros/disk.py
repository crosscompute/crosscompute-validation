from aiofiles.os import (
    listdir, path)


is_existing_path = path.exists
is_file_path = path.isfile
is_folder_path = path.isdir
list_paths = listdir
