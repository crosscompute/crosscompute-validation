from collections import OrderedDict


class LRUDict(OrderedDict):
    # https://gist.github.com/davesteele/44793cd0348f59f8fadd49d7799bd306

    def __init__(self, *args, maximum_length: int, **kwargs):
        assert maximum_length > 0
        self.maximum_length = maximum_length
        super().__init__(*args, **kwargs)

    def __setitem__(self, key, value):
        super().__setitem__(key, value)
        super().move_to_end(key)
        while len(self) > self.maximum_length:
            super().__delitem__(next(iter(self)))

    def __getitem__(self, key):
        value = super().__getitem__(key)
        super().move_to_end(key)
        return value


def apply_functions(value, function_names, function_by_name):
    for function_name in function_names:
        function_name = function_name.strip()
        if not function_name:
            continue
        try:
            f = function_by_name[function_name]
        except KeyError:
            raise
        value = f(value)
    return value
