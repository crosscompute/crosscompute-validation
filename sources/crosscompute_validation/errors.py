from crosscompute_macros.log import (
    redact_path)


class CrossComputeError(Exception):

    def __init__(self, *args, **kwargs):
        super().__init__(*args)
        self.__dict__.update(kwargs)

    def __getattr__(self, name):
        return self._get_inner_dict().get(name)

    def __str__(self):
        texts = [super().__str__()]
        d = self.__dict__
        if 'path' in d:
            x = redact_path(d['path'])
            texts.append(f'path="{x}"')
        if 'variable_id' in d:
            x = d['variable_id']
            texts.append(f'variable_id="{x}"')
        if 'uri' in d:
            x = d['uri']
            texts.append(f'uri="{x}"')
        if 'code' in d:
            x = d['code']
            texts.append(f'code={x}')
        if 'tool' in d:
            x = d['tool']
            texts.extend([
                f'tool_name="{x.name}"',
                f'tool_version="{x.version}"'])
        return '; '.join(texts)

    def _get_inner_dict(self):
        args = self.args
        if args:
            arg = args[0]
            if isinstance(arg, Exception):
                return arg.__dict__
        return {}

    def get_map(self):
        d = {}
        try:
            d['message'] = self.args[0]
        except IndexError:
            pass
        for k, v in self.__dict__.items():
            if k == 'tool':
                d['tool_name'] = v.name
                d['tool_version'] = v.version
            else:
                d[k] = v
        return d


class CrossComputeFormatError(CrossComputeError):
    pass


class CrossComputeConfigurationError(CrossComputeError):
    pass


class CrossComputeDataError(CrossComputeError):
    pass
