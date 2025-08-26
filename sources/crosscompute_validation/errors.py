from crosscompute_macros.log import (
    redact_path)


class CrossComputeError(Exception):

    def __init__(self, *args, **kwargs):
        d = {}
        if args:
            arg = args[0]
            if isinstance(arg, CrossComputeError):
                d.update(arg.__dict__)
            elif isinstance(arg, dict):
                d['errors'] = arg
            else:
                d['message'] = str(arg)
        self.__dict__.update(d | kwargs)

    def __str__(self):
        texts = []
        if hasattr(self, 'message'):
            texts.append(self.message)
        if hasattr(self, 'errors'):
            texts.append(str(self.errors))
        if hasattr(self, 'path'):
            x = redact_path(self.path)
            texts.append(f'path="{x}"')
        if hasattr(self, 'variable_id'):
            x = self.variable_id
            texts.append(f'variable_id="{x}"')
        if hasattr(self, 'uri'):
            x = self.uri
            texts.append(f'uri="{x}"')
        if hasattr(self, 'code'):
            x = self.code
            texts.append(f'code={x}')
        if hasattr(self, 'tool'):
            x = self.tool
            texts.extend([
                f'tool_name="{x.name}"',
                f'tool_version="{x.version}"'])
        return '; '.join(texts)

    def get_map(self):
        d = {}
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
