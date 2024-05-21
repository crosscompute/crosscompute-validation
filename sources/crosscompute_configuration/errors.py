from .macros.log import (
    redact_path)


class CrossComputeError(Exception):

    def __init__(self, *args, **kwargs):
        if 'variable_id' in kwargs:
            self.variable_id = kwargs['variable_id']
        if 'path' in kwargs:
            self.path = kwargs['path']
        if 'uri' in kwargs:
            self.uri = kwargs['uri']
        if 'tool' in kwargs:
            self.tool = kwargs['tool']
        if 'code' in kwargs:
            self.code = kwargs['code']
        super().__init__(*args)

    def __str__(self):
        text = super().__str__()
        if hasattr(self, 'variable_id'):
            text += f' for variable "{self.variable_id}"'
        if hasattr(self, 'path'):
            text += f' in path "{redact_path(self.path)}"'
        if hasattr(self, 'uri'):
            text += f' with uri "{self.uri}"'
        if hasattr(self, 'tool'):
            tool = self.tool
            text + f' of tool "{tool.name} {tool.version}"'
        return text


class CrossComputeFormatError(CrossComputeError):
    pass


class CrossComputeConfigurationError(CrossComputeError):
    pass
