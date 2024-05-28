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
        texts = [super().__str__()]
        if hasattr(self, 'variable_id'):
            texts.append(f'variable_id="{self.variable_id}"')
        if hasattr(self, 'path'):
            texts.append(f'path="{redact_path(self.path)}"')
        if hasattr(self, 'uri'):
            texts.append(f'uri="{self.uri}"')
        if hasattr(self, 'code'):
            texts.append(f'code={self.code}')
        if hasattr(self, 'tool'):
            tool = self.tool
            texts.extend([
                f'tool_name="{tool.name}"'
                f'tool_version="{tool.version}"'])
        return '; '.join(texts)


class CrossComputeFormatError(CrossComputeError):
    pass


class CrossComputeConfigurationError(CrossComputeError):
    pass


class CrossComputeDataError(CrossComputeError):
    pass
