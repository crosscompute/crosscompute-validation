from .macros.log import (
    redact_path)


class CrossComputeError(Exception):

    def __init__(self, *args, **kwargs):
        super().__init__(*args)
        self.__dict__.update(kwargs)

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
