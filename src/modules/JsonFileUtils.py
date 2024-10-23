# fmt: off
# pylint: disable=abstract-method,unnecessary-dunder-call,expression-not-assigned

"""
JSON file handling utility class
"""

__all__ = ["JsonFileUtils"]

import json
from modules.FileUtils import FileUtils

class JsonFileUtils(object):
    """JSON file handling utility class"""

    @classmethod
    def load_json_file(cls, f):
        """Returns JSON file as dict"""
        if FileUtils.is_gcs_path(f):
            return cls.load_json_file_from_gcs(f)
        else:
            return cls.load_json_file_from_local_path(f)

    @classmethod
    def load_json_file_from_gcs(f):
        """Loads JSON file from GCS path"""
        contents = FileUtils.get_gcs_file_as_text(f)
        return json.loads(contents)

    @classmethod
    def load_json_file_from_local_path(cls, f, open_text_mode='r'):
        """Loads JSON file from local storage ignoring byte order mark"""        
        try:
            with open(f, encoding='utf-8-sig', mode=open_text_mode) as json_file:
                return json.load(json_file)
        except OSError as ex:
            print('Error loading file [%s], %s', f, ex)

# fmt: on