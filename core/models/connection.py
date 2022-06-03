import json

from ..config import settings

def get_credentials(base):
    return settings[base].to_dict()


