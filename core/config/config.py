import sys

sys.path.append('/home/hadoop/libs')

from dynaconf import Dynaconf

settings = Dynaconf(envvar_prefix="BLADE",
                    environments=True,
                    merge_enabled=True,
                    env_switcher="ENV_FOR_BLADE",
                    settings_files=['settings.yaml'],
                    secrets='.secrets.yaml')