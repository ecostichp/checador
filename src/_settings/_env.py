from dotenvironment import DotEnvironment
from datetime import date
from .._constants import ENV_VAR_PREFIX

# Instancia de entorno
env = DotEnvironment(ENV_VAR_PREFIX)

class CONFIG:
    TODAY = env.variable('TODAY', date.fromisoformat, date.today)
