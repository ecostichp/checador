from typing import Literal

Devices = Literal['csl', 'sjc']
"""
Dispositivos en ubicaciones.
"""

NumericWeekday = Literal[0, 1, 2, 3, 4, 5, 6]
"""
### Día numérico de la semana
Tipo de dato que decribe los valores posibles de días de la semana en formato
numérico entero.
"""

PayFrequency = Literal['weekly', 'biweekly']
"""
Frecuencia de pago.

Valores disponibles:
- `'weekly'`: Semanal.
- `'biweekly'`: Quincenal.
"""

PermissionTypeOption = Literal['time', 'days']
"""
Tipos de permiso.
"""

ViewOptions = Literal['report', 'verifications']
"""
Opciones de validación.
"""
