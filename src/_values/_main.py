from datetime import timedelta

LUNCH_DURATION_LIMIT = timedelta(hours= 1, seconds= 59)
"""
`timedelta` Límite de duración de tiempo de comida.
"""

TIME_DELTA_ON_ZERO = timedelta()
"""
`timedelta(00:00:00)` Valor de delta de tiempo en ceros.
"""
