"""EyeOnWater API integration."""

from __future__ import annotations

from .account import Account
from .client import Client
from .exceptions import (
    EyeOnWaterAPIError,
    EyeOnWaterAuthError,
    EyeOnWaterAuthExpired,
    EyeOnWaterException,
    EyeOnWaterRateLimitError,
    EyeOnWaterResponseIsEmpty,
    EyeOnWaterUnitError,
)
from .meter import Meter
from .meter_reader import MeterReader
from .models import AtAGlanceData, DailyUsagePoint, DataPoint, EOWUnits, NativeUnits
from .models.units import AggregationLevel, RequestUnits
from .units import convert_to_native, deduce_native_units

__all__ = [
    "Account",
    "AggregationLevel",
    "AtAGlanceData",
    "Client",
    "DailyUsagePoint",
    "DataPoint",
    "EOWUnits",
    "EyeOnWaterAPIError",
    "EyeOnWaterAuthError",
    "EyeOnWaterAuthExpired",
    "EyeOnWaterException",
    "EyeOnWaterRateLimitError",
    "EyeOnWaterResponseIsEmpty",
    "EyeOnWaterUnitError",
    "Meter",
    "MeterReader",
    "NativeUnits",
    "RequestUnits",
    "convert_to_native",
    "deduce_native_units",
]
