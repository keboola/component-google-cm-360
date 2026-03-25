import dataclasses
import json
from dataclasses import dataclass, field
from enum import StrEnum

from keboola.component.exceptions import UserException

FILE_JSON_LABELS = "labels.json"


class ConfigurationException(UserException):
    pass


@dataclass
class FilterPair:
    name: str
    value: str


@dataclass
class Destination:
    table_name: str = ""
    incremental_loading: bool = True
    primary_key: list[str] = None
    primary_key_existing: list[str] = None


@dataclass
class TimeRange:
    period: str = ""
    date_from: str = ""
    date_to: str = ""


@dataclass
class ReportSettings:
    report_type: str = ""
    dimensions: list[str] = None
    metrics: list[str] = None


def _from_dict(cls, data: dict):
    """Deserialize a dict into a dataclass, ignoring unexpected fields."""
    hints = {f.name: f.type for f in dataclasses.fields(cls)}
    kwargs = {}
    for key, value in data.items():
        if key not in hints or value is None:
            continue
        type_hint = hints[key]
        origin = getattr(type_hint, "__origin__", None)
        if origin is list:
            kwargs[key] = value
        elif isinstance(type_hint, type) and issubclass(type_hint, StrEnum):
            kwargs[key] = type_hint(value)
        elif dataclasses.is_dataclass(type_hint) and isinstance(value, dict):
            kwargs[key] = _from_dict(type_hint, value)
        else:
            kwargs[key] = value
    return cls(**kwargs)


class ConfigurationBase:
    @staticmethod
    def _convert_private_value(value: str):
        return value.replace('"#', '"pswd_')

    @staticmethod
    def _convert_private_value_inv(value: str):
        if value and value.startswith("pswd_"):
            return value.replace("pswd_", "#", 1)
        else:
            return value

    @classmethod
    def load_from_dict(cls, configuration: dict):
        """
        Initialize the configuration dataclass object from dictionary.
        Args:
            configuration: Dictionary loaded from json configuration.

        Returns:

        """
        json_conf = json.dumps(configuration)
        json_conf = ConfigurationBase._convert_private_value(json_conf)
        return _from_dict(cls, json.loads(json_conf))

    @classmethod
    def get_dataclass_required_parameters(cls) -> list[str]:
        """
        Return list of required parameters based on the dataclass definition (no default value)
        Returns: List[str]

        """
        return [
            cls._convert_private_value_inv(f.name)
            for f in dataclasses.fields(cls)
            if f.default == dataclasses.MISSING and f.default_factory == dataclasses.MISSING
        ]


class InputVariant(StrEnum):
    REPORT_SPEC = "report_specification"
    REPORT_TEMPLATE = "report_template_id"
    REPORT_IDS = "existing_report_ids"
    METADATA = "metadata"


@dataclass
class Configuration(ConfigurationBase):
    profiles: list[str]
    input_variant: InputVariant
    destination: Destination = field(default_factory=Destination)
    metadata: list[str] = field(default_factory=list)
    time_range: TimeRange = field(default_factory=TimeRange)
    report_specification: ReportSettings = field(default_factory=ReportSettings)
    existing_report_ids: list[str] = field(default_factory=list)
    report_template_id: str = ""

    debug: bool = False
