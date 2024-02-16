import dataclasses
import json
from dataclasses import dataclass, field
from enum import Enum

import dataconf
from keboola.component.exceptions import UserException
from pyhocon.config_tree import ConfigTree

FILE_JSON_LABELS = 'labels.json'


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


class ConfigurationBase:

    @staticmethod
    def _convert_private_value(value: str):
        return value.replace('"#', '"pswd_')

    @staticmethod
    def _convert_private_value_inv(value: str):
        if value and value.startswith('pswd_'):
            return value.replace('pswd_', '#', 1)
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
        return dataconf.loads(json_conf, cls, ignore_unexpected=True)

    @classmethod
    def get_dataclass_required_parameters(cls) -> list[str]:
        """
        Return list of required parameters based on the dataclass definition (no default value)
        Returns: List[str]

        """
        return [cls._convert_private_value_inv(f.name) for f in dataclasses.fields(cls)
                if f.default == dataclasses.MISSING
                and f.default_factory == dataclasses.MISSING]


class InputVariant(str, Enum):
    REPORT_SPEC = "report_specification"
    REPORT_TEMPLATE = "report_template_id"
    REPORT_IDS = "existing_report_ids"
    METADATA = "metadata"


@dataclass
class Configuration(ConfigurationBase):
    profiles: list[str]
    input_variant: InputVariant
    destination: Destination = field(default_factory=lambda: ConfigTree({}))
    metadata: list[str] = field(default_factory=lambda: "")
    time_range: TimeRange = field(default_factory=lambda: ConfigTree({}))
    report_specification: ReportSettings = field(default_factory=lambda: ConfigTree({}))
    existing_report_ids: list[str] = field(default_factory=lambda: "")
    report_template_id: str = ""

    debug: bool = False
