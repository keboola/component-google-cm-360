import json
from enum import StrEnum

from keboola.component.exceptions import UserException
from pydantic import BaseModel, ConfigDict, Field

FILE_JSON_LABELS = "labels.json"


class ConfigurationException(UserException):
    pass


class Destination(BaseModel):
    model_config = ConfigDict(extra="ignore")

    table_name: str = ""
    incremental_loading: bool = True
    primary_key: list[str] | None = None
    primary_key_existing: list[str] | None = None


class TimeRange(BaseModel):
    model_config = ConfigDict(extra="ignore")

    period: str = ""
    date_from: str = ""
    date_to: str = ""


class ReportSettings(BaseModel):
    model_config = ConfigDict(extra="ignore")

    report_type: str = ""
    dimensions: list[str] | None = None
    metrics: list[str] | None = None


class InputVariant(StrEnum):
    REPORT_SPEC = "report_specification"
    REPORT_TEMPLATE = "report_template_id"
    REPORT_IDS = "existing_report_ids"
    METADATA = "metadata"


class Configuration(BaseModel):
    model_config = ConfigDict(extra="ignore")

    profiles: list[str]
    input_variant: InputVariant
    destination: Destination = Field(default_factory=Destination)
    metadata: list[str] = Field(default_factory=list)
    time_range: TimeRange = Field(default_factory=TimeRange)
    report_specification: ReportSettings = Field(default_factory=ReportSettings)
    existing_report_ids: list[str] = Field(default_factory=list)
    report_template_id: str = ""
    debug: bool = False

    @classmethod
    def load_from_dict(cls, configuration: dict) -> "Configuration":
        json_conf = json.dumps(configuration)
        json_conf = json_conf.replace('"#', '"pswd_')
        return cls.model_validate(json.loads(json_conf))
