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
    table_name: str
    incremental_loading: bool = True
    primary_key: list[str] = None
    primary_key_existing: list[str] = None


@dataclass
class TimeRange:
    period: str
    date_from: str = ""
    date_to: str = ""


@dataclass
class ReportSettings:
    report_type: str = ""
    dimensions: list[str] = None
    metrics: list[str] = None


class ConfigurationBase:

    @staticmethod
    def fromDict(parameters: dict):
        return dataconf.dict(parameters, Configuration, ignore_unexpected=True)
        pass


class InputVariant(str, Enum):
    REPORT_SPEC = "report_specification"
    REPORT_TEMPLATE = "report_template_id"
    REPORT_IDS = "existing_report_ids"


@dataclass
class Configuration(ConfigurationBase):
    profiles: list[str]
    input_variant: InputVariant
    destination: Destination
    time_range: TimeRange
    report_specification: ReportSettings = field(default_factory=lambda: ConfigTree({}))
    existing_report_ids: list[str] = field(default_factory=lambda: "")
    report_template_id: str = None

    debug: bool = False

    # def __eq__(self, other):
    #     if self.input_variant == "entry_id":
    #         return self.entry_id == other.entry_id
    #     else:
    #         return self.report_specification == other.report_specification


if __name__ == '__main__':
    json_conf_1 = """
    {
      "profiles": ["8467304", "8653652"]
      "input_variant": "report_specification",
      "time_range": {
        "period": "LAST_90_DAYS"
        "date_from": "yesterday"
        "date_to": "dneska"
      },
      "report_specification": {
        "report_type": "STANDARD",
        "dimensions": ["FILTER_ADVERTISER","FILTER_ADVERTISER_NAME","FILTER_BROWSER"],
        "metrics": ["METRIC_CLICKS", "METRIC_COUNTERS", "METRIC_ENGAGEMENTS"]
      },
      "destination": {
        "table_name": "report_row_1.csv",
        "incremental_loading": true,
        "primary_key": [
          "FILTER_ADVERTISER",
          "FILTER_BROWSER"
        ]
      },
      "debug": true,
      "dalsi_parametr": 12
    }
    """

    json_conf_2 = """
    {
      "profiles": ["8467304", "8653652"]
      "input_variant": "report_template_id",
      "time_range": {
        "period": "LAST_90_DAYS"
        "date_from": "yesterday"
        "date_to": "dneska"
      },
      "report_template_id": "777777:5000000",
      "destination": {
        "table_name": "report_row_1.csv",
        "incremental_loading": true,
        "primary_key": [
          "FILTER_ADVERTISER",
          "FILTER_BROWSER"
        ]
      },
      "debug": true,
      "dalsi_parametr": 12
    }
    """

    json_conf_3 = """
    {
      "profiles": ["8467304", "8653652"]
      "input_variant": "existing_report_ids",
      "time_range": {
        "period": "LAST_90_DAYS"
        "date_from": "yesterday"
        "date_to": "dneska"
      },
      "existing_report_ids": ["3333333:88888888","111111:22222222"]
      "destination": {
        "table_name": "report_row_1.csv",
        "incremental_loading": true,
        "primary_key": [
          "FILTER_ADVERTISER",
          "FILTER_BROWSER"
        ]
      },
      "debug": true,
      "dalsi_parametr": 12
    }
    """

    cf1 = dataconf.loads(json_conf_1, Configuration, ignore_unexpected=True)
    cf2 = dataconf.loads(json_conf_2, Configuration, ignore_unexpected=True)
    cf3 = dataconf.loads(json_conf_3, Configuration, ignore_unexpected=True)
    pass

    # print(f'Equality cf1 == cf2 {cf1 == cf2}')

    # pars = {
    #     "input_variant": "report_settings",
    #     "time_range": {
    #         "period": "LAST_90_DAYS",
    #         "date_from": "yesterday",
    #         "date_to": "dneska"
    #     },
    #     "destination": {
    #         "table_name": "report_row_1.csv",
    #         "primary_key": [
    #             "FILTER_ADVERTISER",
    #             "FILTER_BROWSER"
    #         ],
    #         "incremental_loading": True,
    #     }
    # }
    #
    # cf3 = Configuration.fromDict(pars)

    pass
