from configuration import Configuration

map_report_type_2_criteria = {
    'STANDARD': 'criteria',
    'REACH': 'reachCriteria',
    'FLOODLIGHT': 'floodlightCriteria',
    'PATH': 'pathCriteria',
    'PATH_ATTRIBUTION': 'pathAttributionCriteria'
}


def create_date_range_from_cfg(cfg: Configuration) -> dict:
    # TODO: complete all possible options - start / end dates
    date_range = {
        'relativeDateRange': cfg.time_range.period
    }
    return date_range


def create_report_definition(cfg: Configuration, name: str) -> dict:
    """Method creates a report definition that can be used in an API call
        to insert a new report. It uses current configuration only.
        Method makes sense for 'report_specification' input variant only.
    """
    specification = cfg.report_specification
    report = {
        'name': name,
        'type': specification.report_type,
        'fileName': 'kebola-ex-file',
        'format': 'CSV'
    }
    date_range = create_date_range_from_cfg(cfg)
    if cfg.input_variant == 'report_specification':
        criteria = {
            'dateRange': date_range,
            'dimensions': [{'name': name} for name in specification.dimensions] if specification.dimensions else [],
            'metricNames': specification.metrics if specification.metrics else []
        }
        report[map_report_type_2_criteria[specification.report_type]] = criteria
    return report


def create_report_definition_from_existing(cfg: Configuration, report_instance: dict, keep_time: bool = False) -> dict:
    """Method creates a report definition that can be used in an API call
        to insert a new report. It uses criteria and report type of existing report instance.
        It then uses current configuration to add dateRange and current runtime environment
        to generate report's name.
    """
    report = {
        'name': report_instance['name'],
        'type': report_instance['type'],
        'fileName': report_instance['fileName'],
        'format': report_instance['format']
    }
    # date_range =  if keep_time else create_date_range_from_cfg(cfg)
    criteria_attribute = map_report_type_2_criteria[report_instance['type']]
    criteria = special_copy(report_instance[criteria_attribute])
    report[criteria_attribute] = criteria
    if not keep_time:
        # TODO: code to create time from CFG
        pass
    return report


IGNORED_KEYS = ['kind', 'etag']


def special_copy_list(items: list) -> list:
    result = []
    for item in items:
        if type(item) is list:
            result.append(special_copy_list(item))
        elif type(item) is dict:
            result.append(special_copy(item))
        else:
            if item in IGNORED_KEYS:
                continue
            result.append(item)
    return result


def special_copy(report: dict) -> dict:
    result = {}
    for key, value in report.items():
        if key in IGNORED_KEYS:
            continue
        if type(value) is list:
            result[key] = special_copy_list(value)
        elif type(value) is dict:
            result[key] = special_copy(value)
        else:
            result[key] = value
    return result


if __name__ == '__main__':
    src = {
        "id": "1090921139",
        "ownerProfileId": "8653652",
        "accountId": "1254895",
        "name": "keboola_generated_8888_646464_456456",
        "fileName": "kebola-ex-file",
        "kind": "dfareporting#report",
        "type": "STANDARD",
        "etag": "\"5bd9de8ab7fb765f53f347c65123297cc7869367\"",
        "lastModifiedTime": "1683206375000",
        "format": "CSV",
        "criteria": {
            "dateRange": {
                "startDate": "2023-01-01",
                "endDate": "2023-04-29",
                "kind": "dfareporting#dateRange"
            },
            "dimensions": [
                {
                    "name": "activity",
                    "kind": "dfareporting#sortedDimension"
                },
                {
                    "name": "country",
                    "kind": "dfareporting#sortedDimension"
                },
                {
                    "name": "environment",
                    "kind": "dfareporting#sortedDimension"
                }
            ],
            "metricNames": [
                "costPerClick",
                "clicks"
            ],
            "dimensionFilters": [
                {
                    "dimensionName": "activity",
                    "id": "sfds",
                    "matchType": "EXACT",
                    "kind": "dfareporting#dimensionValue",
                    "etag": "\"17c6732718b13af837839ea084ffae812cc07134\""
                }
            ]
        }
    }
    dst = special_copy(src)

    rep_def1 = create_report_definition_from_existing(cfg=None, report_instance=src)
    rep_def2 = create_report_definition_from_existing(cfg=None, report_instance=src)

    pass
