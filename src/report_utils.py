map_report_type_2_criteria = {
    'STANDARD': 'criteria',
    'REACH': 'reachCriteria',
    'FLOODLIGHT': 'floodlightCriteria',
    'PATH': 'pathCriteria',
    'PATH_ATTRIBUTION': 'pathAttributionCriteria'
}

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


def prepare_patch(report_def, report) -> dict:
    criteria_attribute = map_report_type_2_criteria[report_def['type']]
    body = {}
    if report_def['name'] != report['name']:
        body['name'] = report_def['name']
    if report_def['type'] != report['type']:
        body['type'] = report_def['type']
    if report_def['fileName'] != report['fileName']:
        body['fileName'] = report_def['fileName']
    if report_def['format'] != report['format']:
        body['format'] = report_def['format']

    if report_def['type'] != report['type']:
        report_criteria_attribute = map_report_type_2_criteria[report['type']]
        body[report_criteria_attribute] = None
        body[criteria_attribute] = report_def[criteria_attribute].copy()
        return body

    report_criteria = special_copy(report[criteria_attribute])
    def_criteria = report_def[criteria_attribute]

    date_range = {}
    def_range = def_criteria['dateRange']
    rep_range = report_criteria['dateRange']
    for key, value in def_range.items():
        rval = rep_range.get(key)
        if not rval or value != rval:
            date_range[key] = value
    for key in rep_range:
        if key not in def_range:
            date_range[key] = None
    if date_range:
        date_range = {'dateRange': date_range}

    others = {}
    for key, value in def_criteria.items():
        if key == 'dateRange':
            continue
        rval = report_criteria.get(key)
        if not rval or value != rval:
            others[key] = value
    for key in report_criteria:
        if key not in def_criteria:
            others[key] = None

    others = others | date_range
    if others:
        body[criteria_attribute] = others

    return body


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

    pass
