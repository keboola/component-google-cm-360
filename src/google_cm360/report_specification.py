REPORT_KEBOOLA_BASE_STRUCTURE = {"name": "kebola-ex-generated",
                                 "fileName": "kebola-ex-file",
                                 "kind": "dfareporting#report",
                                 "type": "STANDARD",
                                 "format": "CSV",
                                 "criteria": {}
                                 }

MAP_REPORT_TYPE_2_COMPATIBLE_SECTION = {
    'STANDARD': 'reportCompatibleFields',
    'REACH': 'reachReportCompatibleFields',
    'FLOODLIGHT': 'floodlightReportCompatibleFields',
    'PATH': 'pathReportCompatibleFields',
    'PATH_ATTRIBUTION': 'pathAttributionReportCompatibleFields'
}

MAP_REPORT_TYPE_2_CRITERIA = {
    'STANDARD': 'criteria',
    'REACH': 'reachCriteria',
    'FLOODLIGHT': 'floodlightCriteria',
    'PATH': 'pathCriteria',
    'PATH_ATTRIBUTION': 'pathAttributionCriteria'
}


class CsvReportSpecification:

    def __init__(self, report_dict: dict):
        self.report_representation = report_dict

    @classmethod
    def custom_from_specification(cls, report_name: str, report_type: str, date_range: dict, dimensions, metrics):
        report = REPORT_KEBOOLA_BASE_STRUCTURE.copy()
        report['name'] = report_name
        report['type'] = report_type
        criteria = {
            'dateRange': date_range,
            'dimensions': dimensions or [],
            'metricNames': metrics or []
        }
        report[MAP_REPORT_TYPE_2_CRITERIA[report_type]] = criteria

        return cls(report)

    def modify_date_range(self, date_range: dict):
        self.report_criteria['dateRange'] = date_range

    def update_template_commons(self, report_id: str, profile_id: str, account_id: str):
        self.report_id = report_id
        self.profile_id = profile_id
        self.account_id = account_id

    def prepare_update_body(self, report_definition):
        updated_report_body: dict = report_definition.report_representation.copy()
        updated_report_body['id'] = self.report_id
        updated_report_body['ownerProfileId'] = self.profile_id
        updated_report_body['lastModifiedTime'] = self.report_representation['lastModifiedTime']
        updated_report_body['accountId'] = self.report_representation['accountId']
        return updated_report_body

    def prepare_insert_body(self):
        new_report_body = self.report_representation.copy()
        for key in ['id', 'ownerProfileId', 'lastModifiedTime', 'etag']:
            new_report_body.pop(key, None)
        return new_report_body

    def get_dimensions_names(self):
        dimensions = [item['name'] for item in self.report_criteria['dimensions']]
        return dimensions

    def get_metrics_names(self):
        metrics = self.report_criteria['metricNames'].copy()
        return metrics

    @property
    def report_id(self) -> str:
        return self.report_representation['id']

    @report_id.setter
    def report_id(self, report_id: str):
        self.report_representation['id'] = report_id

    @property
    def profile_id(self) -> str:
        return self.report_representation['ownerProfileId']

    @profile_id.setter
    def profile_id(self, profile_id: str):
        self.report_representation['ownerProfileId'] = profile_id

    @property
    def account_id(self) -> str:
        return self.report_representation['accountId']

    @account_id.setter
    def account_id(self, account_id: str):
        self.report_representation['accountId'] = account_id

    @property
    def report_criteria(self) -> dict:
        criteria_key = MAP_REPORT_TYPE_2_CRITERIA[self.report_representation["type"]]
        return self.report_representation[criteria_key]

    @report_criteria.setter
    def report_criteria(self, criteria: dict):
        criteria_key = MAP_REPORT_TYPE_2_CRITERIA[self.report_representation["type"]]
        self.report_representation[criteria_key] = criteria
