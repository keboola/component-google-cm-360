"""
Template Component main class.

"""
# from typing import List, Tuple
import json
import logging
import time
import os

import dataconf
import requests
from keboola.component.base import ComponentBase, sync_action
from keboola.component.exceptions import UserException
from keboola.component.sync_actions import SelectElement

from configuration import Configuration
from google_cm360 import GoogleCM360Client, translate_filters
from configuration import FILE_JSON_LABELS
from report_utils import special_copy, prepare_patch


map_report_type_2_compatible_section = {
    'STANDARD': 'reportCompatibleFields',
    'REACH': 'reachReportCompatibleFields',
    'FLOODLIGHT': 'floodlightReportCompatibleFields',
    'PATH': 'pathReportCompatibleFields',
    'PATH_ATTRIBUTION': 'pathAttributionReportCompatibleFields'
}

map_report_type_2_criteria = {
    'STANDARD': 'criteria',
    'REACH': 'reachCriteria',
    'FLOODLIGHT': 'floodlightCriteria',
    'PATH': 'pathCriteria',
    'PATH_ATTRIBUTION': 'pathAttributionCriteria'
}


def _load_attribute_labels_from_json(report_type, attribute):
    all_labels = None
    try:
        path = os.path.join(os.path.dirname(__file__), FILE_JSON_LABELS)
        with open(path, mode='r') as file:
            all_labels = json.load(fp=file)
        return all_labels.get(report_type).get(attribute)
    except Exception:
        return {}


class Component(ComponentBase):
    """
        Extends base class for general Python components. Initializes the CommonInterface
        and performs configuration validation.

        For easier debugging the data folder is picked up by default from `../data` path,
        relative to working directory.

        If `debug` parameter is present in the `config.json`, the default logger is set to verbose DEBUG mode.
    """

    def __init__(self):
        super().__init__()
        self.cfg: Configuration = None

    def create_date_range(self) -> dict:
        # TODO: complete all possible options - start / end dates
        date_range = {
            'relativeDateRange': self.cfg.time_range.period
        }
        return date_range

    def create_report_definition_from_specification(self):
        """Method creates a report definition that can be used in an API call
            to insert a new report. It uses current configuration only.
            Method makes sense for 'report_specification' input variant only.
        """
        specification = self.cfg.report_specification
        report = {
            'name': self.generate_report_name(),
            'type': specification.report_type,
            'fileName': 'kebola-ex-file',
            'format': 'CSV'
        }
        date_range = self.create_date_range()
        if self.cfg.input_variant == 'report_specification':
            criteria = {
                'dateRange': date_range,
                'dimensions': [{'name': name} for name in specification.dimensions] if specification.dimensions else [],
                'metricNames': specification.metrics if specification.metrics else []
            }
            report[map_report_type_2_criteria[specification.report_type]] = criteria
        return report

    def create_report_definition_from_instance(self, report_instance: dict) -> dict:
        """Method creates a report definition that can be used in an API call
            to insert a new report. It uses criteria and report type of existing report instance.
            It then uses current configuration to add dateRange and current runtime environment
            to generate report's name.
        """
        report = {
            'name': self.generate_report_name(),
            'type': report_instance['type'],
            'fileName': 'kebola-ex-file',
            'format': 'CSV'
        }
        date_range = self.create_date_range()
        criteria_attribute = map_report_type_2_criteria[report_instance['type']]
        report[criteria_attribute] = special_copy(report_instance[criteria_attribute])
        report[criteria_attribute]['dateRange'] = date_range
        if report_instance.get('delivery'):
            report['delivery'] = special_copy(report_instance.get('delivery'))
        return report

    @staticmethod
    def equate_report_criteria(r1: dict, r2: dict):
        """Method returns True if the 2 definitions equates in type, dimensions and metrics"""
        if r1.get('type') != r2.get('type'):
            return False
        criteria_1 = map_report_type_2_criteria.get(r1.get('type')) if r1.get('id') else 'criteria'
        criteria_2 = map_report_type_2_criteria.get(r2.get('type')) if r2.get('id') else 'criteria'
        if r1[criteria_1]['metricNames'] != r2[criteria_2]['metricNames']:
            return False
        if [it['name'] for it in r1[criteria_1]['dimensions']] != [it['name'] for it in r2[criteria_2]['dimensions']]:
            return False
        return True

    @staticmethod
    def equate_report_time_range(r1: dict, r2: dict):
        """Method returns True if the 2 definitions equates in dateRange"""
        criteria_1 = map_report_type_2_criteria.get(r1.get('type')) if r1.get('id') else 'criteria'
        criteria_2 = map_report_type_2_criteria.get(r2.get('type')) if r2.get('id') else 'criteria'
        if r1[criteria_1]['dateRange']['relativeDateRange'] != r2[criteria_2]['dateRange']['relativeDateRange']:
            return False
        if r1[criteria_1]['dateRange']['relativeDateRange'] != 'CUSTOM_DATES':
            return True
        return r1[criteria_1]['dateRange']['startDate'] == r2[criteria_2]['dateRange']['startDate'] \
            and r1[criteria_1]['dateRange']['endDate'] == r2[criteria_2]['dateRange']['endDate']

    @staticmethod
    def patch_time_range(report_tgt: dict, report_src: dict):
        """Method modifies report criteria/dateRange according to source report so that they match.
        It is possible to combine a report definition against and report instance.
        We differentiate the two options by presence of 'id' attribute which is present
        at report instance but is omitted at report definition.
        """
        criteria_tgt = map_report_type_2_criteria.get(report_tgt.get('type'))
        criteria_src = map_report_type_2_criteria.get(report_src.get('type'))
        report_tgt[criteria_tgt]['dateRange'] = report_tgt[criteria_src]['dateRange'].copy()

    def run(self):
        """Main extractor method - it reads current configuration, run report(s)
        and collects reported data into CSV tables.

        Main steps
        - Prepare a list reports
        - Run all prepared reports
        - Wait for completion of reports
        - Collect reported data into an output table(s)
        """

        logging.debug(self.configuration.parameters)
        self.cfg = Configuration.fromDict(self.configuration.parameters)
        logging.debug(self.cfg)

        prev_state = self.get_state_file()
        existing_reports = prev_state.get('reports') if prev_state else {}
        current_reports = {}

        """
            Prepare a list reports
                Create a report definition based on configuration.
                (either from report specification or from existing report template)
                for each profile:
                    Check whether re-usable report exists in current state
                    If a report was found apply a patch if necessary (date changed...)
                    If no report was found create a new one based on report definition
        """
        client = self._get_google_client()
        if self.cfg.input_variant == 'report_specification' or self.cfg.input_variant == 'report_template_id':

            if self.cfg.input_variant == 'report_specification':
                logging.debug('Input variant: report_specification')
                report_def = self.create_report_definition_from_specification()
            elif self.cfg.input_variant == "report_template_id":
                logging.debug('Input variant: report_template_id')
                src_profile_id, src_report_id = self.cfg.report_template_id.split(':')
                report_template = client.get_report(profile_id=src_profile_id, report_id=src_report_id)
                report_def = self.create_report_definition_from_instance(report_template)

            for profile_id in self.cfg.profiles:
                report_candidate_id = existing_reports.get(profile_id)
                if report_candidate_id:
                    # We have a candidate report ID - check whether it is available, patch it if necessary
                    report = client.get_report(profile_id=profile_id, report_id=report_candidate_id, ignore_error=True)
                    if not report:
                        # Report is no longer available - remove it from the state and cancel the candidate ID
                        existing_reports.pop(profile_id)
                        report_candidate_id = None
                    else:
                        # Report is available - check whether it needs a patch
                        logging.debug(f'Report will be re-used {report_candidate_id} for {profile_id}')
                        patch_body = prepare_patch(report_def, report)
                        if patch_body:
                            logging.debug(f'Report will be patched {patch_body}')
                            client.patch_report(report=patch_body, profile_id=profile_id, report_id=report_candidate_id)
                if not report_candidate_id:
                    # We could not use any existing report, we must create one
                    report = client.create_report(report_def, profile_id=profile_id)
                    report_candidate_id = report['id']
                    logging.debug(f'New report {report_candidate_id} created for {profile_id}')

                # Register a report ID in current state
                current_reports[profile_id] = report_candidate_id
        else:
            # ValueError('Input variant existing_report_ids not yet implemented')
            # TODO: Check that each report specified exists
            pass

        """
            We now have current set of reports in current_reports dictionary
            Let's remove any report that will not be re-used.
        """
        for profile_id, report_id in existing_reports.items():
            if profile_id not in current_reports or report_id != current_reports[profile_id]:
                client.delete_report(profile_id=profile_id, report_id=report_id, ignore_error=True)

        self.write_state_file(state_dict=dict(reports=current_reports))

        if self.cfg.input_variant == 'report_specification' or self.cfg.input_variant == 'report_template_id':
            reports_2_run = [dict(profile_id=key, report_id=value) for key, value in current_reports.items()]
        else:
            reports_2_run = [dict(profile_id=item.split(':')[0], report_id=item.split(':')[1])
                             for item in self.cfg.existing_report_ids]

        # Run all reports
        report_files = []
        for item in reports_2_run:
            profile_id = item['profile_id']
            report_id = item['report_id']
            report_file = client.run_report(profile_id=profile_id, report_id=report_id)
            logging.info(f'Report {report_id} started')
            report_files.append(report_file)

        time.sleep(5)

        # Wait for report runs completion
        while report_files:
            logging.info(f'Waiting for {len(report_files)}')
            wait_files = report_files.copy()
            report_files = []
            was_processed = False
            for report_file in wait_files:
                file = client.report_status(report_id=report_file['reportId'], file_id=report_file['id'])
                status = file['status']
                """Available statuses:
                    PROCESSING
                    REPORT_AVAILABLE
                    FAILED
                    CANCELLED
                    QUEUED
                """
                if status == 'REPORT_AVAILABLE':
                    # TODO: pass on as parameter or generate unique file name where to write data
                    client.get_report_file(report_id=file['reportId'], file_id=file['id'], report_file=file)
                    logging.info(f'Report {file["reportId"]} saved')
                    was_processed = True
                    continue
                if status == 'FAILED' or status == 'CANCELLED':
                    logging.info(f'Report {file["reportId"]} failed or canceled')
                    continue
                logging.info(f'Report {file["reportId"]} : {status}')
                report_files.append(report_file)

            if not report_files:
                break
            if was_processed:
                time.sleep(1)
            else:
                time.sleep(15)

        pass
        # TODO: Process result files into a table - generate manifest

    def _get_google_client(self):
        client = GoogleCM360Client(
            self.configuration.oauth_credentials.appKey,
            self.configuration.oauth_credentials.appSecret,
            self.configuration.oauth_credentials.data
        )
        return client

    @staticmethod
    def download_file(url: str, result_file_path: str):
        # avoid loading all into memory
        res = requests.get(url, stream=True, timeout=180)
        res.raise_for_status()

        with open(result_file_path, 'wb') as out:
            for chunk in res.iter_content(chunk_size=8192):
                out.write(chunk)

    @staticmethod
    def extract_csv_from_raw(raw_file: str, csv_file: str):
        with open(raw_file, 'r') as src, open(csv_file, 'w') as dst:
            while True:
                line = src.readline()
                if not line or line.startswith(',') or line == '\n':
                    break
                dst.write(line)

            pass

    def write_report(self, contents_url: str):
        """

        Args:
            contents_url: URL where Google stored report contents

        Returns:

        """
        pks = translate_filters(self.cfg.destination.primary_key)
        result_table = self.create_out_table_definition(f"{self.cfg.destination.table_name}.csv",
                                                        primary_key=pks,
                                                        incremental=self.cfg.destination.incremental_loading)
        self.write_manifest(result_table)

        raw_output_file = self.files_out_path + '/' + result_table.name + '.raw.txt'
        self.download_file(contents_url, raw_output_file)
        self.extract_csv_from_raw(raw_output_file, result_table.full_path)

    def save_state(self, report_response):
        cur_state = dict(
            report=report_response,
            configuration=json.loads(dataconf.dumps(self.cfg, out="json"))
        )
        self.write_state_file(cur_state)

    def generate_report_name(self):
        return 'keboola_generated_' + self.environment_variables.project_id + '_' + \
               self.environment_variables.config_id + '_' + \
               self.environment_variables.config_row_id

    @sync_action('load_profiles')
    def load_profiles(self):
        client = self._get_google_client()
        profiles = client.list_profiles()
        prof_w_labels = [SelectElement(value=profile[0], label=f'{profile[1]} ({profile[0]})') for profile in profiles]
        return prof_w_labels

    def _load_attribute_values(self, attribute: str):

        report_type = self.configuration.parameters.get('report_specification').get('report_type')
        if not report_type or report_type not in map_report_type_2_compatible_section:
            raise ValueError('No or invalid report_type')

        dims = _load_attribute_labels_from_json(report_type, attribute)
        client = self._get_google_client()
        ids = client.list_compatible_fields(report_type=report_type,
                                            compat_fields=map_report_type_2_compatible_section[report_type],
                                            attribute=attribute)

        # assign labels to attribute ids an generate a response to action
        result = []
        for id in ids:
            label = dims.get(id)
            if not label:
                label = id
            result.append(SelectElement(value=id, label=label))
        return result
        # return [SelectElement(value=k, label=v) for k, v in dims.items()]

    @sync_action('load_dimensions')
    def load_dimensions_standard(self):
        return self._load_attribute_values('dimensions')

    @sync_action('load_metrics')
    def load_metrics(self):
        return self._load_attribute_values('metrics')

    @sync_action('load_reports')
    def load_reports(self):

        profile_ids = self.configuration.parameters.get('profiles')
        if not profile_ids or len(profile_ids) == 0:
            raise ValueError('No profiles were specified')
        client = self._get_google_client()
        list_profiles = client.list_profiles()
        profiles_2_names = {it[0]: it[1] for it in list_profiles}
        reports_w_labels = []
        for profile_id in profile_ids:
            reports = client.list_reports(profile_id=profile_id)
            reports_w_labels.extend([SelectElement(value=f'{profile_id}:{report["id"]}',
                                                   label=f'[{profiles_2_names[profile_id]}] '
                                                         f'{report["name"]} ({profile_id}:{report["id"]})')
                                     for report in reports])
        return reports_w_labels

    @sync_action('dummy')
    def dummy(self):

        rep_def = {
            # "id": "1089010172",
            # "ownerProfileId": "8653652",
            # "accountId": "1254895",
            "name": "keboola_generated_8888_646464_456456",
            "fileName": "kebola-ex-file",
            "kind": "dfareporting#report",
            "type": "STANDARD",
            # "etag": "\"da0c96863a6df56073700ac781ecb155ddd92801\"",
            # "lastModifiedTime": "1682672902000",
            "format": "CSV",
            "criteria": {
                "dateRange": {
                    "relativeDateRange": "LAST_7_DAYS",
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
                        # "value": "fdsd",
                        "id": "sfds",
                        "matchType": "EXACT",
                        "kind": "dfareporting#dimensionValue",
                        # "etag": "fdsfdsfds"
                    }
                ],
                # "activities": {
                #
                # },
                # "customRichMediaEvents": {
                #
                # }
            }
        }

        client = self._get_google_client()
        report = client.create_report(report=rep_def, profile_id='8653652')
        return report

    @sync_action('dummy1')
    def dummy1(self):
        rep_def = {
            "criteria": {
                "dateRange": {
                    "relativeDateRange": None,
                    "startDate": "2023-01-01",
                    "endDate": "2023-04-29"
                },
                "dimensions": [
                    {
                        "name": "country",
                    }

                ],
                #     [
                #     {
                #         "name": "activity",
                #         "kind": "dfareporting#sortedDimension"
                #     }
                # ],
                "dimensionFilters": [
                    {
                        "dimensionName": "activity",
                        "id": "sfds",
                        "matchType": "EXACT",
                        "kind": "dfareporting#dimensionValue",
                        "etag": "\"17c6732718b13af837839ea084ffae812cc07134\""
                    },
                    {
                        "dimensionName": "country",
                        "id": "sfds",
                        "matchType": "EXACT"
                    }
                ]
            },
            "name": "keboola_generated_8888_646464_456456"
        }
        client = self._get_google_client()
        report = client.patch_report(report=rep_def, report_id='1090921139', profile_id='8653652')
        return report


"""
        Main entrypoint
"""
if __name__ == "__main__":
    try:
        comp = Component()
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
