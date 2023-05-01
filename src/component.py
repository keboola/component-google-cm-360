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

    def create_date_range_from_cfg(self) -> dict:
        # TODO: complete all possible options - start / end dates
        date_range = {
            'relativeDateRange': self.cfg.time_range.period
        }
        return date_range

    def create_report_definition(self):
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
        date_range = self.create_date_range_from_cfg()
        if self.cfg.input_variant == 'report_specification':
            criteria = {
                'dateRange': date_range,
                'dimensions': [{'name': name} for name in specification.dimensions] if specification.dimensions else [],
                'metricNames': specification.metrics if specification.metrics else []
            }
            report[map_report_type_2_criteria[specification.report_type]] = criteria
        return report

    def create_report_definition_from_existing(self, report_instance: dict) -> dict:
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
        date_range = self.create_date_range_from_cfg()
        criteria_attribute = map_report_type_2_criteria[report_instance['type']]
        criteria = {
            'dateRange': date_range,
            'dimensions': [{'name': item['name']} for item in report_instance[criteria_attribute]['dimensions']],
            'metricNames': [item for item in report_instance[criteria_attribute]['metricNames']]
        }
        report[criteria_attribute] = criteria
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
        """
        BDM example auth
        """

        logging.debug(self.configuration.parameters)
        self.cfg = Configuration.fromDict(self.configuration.parameters)
        logging.debug(self.cfg)

        prev_state = self.get_state_file()
        existing_reports = prev_state.get('reports') if prev_state else {}
        current_reports = {}

        client = self._get_google_client()
        if self.cfg.input_variant == 'report_specification' or self.cfg.input_variant == 'report_template_id':
            if self.cfg.input_variant == 'report_specification':
                report_def = self.create_report_definition()
            elif self.cfg.input_variant == "report_template_id":
                src_profile_id, src_report_id = self.cfg.report_template_id.split(':')
                report_template = client.get_report(profile_id=src_profile_id, report_id=src_report_id)
                report_def = self.create_report_definition_from_existing(report_template)
            for profile_id in self.cfg.profiles:
                report_candidate_id = existing_reports.get(profile_id)
                if report_candidate_id:
                    report = client.get_report(profile_id=profile_id, report_id=report_candidate_id, ignore_error=True)
                    if not report:
                        existing_reports.pop(profile_id)
                        report_candidate_id = None
                    else:
                        if not self.equate_report_criteria(report, report_def):
                            client.delete_report(profile_id=profile_id, report_id=report_candidate_id)
                            existing_reports.pop(profile_id)
                            report_candidate_id = None
                        else:
                            if not self.equate_report_time_range(report, report_def):
                                self.patch_time_range(report, report_def)
                                client.patch_report(report, profile_id=profile_id, report_id=report_candidate_id)
                if not report_candidate_id:
                    # TODO: Crash with more explanation if create_report() fails
                    report = client.create_report(report_def, profile_id=profile_id)
                    report_candidate_id = report['id']
                current_reports[profile_id] = report_candidate_id
        else:
            ValueError('Input variant existing_report_ids not yet implemented')

        # Now we have a list of reports to be run in current_reports.
        # It is time to clean up - any report in old status not in current_reports may be deleted.
        for profile_id, report_id in existing_reports.items():
            if profile_id not in current_reports or report_id != current_reports[profile_id]:
                client.delete_report(profile_id=profile_id, report_id=report_id, ignore_error=True)

        # Run all reports
        report_files = []
        for profile_id, report_id in current_reports.items():
            report_file = client.run_report(profile_id=profile_id, report_id=report_id)
            report_files.append(report_file)

        # Wait for report runs completion

        while report_files:
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
                    was_processed = True
                if status == 'FAILED' or 'CANCELLED':
                    continue
                report_files.append(report_file)
            if was_processed:
                time.sleep(10)
            else:
                time.sleep(30)
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

    def get_existing_report_id(self, client):
        """ Retrieves existing query ID

        Decide whether we may use already existing query generated previously.
        If state contains configuration identical to current configuration we check
        that correspondent query still exists in dv360 and if so we use its id.
        In any other case, we return None and caller will use a new query (either generated or supplied externally).

        Args:
            client: Service used to check query availability

        Returns: Query id if found else None

        """
        prev_state = self.get_state_file()
        if not prev_state.get('configuration') or not prev_state.get('report'):
            return None
        prev_report_id = prev_state['report']['key']['queryId']
        prev_cfg = Configuration.fromDict(prev_state.get('configuration'))
        if prev_cfg == self.cfg:
            # check for query existence
            q = client.get_query(prev_report_id)
            return prev_report_id if q else None
        # TODO: think over: delete orphan report_id - check input_variant was not entry_id case
        return None

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

    # @sync_action('dummy')
    # def dummy(self):
    #     client = self._get_google_client()
    #     report = client.get_report(report_id='1087268864')
    #     return []


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
