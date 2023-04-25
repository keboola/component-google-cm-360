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

    def create_report_from_specification(self):
        specification = self.cfg.report_specification
        report = {
            'name': 'kebola-ex-generated',
            'type': specification.report_type,
            'fileName': 'kebola-ex-file',
            'format': 'CSV'
        }

        # end_date = date.today()
        # start_date = end_date - timedelta(days=30)
        # end_date = end_date.strftime('%Y-%m-%d')
        # start_date = start_date.strftime('%Y-%m-%d')
        date_range = {
            'relativeDateRange': self.cfg.time_range.period
        }
        criteria = {
            'dateRange': date_range,
            'dimensions': [{'name': name} for name in specification.dimensions],
            'metricNames': specification.metrics,
            'dateRange': date_range
        }
        report[map_report_type_2_criteria[specification.report_type]] = criteria
        return report

    def run(self):
        """
        BDM example auth
        """

        logging.debug(self.configuration.parameters)
        self.cfg = Configuration.fromDict(self.configuration.parameters)
        logging.debug(self.cfg)

        prev_state = self.get_state_file()
        print(prev_state)

        cur_state = dict(
            reports={},
            configuration=json.loads(dataconf.dumps(self.cfg, out="json"))
        )
        self.write_state_file(cur_state)

        report = self.create_report_from_specification()

        client = self._get_google_client()

        inserted_report = client.create_report(report=report, profile_id=self.cfg.profiles[0])

        report_file = client.run_report(report_id=inserted_report['id'], profile_id=self.cfg.profiles[0])

        while True:
            report_file = client.report_status(report_id=report_file['reportId'], file_id=report_file['id'])
            status = report_file['status']
            if status == 'REPORT_AVAILABLE':
                client.get_report_file(report_id=report_file['reportId'], file_id=report_file['id'])
                break
            time.sleep(20)

        pass

        # dimensions that produce errors (invalid combinationof dimension and filter dimensions):
        # 'keyword', 'mediaType'
        # dimensions = ['advertiser', 'placement', 'platformType', 'site']
        # for dimension in dimensions:
        #     client.list_dimension_values(dimension, start_date, end_date, profile_id='8467304')
        # inserted_report = client.create_report(report, profile_id='8467304')
        # report_file = client.run_report(report_id='1079627581', profile_id='8467304')
        # report_file = client.report_status(report_id='1079627581', file_id='4080792184')
        # client.get_report_file(report_id='1079627581', file_id='4080792184')
        pass

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

    def generate_query_name(self):
        # TODO: Currently keboola has an inssue: It does not pass row-id in variables, we use a workaround:
        import os
        configrow_id = os.getenv('KBC_CONFIGROWID', 'xxxxxx')
        return 'keboola_generated_' + self.environment_variables.project_id + '_' + \
               self.environment_variables.config_id + '_' + \
               configrow_id

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
        client = self._get_google_client()
        reports = client.list_reports()
        reports_w_labels = [SelectElement(value=report['id'], label=f'{report["name"]} ({report["id"]})')
                            for report in reports['items']]
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
