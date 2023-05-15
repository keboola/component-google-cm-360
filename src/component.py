"""
Template Component main class.

"""
# from typing import List, Tuple
import json
import logging
import os
import time
from typing import Dict, List

import dataconf
import requests
from keboola.component.base import ComponentBase, sync_action
from keboola.component.exceptions import UserException
from keboola.component.sync_actions import SelectElement

from configuration import Configuration, InputVariant
from configuration import FILE_JSON_LABELS
from google_cm360 import GoogleCM360Client, translate_filters
from google_cm360.report_specification import CsvReportSpecification, MAP_REPORT_TYPE_2_COMPATIBLE_SECTION


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
        self.google_client: GoogleCM360Client

        prev_state = self.get_state_file()
        self.existing_reports_cache = prev_state.get('reports') if prev_state else {}

    def create_date_range(self) -> dict:
        # TODO: complete all possible options - start / end dates
        date_range = {
            'relativeDateRange': self.cfg.time_range.period
        }
        return date_range

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
        self.cfg: Configuration = Configuration.fromDict(self.configuration.parameters)
        logging.debug(self.cfg)

        """
            Prepare a list reports
                Create a report definition based on configuration.
                (either from report specification or from existing report template)
                for each profile:
                    Check whether re-usable report exists in current state
                    If a report was found apply a patch if necessary (date changed...)
                    If no report was found create a new one based on report definition
        """
        self._init_google_client()

        if self.cfg.input_variant != InputVariant.REPORT_IDS:
            reports_2_run = self._process_generated_reports()
        else:
            # existing reports
            # TODO: handle 404 when existing report is deleted
            reports_2_run = [dict(profile_id=item.split(':')[0], report_id=item.split(':')[1])
                             for item in self.cfg.existing_report_ids]

        # Run all reports
        report_files = []
        for item in reports_2_run:
            profile_id = item['profile_id']
            report_id = item['report_id']
            report_file = self.google_client.run_report(profile_id=profile_id, report_id=report_id)
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
                file = self.google_client.report_status(report_id=report_file['reportId'], file_id=report_file['id'])
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
                    self.google_client.get_report_file(report_id=file['reportId'], file_id=file['id'], report_file=file)
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

        self.write_state_file(state_dict=dict(reports=self.existing_reports_cache))
        # TODO: Process result files into a table - generate manifest

    def _process_generated_reports(self) -> List[Dict[str, str]]:
        """
        Process generated reports either from template or custom mode.
        Cleans unused profiles from remote.
        Updates state cache.

        Returns: List of profile and report ids to process

        """

        new_report_definition = self._get_report_definition()

        current_reports = {}
        for profile_id in self.cfg.profiles:
            existing_report_def = self._get_existing_report_for_profile(profile_id)

            if existing_report_def:
                current_report_id = self._update_existing_report(profile_id, existing_report_def,
                                                                 new_report_definition)
            else:
                logging.info(f"Creating a new report in profile {profile_id}")
                current_report_id = self._create_new_report(profile_id, new_report_definition)

            # Register a report ID in current state
            current_reports[profile_id] = current_report_id
        """
            We now have current set of reports in current_reports dictionary
            Let's remove any report that will not be re-used in case a profile has been removed from the config.
        """
        for profile_id, report_id in self.existing_reports_cache.items():
            if profile_id not in current_reports or report_id != current_reports[profile_id]:
                self.google_client.delete_report(profile_id=profile_id, report_id=report_id, ignore_error=True)

        return [dict(profile_id=key, report_id=value) for key, value in current_reports.items()]

    def _update_existing_report(self, profile_id: str, existing_report: CsvReportSpecification,
                                new_report: CsvReportSpecification) -> str:
        """
        Updates existing report definition based on the new definition (user or template)
        Args:
            profile_id:
            existing_report:
            new_report:

        Returns: ID of existing report

        """
        logging.info(f"Updating an existing report in profile {profile_id}")
        # Report is available - check whether it needs a patch
        logging.debug(f'Report will be re-used {existing_report.report_id} for {profile_id}')

        # updating relevant parts just in case the definition / template had changed
        new_report.update_template_commons(existing_report.report_id,
                                           profile_id,
                                           existing_report.account_id)

        logging.debug(f'Report will be updated {new_report.report_representation}')

        self.google_client.update_report(report=new_report.report_representation,
                                         profile_id=profile_id,
                                         report_id=new_report.report_id)
        return existing_report.report_id

    def _create_new_report(self, profile_id: str, new_report: CsvReportSpecification) -> str:
        """
        Creates new report based on the specification. Updates state.
        Args:
            profile_id:
            new_report:

        Returns: Id of the newly created report.

        """
        # We could not use any existing report, we must create one
        report = self.google_client.create_report(new_report.report_representation,
                                                  profile_id=profile_id)
        # Register a report ID in current state
        self.existing_reports_cache[profile_id] = report['id']
        logging.debug(f'New report {report["id"]} created for {profile_id}')
        return report['id']

    def _get_existing_report_for_profile(self, profile_id: str) -> CsvReportSpecification:
        """
        Returns the existing report assinged to this profile and configuration if exists. None otherwise
        Args:
            profile_id: profile_id to look in state

        Returns: CsvReportSpecification

        """
        existing_report_id = self.existing_reports_cache.get(profile_id)
        report_response = None
        if existing_report_id:
            report_response = self.google_client.get_report(profile_id=profile_id, report_id=existing_report_id,
                                                            ignore_error=True)
            if not report_response:
                # Report is no longer available - remove it from the state and cancel the candidate ID
                logging.warning(f"The report ID {existing_report_id} in state was deleted manually from the source!")
                self.existing_reports_cache.pop(profile_id)

        return CsvReportSpecification(report_response)

    def _get_report_definition(self) -> CsvReportSpecification:
        if self.cfg.input_variant == 'report_specification':
            logging.debug('Input variant: report_specification')
            specification = self.cfg.report_specification

            dimensions = [{'name': name} for name in
                          specification.dimensions] if specification.dimensions else [],
            metrics = specification.metrics if specification.metrics else []

            report_def = CsvReportSpecification.custom_from_specification(report_name=self.generate_report_name(),
                                                                          report_type=specification.report_type,
                                                                          date_range=self.create_date_range(),
                                                                          dimensions=dimensions,
                                                                          metrics=metrics)
        elif self.cfg.input_variant == "report_template_id":
            logging.debug('Input variant: report_template_id')
            src_profile_id, src_report_id = self.cfg.report_template_id.split(':')
            report_template = self.google_client.get_report(profile_id=src_profile_id, report_id=src_report_id)
            report_def = CsvReportSpecification(report_template)
        else:
            raise UserException(f'Unsupported mode: {self.cfg.input_variant}')

        return report_def

    def _init_google_client(self):
        client = GoogleCM360Client(
            self.configuration.oauth_credentials.appKey,
            self.configuration.oauth_credentials.appSecret,
            self.configuration.oauth_credentials.data
        )
        self.google_client = client

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
        self._init_google_client()
        profiles = self.google_client.list_profiles()
        prof_w_labels = [SelectElement(value=profile[0], label=f'{profile[1]} ({profile[0]})') for profile in profiles]
        return prof_w_labels

    def _load_attribute_values(self, attribute: str):

        report_type = self.configuration.parameters.get('report_specification').get('report_type')
        if not report_type or report_type not in MAP_REPORT_TYPE_2_COMPATIBLE_SECTION:
            raise ValueError('No or invalid report_type')

        dims = _load_attribute_labels_from_json(report_type, attribute)
        self._init_google_client()
        ids = self.google_client.list_compatible_fields(report_type=report_type,
                                                        compat_fields=MAP_REPORT_TYPE_2_COMPATIBLE_SECTION[report_type],
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
        self._init_google_client()
        list_profiles = self.google_client.list_profiles()
        profiles_2_names = {it[0]: it[1] for it in list_profiles}
        reports_w_labels = []
        for profile_id in profile_ids:
            reports = self.google_client.list_reports(profile_id=profile_id)
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

        self._init_google_client()
        report = self.google_client.create_report(report=rep_def, profile_id='8653652')
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
        self._init_google_client()
        report = self.google_client.patch_report(report=rep_def, report_id='1090921139', profile_id='8653652')
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
