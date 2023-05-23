"""
Template Component main class.

"""
import csv
# from typing import List, Tuple
import json
import logging
import os
import time
from typing import Dict, List
import dateparser

import requests
from keboola.component.base import ComponentBase, sync_action
from keboola.component.exceptions import UserException
from keboola.component.sync_actions import SelectElement

from configuration import Configuration, InputVariant
from configuration import FILE_JSON_LABELS
from google_cm360 import GoogleCM360Client
from google_cm360.report_specification import \
    CsvReportSpecification, MAP_REPORT_TYPE_2_COMPATIBLE_SECTION, MAP_REPORT_TYPE_2_CRITERIA


def _load_attribute_labels_from_json(report_type, attribute):
    all_labels = None
    try:
        path = os.path.join(os.path.dirname(__file__), FILE_JSON_LABELS)
        with open(path, mode='r') as file:
            all_labels = json.load(fp=file)
        return all_labels.get(report_type).get(attribute)
    except Exception:
        return {}


def _translate_dimensions(report_type: str, dims: list):
    labels = _load_attribute_labels_from_json(report_type=report_type, attribute="dimensions")
    return [labels.get(dim_id) if dim_id in labels else dim_id for dim_id in dims]


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

        # prev_state = self.get_state_file()
        # self.existing_reports_cache = prev_state.get('reports') if prev_state else {}
        self.existing_reports_cache: dict = {}
        self.common_report_type: str = None
        self.common_dimensions: list = None
        self.common_metrics: list = None

    def create_date_range(self) -> dict:
        # TODO: complete all possible options - start / end dates
        if self.cfg.time_range.period == 'CUSTOM_DATES':
            date_from = dateparser.parse(self.cfg.time_range.date_from)
            date_to = dateparser.parse(self.cfg.time_range.date_to)
            if not date_from or not date_to:
                raise UserException("Error with dates, make sure both start and end date are defined properly")
            day_diff = (date_to - date_from).days
            if day_diff < 0:
                raise UserException("start_date cannot exceed end_date.")
            date_range = {
                'relativeDateRange': None,
                'startDate': f'{date_from.year:04}-{date_from.month:02}-{date_from.day:02}',
                'endDate': f'{date_to.year:04}-{date_to.month:02}-{date_to.day:02}'
            }
        else:
            date_range = {
                'relativeDateRange': self.cfg.time_range.period,
                'startDate': None,
                'endDate': None
            }
        return date_range

    def _get_report_raw_file_path(self, profile_id, report_id) -> str:
        path = f'{self.files_out_path}/{profile_id}_{report_id}.raw'
        return path

    def _get_final_directory(self) -> str:
        path = f'{self.tables_out_path}/{self.cfg.destination.table_name}.csv'
        return path

    def _get_final_file_path(self, profile_id, report_id) -> str:
        path = f'{self._get_final_directory()}/{profile_id}_{report_id}.csv'
        return path

    def _retrieve_table_from_raw(self, profile_id, profile_name, report_id) -> list:
        # TODO: Raw data contain '(not set)' if value is not available. Shall we change it?
        in_file = self._get_report_raw_file_path(profile_id=profile_id, report_id=report_id)
        out_file = self._get_final_file_path(profile_id=profile_id, report_id=report_id)
        with open(in_file, 'rt') as src, open(out_file, 'wt') as tgt:
            csv_src = csv.reader(src, delimiter=',')
            csv_tgt = csv.writer(tgt, delimiter=',', lineterminator='\n')
            for row in csv_src:
                if row == ['Report Fields']:
                    break

            header = next(csv_src)
            header.insert(0, 'profile_name')
            header.insert(0, 'profile_id')

            for row in csv_src:
                if row[0] == 'Grand Total:':
                    break
                row.insert(0, profile_name)
                row.insert(0, profile_id)
                csv_tgt.writerow(row)

        logging.debug(f'Final table file {out_file} was saved')
        return header

    def _process_report_files(self, report_files: list):
        os.makedirs(self._get_final_directory(), exist_ok=True)
        header = []
        for rf in report_files:
            cur_header = self._retrieve_table_from_raw(rf['profile_id'], rf['profile_name'], rf['report_id'])
            # header_normalizer = DefaultHeaderNormalizer()
            # cur_header = header_normalizer.normalize_header(cur_header)
            if not header:
                header = cur_header
            else:
                if header != cur_header:
                    raise UserException(f'missmatch in headers found: {header} x {cur_header}')

        return header

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

        prev_state = self.get_state_file()
        self.existing_reports_cache = prev_state.get('reports')
        if not self.existing_reports_cache:
            self.existing_reports_cache = {}

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
            report_files.append(dict(profile_id=profile_id, report_id=report_id, file_id=report_file['id']))

        self._assign_profile_names(report_files)
        time.sleep(5)

        wait_files = report_files.copy()
        while wait_files:
            logging.info(f'Waiting for {len(wait_files)} running report(s)')
            wait_files = self._wait_process_report_files(wait_files)
            if wait_files:
                time.sleep(20)

        self.write_state_file(state_dict=dict(reports=self.existing_reports_cache))

        header = self._process_report_files(report_files)
        final_header = self.common_dimensions.copy()
        final_header.insert(0, header[1])
        final_header.insert(0, header[0])
        self._write_common_manifest(dimensions=final_header, metrics=self.common_metrics)

    def _assign_profile_names(self, report_files: list):
        profiles_2_names = self.google_client.list_profiles()
        for report_file in report_files:
            report_file['profile_name'] = profiles_2_names[report_file['profile_id']] \
                if report_file['profile_id'] in profiles_2_names else report_file['profile_id']
            pass

    def _write_common_manifest(self, dimensions, metrics):
        pks = self.cfg.destination.primary_key_existing if self.cfg.input_variant == 'existing_report_id' else \
            self.cfg.destination.primary_key
        pks.insert(0, dimensions[1])
        pks.insert(0, dimensions[0])
        result_table = self.create_out_table_definition(f"{self.cfg.destination.table_name}.csv",
                                                        primary_key=pks,
                                                        incremental=self.cfg.destination.incremental_loading,
                                                        columns=dimensions + metrics)
        self.write_manifest(result_table)

    def _wait_process_report_files(self, wait_files):
        report_files = []
        for report_file in wait_files:
            profile_id, report_id, file_id = report_file['profile_id'], report_file['report_id'], \
                report_file['file_id']
            file = self.google_client.report_status(report_id=report_id, file_id=file_id)
            status = file['status']
            # Available statuses: PROCESSING|REPORT_AVAILABLE|FAILED|CANCELLED|QUEUED
            if status == 'REPORT_AVAILABLE':
                file_name = self._get_report_raw_file_path(profile_id, report_id)
                self.google_client.get_report_file(report_id=report_id, file_id=file_id, local_file_name=file_name)
                logging.debug(f'Report file {file_name} was saved')
            elif status == 'FAILED' or status == 'CANCELLED':
                logging.info(f'Report {report_id} failed or canceled')
            else:
                logging.debug(f'Report {file["reportId"]} : {status}')
                report_files.append(report_file)

        return report_files

    def _process_generated_reports(self) -> List[Dict[str, str]]:
        """
        Process generated reports either from template or custom mode.
        Cleans unused profiles from remote.
        Updates state cache.

        Returns: List of profile and report ids to process

        """

        report_definition = self._get_report_definition()
        self.common_report_type = report_definition.report_representation.get('type')
        self.common_dimensions = report_definition.get_dimensions_names()
        self.common_metrics = report_definition.get_metrics_names()

        current_reports = {}
        for profile_id in self.cfg.profiles:
            existing_report_def = self._get_existing_report_for_profile(profile_id)

            if existing_report_def:
                current_report_id = self._update_existing_report(profile_id, existing_report_def,
                                                                 report_definition)
            else:
                logging.info(f"Creating a new report in profile {profile_id}")
                current_report_id = self._create_new_report(profile_id, report_definition)

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
                                report_definition: CsvReportSpecification) -> str:
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

        updated_report_body = existing_report.prepare_update_body(report_definition)

        logging.debug(f'Report will be updated {updated_report_body}')

        self.google_client.update_report(report=updated_report_body,
                                         profile_id=profile_id,
                                         report_id=existing_report.report_id)
        return existing_report.report_id

    def _create_new_report(self, profile_id: str, report_definition: CsvReportSpecification) -> str:
        """
        Creates new report based on the specification. Updates state.
        Args:
            profile_id:
            report_definition:

        Returns: Id of the newly created report.

        """
        new_report_body = report_definition.prepare_insert_body()

        report = self.google_client.create_report(new_report_body, profile_id=profile_id)
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
        if existing_report_id:
            report_response = self.google_client.get_report(profile_id=profile_id, report_id=existing_report_id,
                                                            ignore_error=True)
            if not report_response:
                # Report is no longer available - remove it from the state and cancel the candidate ID
                logging.warning(f"The report ID {existing_report_id} in state was deleted manually from the source!")
                self.existing_reports_cache.pop(profile_id)
                return None
            return CsvReportSpecification(report_response)
        else:
            return None

    def _get_report_definition(self) -> CsvReportSpecification:
        """Method creates a report definition based either on a report specification found in parameters
        or on existing report, which acts as a template. The method is not used for existing reports variant.

        In both cases the date range specification is based on parameters settings.

        Returns: Report definition in a form of CsvReportSpecification class.

        """
        if self.cfg.input_variant == 'report_specification':
            logging.debug('Input variant: report_specification')
            specification = self.cfg.report_specification

            dimensions = [{'name': name} for name in
                          (specification.dimensions if specification.dimensions else [])]
            metrics = specification.metrics if specification.metrics else []

            report_def = CsvReportSpecification.custom_from_specification(report_name=self._generate_report_name(),
                                                                          report_type=specification.report_type,
                                                                          date_range=self.create_date_range(),
                                                                          dimensions=dimensions,
                                                                          metrics=metrics)
        elif self.cfg.input_variant == "report_template_id":
            logging.debug('Input variant: report_template_id')
            src_profile_id, src_report_id = self.cfg.report_template_id.split(':')
            report_template = self.google_client.get_report(profile_id=src_profile_id, report_id=src_report_id)
            report_def = CsvReportSpecification(report_template)
            report_def.modify_date_range(date_range=self.create_date_range())
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

    def _generate_report_name(self):
        return 'keboola_generated_' + self.environment_variables.project_id + '_' + \
               self.environment_variables.config_id + '_' + \
               self.environment_variables.config_row_id

    @sync_action('load_profiles')
    def load_profiles(self):
        self._init_google_client()
        ids_2_names = self.google_client.list_profiles()
        prof_w_labels = [SelectElement(value=pid, label=f'{pid} ({name})') for pid, name in ids_2_names.items()]
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
        profiles_2_names = self.google_client.list_profiles()
        reports_w_labels = []
        for profile_id in profile_ids:
            reports = self.google_client.list_reports(profile_id=profile_id)
            reports_w_labels.extend([SelectElement(value=f'{profile_id}:{report["id"]}',
                                                   label=f'[{profiles_2_names[profile_id]}] '
                                                         f'{report["name"]} ({profile_id}:{report["id"]})')
                                     for report in reports])
        return reports_w_labels

    @sync_action('list_report_dimensions')
    def list_report_dimensions(self):
        input_variant = self.configuration.parameters.get('input_variant')
        try:
            profile_id = None
            report_id = None
            if input_variant == 'report_template_id':
                value = self.configuration.parameters.get('report_template_id')
            else:
                value = self.configuration.parameters.get('existing_report_ids')[0]
            profile_id, report_id = value.split(':')
        except Exception:
            raise UserException(f'Report id / profile id not specified: {report_id} / {profile_id}')

        try:
            self._init_google_client()
            report = self.google_client.get_report(profile_id=profile_id, report_id=report_id)
            report_type = report.get('type')
            if not report_type or report_type not in MAP_REPORT_TYPE_2_CRITERIA:
                raise ValueError('No or invalid report_type')

            dimensions = report.get(MAP_REPORT_TYPE_2_CRITERIA[report_type]).get('dimensions')
            dimensions = [item['name'] for item in dimensions]
            map_2_labels = _load_attribute_labels_from_json(report_type=report_type, attribute="dimensions")
            dims_w_labels = [SelectElement(value=id, label=map_2_labels[id] if id in map_2_labels else id)
                             for id in dimensions]
            return dims_w_labels
        except Exception:
            raise UserException(f'Cannot load Report id / profile: {report_id} / {profile_id}')


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
