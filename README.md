# Google Campaign Manager 360 Data Source

[Google Campaign Manager 360](https://support.google.com/campaignmanager/answer/2709362?hl=en) orchestrates digital advertising campaigns, offering precise ad placements across multiple
channels. 

This connector enables easy retrieval of Campaign Manager 360 reports **from multiple ad accounts** and allows you to run CM360 reports in several modes:

1. Export of metadata
2. Define customized reports directly in the UI.
2. Select multiple reports with the same structure across multiple accounts.
3. Use an existing report as a template and run it across multiple selected accounts.

**Table of contents:**

[TOC]


## Prerequisites

1. Get access to your [account](https://support.google.com/campaignmanager?sjid=16894252783161215189-EU#topic=2758513).

2. Log in to your account using the Authorize Account button in the Keboola interface. 

![OAuth Authorization](docs/imgs/config_oauth.png)

## Functionality & Configuration

The connector supports four modes:

1. Export of selected metadata.
2. Template-based report execution: Use an existing report definition as a template and execute it across selected accounts.
    - It’s ideal if you need to define a complex report in the [CM360 Report Builder](https://www.google.com/analytics/dfa/) and use it across multiple accounts. 
    - The selected report is left untouched, and its copy is created in all selected accounts. The resulting reports are linked to the configuration. Naming convention: `keboola_generated_{PROJECT_ID}_{CONFIG_ID}_{ROWID}`
3. Running and downloading existing report definitions: Suitable for multiple identical reports across required ad accounts, previously defined using the [CM360 Report Builder](https://support.google.com/campaignmanager/answer/2823849?sjid=16894252783161215189-EU&visit_id=638403222303021904-3691116343&rd=1).
4. Defining report definition directly in the UI: Define simple report definition directly in the configuration UI, automatically creating an offline report in the [CM360 Report Builder](https://www.google.com/analytics/dfa) that will be linked to the configuration. Naming convention: `keboola_generated_{PROJECT_ID}_{CONFIG_ID}_{ROWID}`

### Export of selected metadata

This option is helpful if you need to export metadata from the CM360 account. The metadata is exported into separate table.
You can select which metadata to export in the `Metadata` section.

### Creating and running reports from an existing report definitions (template)

This option is helpful if you need to define a complex report in the [CM360 Report Builder](https://www.google.com/analytics/dfa/) nd use it across multiple accounts. The selected report is left untouched, and its copy is created in all selected accounts. The resulting reports are linked to the configuration. 

The naming convention of the created report is: `keboola_generated_{PROJECT_ID}_{CONFIG_ID}_{ROWID}`

All results are downloaded into a single table.

To use an existing report as a template, follow these steps:

1. Set up your report in the [CM360 Report Builder](https://www.google.com/analytics/dfa/). See the official [docs](https://support.google.com/campaignmanager/answer/2823849?sjid=16894252783161215189-EU&visit_id=638403222303021904-3691116343&rd=1).
2. Select the `Report template` in the `Report definition mode` configuration option.
3. Select the existing report from the dropdown of available reports. This report will be then used as a template and recreated across selected accounts.
4. Select the desired `Time Range` (either a predefined period or `Custom Date Range`). This option allows you to define a relative report period range.
5. Set the **Destination** parameters to control how the result is stored. See the `Destination` section.

### Running existing reports

This option is suitable when you already have multiple identical reports defined across required ad accounts.

To run an existing report, follow these steps:

1. Set up your report in the [CM360 Report Builder](https://www.google.com/analytics/dfa/). See the official [documentation](https://support.google.com/campaignmanager/answer/2823849?sjid=16894252783161215189-EU&visit_id=638403222303021904-3691116343&rd=1).
2. Select `Existing report ID(s)` in `Report definition mode`. 
    - **WARNING**: Ensure selected reports have the same structure and definition; otherwise extraction will fail.
3. Select the existing report ID from the dropdown of available reports.
4. The time range is, in this case, defined by the source report. This is to keep the source reports untouched since they are not controlled by the component.
5. Set the **Destination** parameters to control how the result is stored. See the `Destination` section.

### Setting up reports directly in the UI

1. Select `Report specification` in `Report definition mode`.
2. Set up your report in  `Report Details`.
   1. `Report Type`
   2. `Dimensions`
   3. `Metrics`
   4. Optional filters
3. Select the desired `Time Range` (either a predefined period or `Custom Date Range`). This option allows you to define a relative report period range.
4. Set the **Destination** parameters to control how the result is stored. See the `Destination` section.

### Destination – report output

This section defines how the extracted data will be saved in Keboola Storage. The resulting table always contains `Profile ID` and `Profile Name` columns because the component runs through multiple accounts.

- **Load type** – If `full load` is used, the destination table will be overwritten every run. If `incremental load` is used, data will be “upserted” into the destination table.
- **Storage table name** – Name of the resulting table stored in Storage.
- **Primary key** - Since the reports are always custom-defined, define what dimensions (columns) represent the unique primary key. This is then used to perform "upserts".
    - **Note**: If the primary key is not defined properly, you may lose some data during deduplication. If there is no primary key defined and `incremental load` mode is used, each execution leads to a new set of records. Also, if this field is not empty, `Profile ID` and `Profile Name` are always used as the primary key because the component runs through multiple accounts.

## Features

| **Feature**             | **Note**                                      |
|-------------------------|-----------------------------------------------|
| Generic UI form         | Dynamic UI form                               |
| Row Based configuration | Allows structuring the configuration in rows  |
| oAuth                   | oAuth authentication enabled                  |
| Incremental loading     | Allows fetching data in new increments        |
| Dimension filter        | Fetch data of certain dimension values only   |
| Date range filter       | Specify date range                            |

## Sample Raw Configuration

### Existing report IDs

```json
{
    "parameters": {
    "debug": false,
    "profiles": [
      "8467304",
      "8653652"
    ],
    "time_range": {
      "period": "LAST_7_DAYS"
    },
    "destination": {
      "table_name": "vystup",
      "selected_variant": "existing_report_ids",
      "incremental_loading": true,
      "primary_key_existing": []
    },
    "input_variant": "existing_report_ids",
    "existing_report_ids": [
      "1079840351"
    ]
  }
}
```


### Template report

```json
{
  "parameters": {
    "debug": true,
    "profiles": [
      "8653652",
      "8467304"
    ],
    "time_range": {
      "period": "LAST_7_DAYS"
    },
    "destination": {
      "table_name": "templated",
      "selected_variant": "report_template_id",
      "incremental_loading": true,
      "primary_key_existing": [
        "activity",
        "country",
        "environment"
      ]
    },
    "input_variant": "report_template_id",
    "report_template_id": "8653652:1096894707"
  }
}
```

### Report specification

```json
{
  "parameters": {
    "debug": true,
    "profiles": [
      "8467304",
      "8653652"
    ],
    "time_range": {
      "period": "CUSTOM_DATES",
      "date_to": "today",
      "date_from": "today - 4"
    },
    "destination": {
      "table_name": "vystup",
      "primary_key": [
        "activityId",
        "campaignId"
      ],
      "selected_variant": "report_specification",
      "incremental_loading": true
    },
    "input_variant": "report_specification",
    "report_specification": {
      "metrics": [
        "clicks",
        "costPerClick"
      ],
      "dimensions": [
        "activityId",
        "campaignId"
      ],
      "report_type": "STANDARD"
    }
  }
}
```

Development
-----------

If required, change the local data folder (the `CUSTOM_FOLDER` placeholder) path to your custom path in
the `docker-compose.yml` file:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    volumes:
      - ./:/code
      - ./CUSTOM_FOLDER:/data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Clone this repository, initialize the workspace, and run the component with the following command:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
docker-compose build
docker-compose run --rm dev
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Run the test suite and lint check using this command:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
docker-compose run --rm test
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Integration
===========

For information about deployment and integration with Keboola, please refer to the
[deployment section of our developer documentation](https://developers.keboola.com/extend/component/deployment/).