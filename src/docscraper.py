import requests
from bs4 import BeautifulSoup as bs
import logging
import json
import os
from configuration import FILE_JSON_LABELS

URL = "https://developers.google.com/doubleclick-advertisers/v4/dimensions"


logger = logging.getLogger(name=__name__)


def map2id(report_type: str, attribute):
    return report_type.lower().replace('_', '-')+'-'+attribute


def scrape_props_from_doc(report_type: str, attributes: list([str])):
    results = []
    try:
        logger.info(f'Reading from: {URL}')
        page = requests.get(URL)
        soup = bs(page.content, 'html.parser')
        for attribute in attributes:
            h2_id = map2id(report_type, attribute)
            logger.info(f'search for h2 of class "{h2_id}"')
            result = {}
            results.append(result)
            try:
                h2 = soup.find('h2', id=h2_id)
                if not h2:
                    continue
                table = h2.findNext('table')
                if not table:
                    continue
                logger.info('  table found...')
                rows = table.findAll('tr')
                for row in rows:
                    cols = row.findAll("td")
                    if not cols or len(cols) < 2:
                        continue
                    result[cols[0].text] = cols[1].text
                logger.info(f'    items found: {len(result)}')
            except Exception:  # noqa: F841
                pass
        return [result for result in results]
    except Exception:
        return [{} for it in attributes]  # noqa: F841


if __name__ == '__main__':
    logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)
    all_mapping = {}

    for report_type in ['STANDARD', 'REACH', 'FLOODLIGHT', 'PATH', 'PATH_ATTRIBUTION']:
        dims, metrics = scrape_props_from_doc(report_type, ['dimensions', 'metrics'])
        all_mapping[report_type] = {'dimensions': dims, 'metrics': metrics}

    logger.info(f'Writing file {FILE_JSON_LABELS}')
    with open(FILE_JSON_LABELS, mode='w') as file:
        json.dump(all_mapping, fp=file, indent=2)

    logger.info(f'File written: {os.path.realpath(FILE_JSON_LABELS)}')
