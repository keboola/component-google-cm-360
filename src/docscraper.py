import requests
from bs4 import BeautifulSoup as bs

URL = "https://developers.google.com/doubleclick-advertisers/v4/dimensions"


def map2id(report_type: str, attribute):
    return report_type.lower().replace('_', '-')+'-'+attribute


def scrape_props_from_doc(report_type: str, attributes: list([str])):
    results = []
    try:
        page = requests.get(URL)
        soup = bs(page.content, 'html.parser')
        for attribute in attributes:
            h2_id = map2id(report_type, attribute)
            result = {}
            results.append(result)
            try:
                h2 = soup.find('h2', id=h2_id)
                if not h2:
                    continue
                table = h2.findNext('table')
                if not table:
                    continue
                rows = table.findAll('tr')
                for row in rows:
                    cols = row.findAll("td")
                    if not cols or len(cols) < 2:
                        continue
                    result[cols[0].text] = cols[1].text
            except Exception:  # noqa: F841
                pass
        return (result for result in results)
    except Exception:
        return ({} for it in attributes)  # noqa: F841

# if __name__ == '__main__':
#     _dims, _metrics = scrape_props_from_doc('STANDARD', ['dimensions', 'metrics'])
#     _dims, _metrics = scrape_props_from_doc('REACH', ['dimensions', 'metrics'])
#     _dims, _metrics = scrape_props_from_doc('FLOODLIGHT', ['dimensions', 'metrics'])
#     _dims, _metrics = scrape_props_from_doc('PATH', ['dimensions', 'metrics'])
#     _dims, _metrics = scrape_props_from_doc('PATH_ATTRIBUTION', ['dimensions', 'metrics'])
#     pass
