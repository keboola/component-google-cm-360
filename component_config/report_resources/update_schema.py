import csv
import json

from tkinter import Tk


def copy_to_clipbard(par: dict):
    res = json.dumps(par)
    r = Tk()
    r.withdraw()
    r.clipboard_clear()
    r.clipboard_append(res)
    r.update()  # now it stays on the clipboard after the window is closed
    r.destroy()


with open('../configRowSchema.json', 'r') as f:
    schema = json.load(f)

report_properties = schema['properties']['report_specification']['properties']

dimensions = [
    'dimensions_standard',
    'dimensions_reach',
    'dimensions_path_to_conversion',
    'dimensions_cross_dimension_reach',
    'dimensions_floodlight',
    'dimensions_path',
    'dimensions_path_attribution'
]

metrics = [
    'metrics_standard',
    'metrics_reach',
    'metrics_path_to_conversion',
    'metrics_cross_dimension_reach',
    'metrics_floodlight',
    'metrics_path',
    'metrics_path_attribution'
]

filters = [
    'filters_standard',
    'filters_reach',
    'filters_path_to_conversion',
    'filters_cross_dimension_reach',
    'filters_floodlight',
    'filters_path',
    'filters_path_attribution'
]


def read_ids_labels(file_name):
    with open(file_name, 'r') as file:
        reader = csv.DictReader(file, fieldnames=['id', 'label', 'type'], dialect=csv.excel_tab)
        ids = []
        labels = []
        counter = 0
        for row in reader:
            ids.append(row['id'])
            labels.append(row['label'])
            counter += 1
            if counter >= 5:
                break
    return ids, labels

def update_item(item: str):
    file_name = f'{item}.csv'
    ids, labels = read_ids_labels(file_name)
    if item.startswith('filter'):
        report_properties[item]['items']['properties']['name']['enum'] = ids
        report_properties[item]['items']['properties']['name']['options']['enum_titles'] = labels
    else:
        report_properties[item]['items']['enum'] = ids
        report_properties[item]['items']['options']['enum_titles'] = labels

for item in dimensions + metrics + filters:
    update_item(item)

copy_to_clipbard(schema)
print('New version in clipbard')
