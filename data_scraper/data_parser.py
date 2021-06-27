import json

import locale

try:
    locale.setlocale(locale.LC_TIME, 'es_DO.UTF-8')
except locale.Error:
    locale.setlocale(locale.LC_TIME, 'es')

MAIN_TABLE_ID = 'tblMainTable_trRowMiddle_tdCell1_tblForm_trGridRow_tdCell1_grdResultList_tbl'


def raw_column_names(rows):
    for element in rows[:1]:
        return [column.text for column in element.find_all('th') if column.text]


def raw_cell_values(rows):
    line_content = []
    for element in rows:
        line_value = [column.text for column in element.find_all('td') if column][:-1]
        if not line_value or line_value[0].startswith('\n'):
            continue
        line_content.append(line_value)
    return line_content


def get_rows(content):
    table = content.find(attrs={'id': MAIN_TABLE_ID})
    return table.findAll('tr')


def to_cvs(content, output_file_name='spendings_report.csv'):
    rows = get_rows(content)
    file_content = []

    # Add column names
    file_content.append(f"{'|'.join(raw_column_names(rows)).rstrip('|')}\n")

    for line in raw_cell_values(rows):
        file_content.append(f"{'|'.join(line).rstrip('|')}\n")

    with open(output_file_name, 'w+', encoding='UTF-8') as spendings:
        spendings.writelines(file_content)


def to_json(content):
    rows = get_rows(content)
    items = []

    column_names = raw_column_names(rows)
    lines = raw_cell_values(rows)

    for line in lines:
        item = {}
        for column_index in range(len(column_names)):
            property_name = column_names[column_index].replace(' ', '')
            item.update({
                property_name: line[column_index]
                })
        items.append(item)

    return items

    # with open(output_file_name, 'w+', encoding='UTF-8') as spendings:
    #     spendings.write(json.dumps(items, ensure_ascii=False))
