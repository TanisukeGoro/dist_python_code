import requests
import bs4
import pandas as pd
import numpy as np
import time
import asyncio #　今後非同期処理を頑張るならこれを使いたい
from module.progress import progress_bar

REQUEST_EXEPTION_REPORT_LIST = []
MULTI_FORM_EXCEPTION_LIST = []
NOT_EXIST_FORM_EXCEPTION_LIST = []
PARSE_FORM_LIST = []
title = ['company', 'form', 'hasTable']
header = title + ['input','','','','','labels','','','']
column = [''] * len(title) + ['id', 'name', 'type', 'value', '_class', 'id', 'form', '_class', 'text']

def get_html_soup(url):
    try:
        res = requests.get(url)
    except Exception as e:
        return None
    res.encoding = res.apparent_encoding # 日本語文字化け対策 https://qiita.com/nittyan/items/d3f49a7699296a58605b
    if res.status_code != 200:
        return res.status_code
    return bs4.BeautifulSoup(res.text, "html.parser")

def get_csv(name):
    return pd.read_csv(name)

def count_form(document):
    return len(document.findAll('form'))

def request_exception(status, list):
    list.insert(1,status)
    REQUEST_EXEPTION_REPORT_LIST.append(list)

def multi_form_exception(list):
    MULTI_FORM_EXCEPTION_LIST.append(list)

def not_exist_exception(list):
    NOT_EXIST_FORM_EXCEPTION_LIST.append(list)

def not_connection_exception(list):
    exception = pd.DataFrame([list], columns=['name', 'url'])
    exception.to_csv('not_connection_exception.csv', mode='a', header=False)

def indexing_input_label_pair(inputs, labels):
    input_ids = [i.get('id') for i in inputs]
    label_fors = [i.get('for') for i in labels]
    list = []
    for index , id in enumerate(input_ids):
        if (id == None):
            list.append([index, ''])
            continue
        try:
            label_index = label_fors.index(id)
            list.append([index, label_index])
        except Exception as e:
            list.append([index, ''])
    return list

def get_input_data(input):
    id = input.get('id')
    name = input.get('name')
    type = input.get('type')
    value = input.get('value')
    _class = input.get('class')
    return [id, name, type, value, _class]

def get_label_data(label):
    id = label.get('id')
    form = label.get('form')
    _class = label.get('class')
    text = label.text
    return [id, form, _class, text]

def parse_document(document, list):
    global PARSE_FORM_LIST
    form = document.find('form')
    inputs = form.findAll('input')
    labels = form.findAll('label')
    input_label_pair = indexing_input_label_pair(inputs, labels)
    hastable = 'あり' if len(form.findAll('table')) else 'なし'

    attributes = []
    for index, pair in enumerate(input_label_pair):
        input_label_attribute = []
        input = range(5) if pair[0] is int else get_input_data(inputs[pair[0]])
        label = range(4) if  pair[1] is int else get_label_data(inputs[pair[0]])
        attributes.append(list + [hastable] + input + label)
    PARSE_FORM_LIST = PARSE_FORM_LIST + attributes
    return attributes

def handleDocument(document, list):
    list = list.tolist()
    if document is None: return not_connection_exception(list)
    if type(document) == int: return request_exception(document, list)
    if count_form(document) == 0: return not_exist_exception(list)
    if count_form(document) == 1: return parse_document(document, list)
    if count_form(document) > 1: return multi_form_exception(list)

def main():
    global PARSE_FORM_LIST
    global REQUEST_EXEPTION_REPORT_LIST
    global MULTI_FORM_EXCEPTION_LIST
    global NOT_EXIST_FORM_EXCEPTION_LIST

    csv = get_csv('form_list.csv')
    lists = [i for i in np.asarray(csv) if 'http' in i[1]]
    for index, list in enumerate(lists):
        document = get_html_soup(list[1])

        handleDocument(document, list)
        if index%100 == 0:
            Results =  [column] + PARSE_FORM_LIST if index == 0 else PARSE_FORM_LIST
            PARSE_FORM = pd.DataFrame(Results, columns=header)
            REQUEST_EXEPTION_REPORT = pd.DataFrame(REQUEST_EXEPTION_REPORT_LIST, columns=['status_code', 'name', 'url'])
            MULTI_FORM_EXCEPTION = pd.DataFrame(MULTI_FORM_EXCEPTION_LIST, columns=['name', 'url'])
            NOT_EXIST_FORM_EXCEPTION = pd.DataFrame(NOT_EXIST_FORM_EXCEPTION_LIST, columns=['name', 'url'])
            if index == 0:
                PARSE_FORM.to_csv('./PARSE_FORM.csv', mode='a')
                REQUEST_EXEPTION_REPORT.to_csv('./REQUEST_EXEPTION_REPORT.csv', mode='a')
                MULTI_FORM_EXCEPTION.to_csv('./MULTI_FORM_EXCEPTION.csv', mode='a')
                NOT_EXIST_FORM_EXCEPTION.to_csv('./NOT_EXIST_FORM_EXCEPTION.csv', mode='a')
            else:
                PARSE_FORM.to_csv('./PARSE_FORM.csv', mode='a', header=False)
                REQUEST_EXEPTION_REPORT.to_csv('./REQUEST_EXEPTION_REPORT.csv', mode='a', header=False)
                MULTI_FORM_EXCEPTION.to_csv('./MULTI_FORM_EXCEPTION.csv', mode='a', header=False)
                NOT_EXIST_FORM_EXCEPTION.to_csv('./NOT_EXIST_FORM_EXCEPTION.csv', mode='a', header=False)
            progress_bar(index, len(lists), str(index) + "> " + list[0] + "  出力!!!!!!!!!!!!!!")
            PARSE_FORM_LIST = []
            REQUEST_EXEPTION_REPORT_LIST = []
            MULTI_FORM_EXCEPTION_LIST = []
            NOT_EXIST_FORM_EXCEPTION_LIST = []
            time.sleep(2)
            continue
        progress_bar(index, len(lists), str(index) + "> " + list[0])

if __name__ == '__main__':
    main()
