from __future__ import annotations
from json_converter import convert
from CPP.shared import ICdataUtils


def words_from_json(json: str | list | dict):
    if type(json) == str:
        js = convert(json)
        if type(js) == dict:
            js = [js]
        return ICdataUtils.countWords(js)
    elif type(json) == list:
        return ICdataUtils.countWords(json)
    elif type(json) == dict:
        return ICdataUtils.countWords([json])
    else:
        print(f"JSON must be a str, list or dict you passed a {type(json)}")
