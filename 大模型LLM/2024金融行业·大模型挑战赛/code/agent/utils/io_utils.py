import json
import jsonlines


def load_external_answers(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        ext_data = json.load(f)
    return ext_data

def dump_2_report_answer(info, path):
    with open(path, 'w') as output_json_file:
        json.dump(info, output_json_file, ensure_ascii=False, indent=4)

def dump_jsonl(info, path):
    with jsonlines.open(path, "w") as json_file:
        for obj in info:
            json_file.write(obj)

def read_jsonl(path):
    content = []
    with jsonlines.open(path, "r") as json_file:
        for obj in json_file.iter(type=dict, skip_invalid=True):
            content.append(obj)
    return content