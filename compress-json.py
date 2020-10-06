import json
import os
import pickle

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
FILE_NAME = os.path.join(ROOT_DIR, "output.json")
OUTPUT = os.path.join(ROOT_DIR, "output-compressed.json")
OUTPUT_PKL = os.path.join(ROOT_DIR, "dict.pkl")


def create_json_without_formatting(input_file: str, output_file: str) -> None:
    with open(input_file, 'r', encoding="utf8") as f:
        data = json.load(f)
    with open(output_file, 'w', encoding="utf8") as f:
        json.dump(data, f, ensure_ascii=False)


if __name__ == "__main__":
    create_json_without_formatting(FILE_NAME, OUTPUT)
