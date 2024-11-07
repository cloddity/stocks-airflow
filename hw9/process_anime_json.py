import pandas as pd
import json

def process_tmdb_csv(input_file, output_file):
    with open(input_file, 'r') as file:
        data = json.load(file)

    data = data["data"]

    final_json_file = [] 

    for i, ani in enumerate(data):
        put = f"id:hybrid-search:doc::{i}"
        doc_id = i
        title = ani["title"]
        title = title.replace("\\", "").replace("\"", "")
        tag_string = ""
        for tag in ani["tags"]:
            tag_string += tag 
            tag_string += " "
        ani_dict = {
            "put": put,
            "fields": {
                "doc_id":doc_id,
                "title":title,
                "text":tag_string }
        }
        final_json_file.append(ani_dict)

    with open(output_file, 'w') as f:
        json.dump(final_json_file, f)

process_tmdb_csv('anime-offline-database-minified.json', 'anime_db.jsonl')