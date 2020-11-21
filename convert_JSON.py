import json
import csv

def write_edges(d,writer):
    if d["className"] == "dependencyEdge":
        edgeType = "REQUIERES"
    else:
        edgeType = "IS_PART_OF"
    writer.writerow([d["source"],d["target"],edgeType])

def write_topics(d,writer):
    writer.writerow([d["id"],d["data"]["label"],d["data"]["description"],d["position"]["x"],d["position"]["y"]])

def write_learning_ressources(d,writer):
    writer.writerow([d["id"],d["name"],d["description"],d["data"],d["mediaType"],d["added"]])

def write_paths(d,writer):
    for t in d["topics"]:
        writer.writerow([d["id"],t])

def convert_json(filename, data_type):
    with open(f"./data/json/{filename}.json") as json_file:
        data = json.load(json_file)
        with open(f"./{filename}.csv", mode="w", newline="") as csv_file:
            writer = csv.writer(csv_file, delimiter=',')
            for d in data:
                if data_type == "edges":
                    write_edges(d, writer)
                elif data_type == "learning_ressources":
                    write_learning_ressources(d, writer)
                elif data_type == "topics":
                    write_topics(d, writer)
                elif data_type == "paths":
                    write_paths(d,writer)

convert_json("mockEdges","edges")
convert_json("mockNodes","topics")
convert_json("mockLearningRessources", "learning_ressources")
convert_json("mockStudyPaths","paths")