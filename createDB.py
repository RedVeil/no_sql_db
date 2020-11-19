from neo4j import GraphDatabase
import csv
import neo4j


class CreateDB:
    def __init__(self, uri, user, password, topics, learning_ressources, edges):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.topics = topics
        self.learning_ressources = learning_ressources
        self.edges = edges

    def close(self):
        self.driver.close()

    def import_topics(self):
        with self.driver.session() as session:
            for topic in topics:
                topic_name = session.write_transaction(
                    self._create_topic, topic)
            print(f"topic: {topic_name} added to db")

    def import_learning_ressources(self):
        with self.driver.session() as session:
            for learning_ressource in learning_ressources:
                learning_ressource_name = session.write_transaction(
                    self._create_learning_ressource, learning_ressource)
            print(f"learning ressource: {learning_ressource_name} added to db")

    def import_edges(self):
        with self.driver.session() as session:
            for edge in edges:
                edge_names = session.write_transaction(
                    self._create_edge, edge)
            print(f"edge: from-{edge_names[0]} to-{edge_names[1]} added to db")

    @staticmethod
    def _create_topic(tx, topic):
        result = tx.run("CREATE (a:Topic{id:$topic_id,name:$name,desc:$desc,x:$x,y:$y}) "
                        "RETURN a.name", topic_id=topic["id"], name=topic["name"], desc=topic["desc"], x=topic["x"], y=topic["y"])
        return result.single()[0]

    @staticmethod
    def _create_learning_ressource(tx, learning_ressource):
        result = tx.run("CREATE (a:Learning_Ressource{id:$ressource_id,name:$name,desc:$desc,data:$data,dataType:$dataType,url:$url,added:$added}) "
                        "RETURN a.name", ressource_id=learning_ressource["id"],
                        name=learning_ressource["name"], desc=learning_ressource["desc"],
                        data=learning_ressource["data"], dataType=learning_ressource["dataType"],
                        url=learning_ressource["url"], added=learning_ressource["added"])
        return result.single()[0]

    @staticmethod
    def _create_edge(tx, edge):
        result = tx.run(f"Match (a) WHERE a.id = '{edge['from']}'"
                        f"Match (b) WHERE b.id = '{edge['to']}'"
                        f"CREATE (a)-[:{edge['type']}]->(b)"
                        "RETURN a.id, b.id")
        return result.single()[0]


def read_csv(file_path):
    data = []
    with open(file_path) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        line_count = 0
        for row in csv_reader:
            if line_count == 0:
                keys = row
            else:
                dataInput = {}
                for i, value in enumerate(row):
                    dataInput[keys[i]] = value
                data.append(dataInput)
            line_count += 1
    print(f"{file_path} done. {line_count} lines processed")
    return data


if __name__ == "__main__":
    topics = read_csv("topics.csv")
    learning_ressources = read_csv("learning_ressources.csv")
    edges = read_csv("edges.csv")
    db = CreateDB("bolt://localhost:7687", "neo4j", "1001",
                  topics, learning_ressources, edges)
    db.import_topics()
    db.import_learning_ressources()
    db.import_edges()
    db.close()
