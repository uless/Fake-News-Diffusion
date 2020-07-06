import jsonpickle
import networkx as nx
import sys

def get_all_comments(filename):
    with open(filename, 'r') as infile:
        data = infile.read().strip().split("\n")
    return data


def get_data_for_post(filename):
    with open(filename, 'r') as infile:
        data = jsonpickle.decode(infile.read())
        real_data = []
        for row in data:
            comment = row["py/seq"][-1]
            real_data.append(comment)
    return real_data

def add_to_graph(comments, comment_to_user_mapping, graph):
    for comment in comments:
        if comment["link_id"] == comment["parent_id"]:
            continue
        author = comment["author"]
        try:
            parent_author = comment_to_user_mapping[comment["parent_id"].split("_")[1]]
        except:
            continue
        if graph.has_edge(author, parent_author):
            graph[author][parent_author]["num_edges"] += 1
        else:
            graph.add_edge(author, parent_author, num_edges = 1)

if __name__ == "__main__":
    comment_files = get_all_comments(sys.argv[1])
    graph = nx.DiGraph()
    comment_to_user_mapping = {}
    all_comments_data = []
    for each_file in comment_files:
        print(each_file, each_file.split(".")[0].split("_")[1])
        comment_data = get_data_for_post("comments/"+each_file)
        for comment in comment_data:
            comment_to_user_mapping[comment["id"]] = comment["author"]
        all_comments_data.append(comment_data)
    for comment_data in all_comments_data:
        add_to_graph(comment_data, comment_to_user_mapping, graph)
    nx.write_gexf(graph, "user_graph.gexf")

