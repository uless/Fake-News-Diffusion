import jsonpickle
import networkx as nx
import sys

def get_all_comments(filename):
    with open(filename, 'r') as infile:
        data = infile.read().strip().split("\n")
    return data

#parent_id == link_id in case of top level comment
#to find parent comment, remove t1_ from parent id of others

def get_data_for_post(filename):
    with open(filename, 'r') as infile:
        data = jsonpickle.decode(infile.read())
        real_data = []
        for row in data:
            comment = row["py/seq"][-1]
            real_data.append(comment)
    return real_data

def create_tree(comments, post_id):
    graph = nx.DiGraph()
    graph.add_node(post_id, type="post")
    for comment in comments:
        node_id = comment["id"]
        parent_id = comment["parent_id"].split("_")[1]
        graph.add_node(node_id, data=comment)
        graph.add_edge(node_id, parent_id)
    nx.write_gpickle(graph, "comment_trees/"+post_id+".gpickle")

if __name__ == "__main__":
    comment_files = get_all_comments(sys.argv[1])
    for each_file in comment_files:
        print(each_file, each_file.split(".")[0].split("_")[1])
        comment_data = get_data_for_post("comments/"+each_file)
        create_tree(comment_data, each_file.split(".")[0].split("_")[1])