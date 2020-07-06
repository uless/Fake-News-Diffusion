#import jsonpickle
from networkx import Graph
import networkx as nx


def basic(graph):
    all_nodes = list(graph.nodes())
    cascade_size = len(all_nodes)
    print(cascade_size)

    
if __name__ == "__main__":
    print("Start")
    #just for testing the data
    #graph_file ="comment_trees/fqtmst.gpickle" #get_file_list(sys.argv[1])
    graph_file ="comment_trees/65b3sd.gpickle" #get_file_list(sys.argv[1])
    print(graph_file)
    graph = nx.read_gpickle(graph_file)
    basic(graph)
    
    
    
