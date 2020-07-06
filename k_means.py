import jsonpickle, json, sys
import pandas as pd
from sklearn.cluster import KMeans
from sklearn import preprocessing
from sklearn.metrics import davies_bouldin_score, calinski_harabasz_score, silhouette_score

def get_file_list(filename):
    with open(filename, 'r') as infile:
        data = infile.read().split()
        return data

def read_file(filename):
    try:
        with open("cascade_info/" + filename, 'r') as infile:
            data = jsonpickle.decode(infile.read())
        del data["level_l_size"]
        return data
    except:
        print(filename)

def get_all_file_data(file_list):
    data = []
    for filename in file_list:
        value = read_file(filename)
        if value:
            data.append(value)
    df = pd.DataFrame(data)
    cols = df.columns
    df = df[df.cascade_size > 1]
    normalized_data = preprocessing.normalize(df)
    normalized_df = pd.DataFrame(normalized_data, columns = cols)
    # print(df.head())
    # print(normalized_df.head())
    # df.to_csv("df.csv")
    # normalized_df.to_csv("norm_df.csv")
    return normalized_df

def do_k_means(k, data):
    kmeans = KMeans(n_clusters=k).fit(data)
    labels = kmeans.labels_
    print(k, silhouette_score(data, labels, metric='euclidean'))

if __name__ == "__main__":
    file_list = get_file_list(sys.argv[1])
    all_data = get_all_file_data(file_list)
    for k in range(2, 1001):
        do_k_means(k, all_data)