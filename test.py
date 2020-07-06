import jsonpickle

def get_count(filename):
    with open(filename, 'r') as infile:
        data = jsonpickle.decode(infile.read())
        return len(data)

if __name__ == "__main__":
    with open("all_comments.txt", "r") as infile:
        data = infile.read().strip().split("\n")
    count = 0
    for filename in data:
        count += get_count("comments/" + filename)
    print(count)
