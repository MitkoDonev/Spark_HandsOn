import pandas as pd

FILE_PATH = "./datasets/ml-100k/u.data"

data = pd.read_csv(FILE_PATH, delimiter="\t")

data.columns = ["UserID", "MovieID", "Rating", "Timestamp"]

ratings_count = {}

for row in data.index:
    if data["Rating"][row] not in ratings_count:
        ratings_count[data["Rating"][row]] = 1
    else:
        ratings_count[data["Rating"][row]] += 1

ratings_count = sorted(ratings_count.items())

for key, value in ratings_count:
    print(f"{key} {value}")
