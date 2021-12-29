import pandas as pd


def average(lst):
    return int(sum(lst) / len(lst))


FILE_PATH = "./datasets/fakefriends.csv"

data = pd.read_csv(FILE_PATH, delimiter=",")

data.columns = ["ID", "User", "Age", "Friends"]

friend_by_age = {}

for row in data.index:
    if data["Age"][row] not in friend_by_age:
        friend_by_age[data["Age"][row]] = [data["Friends"][row]]
    else:
        friend_by_age[data["Age"][row]].append((data["Friends"][row]))

friend_by_age = sorted(friend_by_age.items())

result = []

for value in friend_by_age:
    result.append((value[0], average(value[1])))

print(result)
