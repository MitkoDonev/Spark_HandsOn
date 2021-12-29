import pandas as pd

FILE_PATH = "./datasets/1800.csv"

data = pd.read_csv(FILE_PATH, delimiter=",")

only_MinTemp = {}

for index, row in data.iterrows():
    temp = float(row[3]) * 0.1
    if row[2] == "TMAX" and row[0] not in only_MinTemp:
        only_MinTemp[row[0]] = [temp]
    elif row[2] == "TMAX":
        only_MinTemp[row[0]].append(temp)


result = {key: f"{max(value):.2f}" for (key, value) in only_MinTemp.items()}

print(result)
