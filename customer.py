import pandas as pd

FILE_PATH = "./datasets/customer-orders.csv"

data = pd.read_csv(FILE_PATH, delimiter=",")
data.columns = ["UserID", "ItemID", "Price"]

user_expence_count = {}

for index in data.index:
    if data["UserID"][index] not in user_expence_count:
        user_expence_count[data["UserID"][index]] = data["Price"][index]
    else:
        user_expence_count[data["UserID"][index]] += data["Price"][index]

result = [
    (key, float(f"{value:.2f}"))
    for (key, value) in user_expence_count.items()
    if value >= 6000
]

result = sorted(result, key=lambda x: x[1], reverse=True)
print(result)
