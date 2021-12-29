import pandas as pd

FILE_PATH = "./datasets/Book.txt"


def get_words():
    with open(FILE_PATH, "r") as f:
        contents = f.read()
        contents = contents.split()

        result = []

        for word in contents:
            if word.isalpha():
                result.append(word.lower())

        f.close()
        return result


data = pd.DataFrame(get_words())
data.columns = ["Word"]

word_count = {}

for row in data.index:
    if data["Word"][row] not in word_count:
        word_count[data["Word"][row]] = 1
    else:
        word_count[data["Word"][row]] += 1

result = []

for key, value in word_count.items():
    if value >= 500:
        result.append((key, value))

result = sorted(result, key=lambda x: x[1], reverse=True)

for value in result:
    print(f"{value[0]}: {value[1]}")
