import pandas as pd

csv_url = "https://github.com/davidmegginson/ourairports-data/blob/main/airports.csv"

df = pd.read_csv(csv_url).fillna('')

print("the shape of the data frame is ", df.shape)

print("The first 10 lines of the data frame are", df.head(10))
