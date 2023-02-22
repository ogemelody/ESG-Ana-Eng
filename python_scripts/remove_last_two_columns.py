import pandas as pd

df= pd.read_csv("C:/Users/melod/OneDrive/Documents/Data2bots/new_ESG_project/new_file.csv")
print(df)

#df1= df.drop(df.columns[-1], axis=1, inplace=True)
df1 = df[df.columns[:-2]]
print(df1)

df1 = df1.to_csv("C:/Users/melod/OneDrive/Documents/Data2bots/new_ESG_project/analytics.csv")
#print(output_path)