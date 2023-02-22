import pandas as pd
import pathlib
import re

#compile all the multiple excel sheets into one
#remove headers and footers


def f(x):
    try:
        if x < -1 or x > 1000000:
            return 0
        return x
    except:
        return x



class Preprocess:
    #     "remove headers and footers"
    def __init__(self, filepath):
        self._filepath = filepath

    
    def preprocess(self) -> pd.DataFrame:
        dfs = []
        path = pathlib.Path(self._filepath)
        files = path.glob("**/*.xlsx")
        # print(list(files))
        # Loop through folder
        for file in files:
            excel = pd.ExcelFile(file) # read excel file
            sheets = excel.sheet_names

            print(sheets)

            for sheet in sheets:
                df = excel.parse(sheet, skiprows=2, skipfooter=3)
                header = df.iloc[:1, :10]
                date = header.loc[0, "Date"]
                portfolio_short_name = header.loc[0, "Portfolio Short Name"]

                columns = df.iloc[3, :].values
                df = df.iloc[4:, :]
                df.columns = columns
                df.rename(columns=columns_dict, inplace=True)

                d = re.compile('\(([12])\)')
                reg = re.findall(d, sheet)

                TIME_LENS_DICT = {
                    '0': 'MTD',
                    '1': 'QTD',
                    '2': 'YTD'
                }

                lens_value = reg[0] if reg else '0'

                df = df.assign(
                    Reporting_Date=date, 
                    Portfolio_Code=portfolio_short_name,
                    time_lens=TIME_LENS_DICT[lens_value]
                )
                dfs.append(df)
            df = pd.concat(dfs)
            df = df[(df["Level"] != 1) & (df["Level"] != 2)]
            df.reset_index(inplace=True)
            df.drop('index', axis=1, inplace=True)
            df.to_excel('processed.xlsx', index=False)

            df.to_csv("processed.csv", index=False)


            return df


# len([1, 2, 3, 4])
preprocessing  = Preprocess("./30_10_2020/Active Attribution")

data = preprocessing.preprocess()
print(data)