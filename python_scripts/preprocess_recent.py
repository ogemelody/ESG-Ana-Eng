import pandas as pd
import pathlib
import re
import pprint


columns_dict = {
    "Filter Level 1": "Filter_Level_1",
    "Filter Level 2": "Filter_Level_2",
    "Filter Level 3": "Filter_Level_3",
    "Portfolio": "Portfolio_weight",
    "Benchmark": "Benchmark_weight",
    "Active": "Active_weight",
    "Portfolio (bp)": "Portfolio_return",
    "Benchmark (bp)": "Benchmark_return",
    "Active (bp)": "Active_return",
    "Portfolio (bp).1": "Portfolio_returncontribution",
    "Benchmark (bp).1": "Benchmark_returncontribution",
    "Active (bp).1": "Active_returncontribution",
    "Sector Allocation (bp)": "Sector_Allocation",
    "Security Selection (bp)": "Security_Selection",
    "Portfolio_from_file": "Portfolio"
}


reverse_column_dict = {value: key for key, value in columns_dict.items()}

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
        self._filepath = pathlib.Path(filepath)
        self._metadata_dict = {}
        self._dataframes = {}
        self._result = None
        self._initialize()


    def _initialize(self):
        path = self._filepath
        # self._sheet_names = []
        files = path.glob("**/*.xlsx")
        for file in files:
            print(file.name)
            
            excel = pd.ExcelFile(file) # read excel file
            sheets = excel.sheet_names
            # print(sheets)

            self._metadata_dict[file.name] = {}
            self._dataframes[file.name] = {}

            for sheet in sheets:
                df = excel.parse(sheet, skiprows=2, skipfooter=3)
                header = df.iloc[:1, :10]
                columns = df.iloc[3, :].values
                df = df.iloc[4:, :]
                df.columns = columns
                self._metadata_dict[file.name][sheet] = header.iloc[0, :].to_dict()
                self._dataframes[file.name][sheet] = df

    def _combine_dataframes(self):
        dfs = []
        for file in self._dataframes.keys():
            fms = self._dataframes[file].values()
            dfs.extend(fms)
        
        self._result = pd.concat(dfs)

    def to_excel(self, name, index=False):
        if self._result is None:
            self._combine_dataframes()
        self._result.to_excel(f'{name}.xlsx', index=index)

    def _rename_df_columns(self, df: pd.DataFrame, columns_dict: dict):
        df.columns.rename(columns_dict, inplace=True)

    def rename_columns(self, columns_dict):
        try:
            if self._result:
                self._rename_df_columns(self._result, columns_dict)
            else:
                for file in self._dataframes.keys():
                    fms = self._dataframes[file].values()
                    for df in fms:
                        self._rename_df_columns(df, columns_dict)
        except:
            print("An error occured")

    
    def add_meta_cols(self, metadata_dict):
        dfs = []
        for file in self._dataframes.keys():
            for sheet, frame in self._dataframes[file].items():
                header = self._metadata_dict[file][sheet]
                d = frame.assign(
                        Reporting_Date=header["Date"], 
                        Portfolio_Code=header["Portfolio Short Name"],
                    )
                dfs.append(d)
        self._result = pd.concat(dfs)
        self._result.reset_index(inplace=True)
        self._result.drop('index', axis=1, inplace=True)
        pprint.pprint(self._result)

   

    def preprocess(self) -> pd.DataFrame:
        dfs = []
        files = self._filepath.glob("**/*.xlsx")
        # print(list(files))
        # Loop through folder
        for file in files:
            
            excel = pd.ExcelFile(file) # read excel file
            sheets = excel.sheet_names

            print(sheets)

            self._metadata_dict[file.name] = {}

            for sheet in sheets:
                df = excel.parse(sheet, skiprows=2, skipfooter=3)
                header = df.iloc[:1, :10]
                date = header.loc[0, "Date"]
                self._metadata_dict[file.name][sheet] = header.iloc[0, :].to_dict()
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


preprocessing  = Preprocess("./30_10_2020/Analytics")

#data = preprocessing.add_meta_cols({})
#preprocessing.to_excel('IMP1')
# t = preprocessing.preprocess()

# print(data)