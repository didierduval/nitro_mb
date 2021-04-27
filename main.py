import glob
import ntpath

import pandas as pd


def merge_csv():
    csvs = glob.glob(r'c:\vap_temp\MB4b C72\*.csv')
    list_df = []
    for csv in csvs:
        head, tail = ntpath.split(csv)
        clea_csv = head + '\\clean\\clean_' + tail
        with open(csv, "r") as f:
            lines = f.readlines()
        with open(clea_csv, "w") as f:
            insert = False
            for line in lines:
                if ('#DateTime' in line) or insert:
                    insert = True
                    f.write(line)
        df = pd.read_csv(clea_csv)
        list_df.append(df)
    df_merged = pd.concat(list_df)
    df_merged.to_csv(r'c:\vap_temp\MB4b C72\merged\merged.csv', index=False, sep=';', decimal=',')


def merge_csv_2():
    csvs = glob.glob(r'c:\vap_temp\MB4b C72\*.csv')
    list_df = []
    for csv in csvs:
        # head, tail = ntpath.split(csv)
        # clean_csv = head + '\\clean\\clean_' + tail
        with open(csv, "r") as f:
            lines = f.readlines()
        i = 0
        for i, line in enumerate(lines):
            if '#DateTime' in line:
                break
        # print(skipped)
        df = pd.read_csv(csv, skiprows=i)
        list_df.append(df)
    df_merged = pd.concat(list_df)
    df_merged.to_csv(r'c:\vap_temp\MB4b C72\merged\merged_all.csv', index=False, sep=';', decimal=',')


def merge_csv_3():

    csvs = glob.glob(r'C:\Users\diduval\OneDrive - Danaher\NITRO\FRACAS\raw\*.csv')
    list_df = []
    for csv in csvs:
        # head, tail = ntpath.split(csv)
        # clean_csv = head + '\\clean\\clean_' + tail
        with open(csv, "r") as f:
            lines = f.readlines()
        i = 0
        for i, line in enumerate(lines):
            if '#DateTime' in line:
                break
        # print(skipped)
        dfs = pd.read_csv(csv, skiprows=i, chunksize=1000, parse_dates=True)
        for df in dfs:
            df = df[['#DateTime', 'ErrMsg', 'FlashLampTotalCount', 'ADCVoltageActGain_M1', 'ADCVoltageActGain_M2', 'ADCVoltageActGain_M3']]
            list_df.append(df)

    df_merged = pd.concat(list_df)
    # df_exp = df_merged[['ErrMsg', 'IN_FlashLamp_MeasurementCount', 'ADCVoltageActGain_M1', 'ADCVoltageActGain_M2', 'ADCVoltageActGain_M3']]
    df_merged.to_csv(r'C:\Users\diduval\OneDrive - Danaher\NITRO\FRACAS\raw\export\export_all.csv', index=False, sep=';', decimal=',', date_format='%Y-%m-%d %H:%M:%S')
    # df_merged.to_excel(r'C:\Users\diduval\OneDrive - Danaher\NITRO\FRACAS\raw\export\export.xlsx', index=False)


if __name__ == '__main__':
    merge_csv_2()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
