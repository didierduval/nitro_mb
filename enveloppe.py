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
    df_merged.to_csv(r'c:\vap_temp\MB4b C72\merged\merged.csv', index=False, sep=';', decimal=',')


def merge_csv_3():
    import numpy as np
    import matplotlib.pyplot as plt
    from scipy.signal import hilbert, chirp

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
    l, u = enveloppe(df_merged['ADCVoltageActGain_M1'].bfill().ffill())

    analytic_signal = hilbert(df_merged['ADCVoltageActGain_M1'].bfill().ffill())
    amplitude_envelope = np.abs(analytic_signal)
    df_merged['env'] = amplitude_envelope
    df_merged['env_l'] = l
    df_merged['env_u'] = u

    df_merged.to_csv(r'C:\Users\diduval\OneDrive - Danaher\NITRO\FRACAS\raw\export\export_env.csv', index=False, sep=';', decimal=',', date_format='%Y-%m-%d %H:%M:%S')
    # df_merged.to_excel(r'C:\Users\diduval\OneDrive - Danaher\NITRO\FRACAS\raw\export\export.xlsx', index=False)


def enveloppe(s):
    import numpy as np
    import scipy.interpolate
    import matplotlib.pyplot as pt

    # t = np.multiply(list(range(1000)), .1)
    # s = 10 * np.sin(t) * t ** .5
    t = range(len(s))
    u_x = [0]
    u_y = [s[0]]

    l_x = [0]
    l_y = [s[0]]

    # Detect peaks and troughs and mark their location in u_x,u_y,l_x,l_y respectively.
    for k in range(2, len(s) - 1):
        if s[k] >= max(s[:k - 1]):
            # u_x.append(t[k])
            u_y.append(s[k])

    for k in range(2, len(s) - 1):
        if s[k] <= min(s[:k - 1]):
            # l_x.append(t[k])
            l_y.append(s[k])

    u_p = scipy.interpolate.interp1d(u_x, u_y, kind='cubic', bounds_error=False, fill_value=0.0)
    l_p = scipy.interpolate.interp1d(l_x, l_y, kind='cubic', bounds_error=False, fill_value=0.0)

    q_u = np.zeros(s.shape)
    q_l = np.zeros(s.shape)
    for k in range(0, len(s)):
        q_u[k] = u_p(t[k])
        q_l[k] = l_p(t[k])

    return q_l, q_u


if __name__ == '__main__':
    merge_csv_3()
