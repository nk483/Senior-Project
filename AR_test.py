import pandas as pd
import numpy as np
import sys
from scipy.stats import norm
from tabulate import tabulate
#Implements the main AR statistical test on our data and prints the results as a table
def perform_test(df, to_print=True):
    df['Average EM'] = df['Movement'] - df['Uncertainty Reduction']
    df = df.dropna(subset=['Average EM'])
    sportsbooks_df = df.groupby('Sportsbook')
    sb_df = sportsbooks_df[['Average EM', 'Uncertainty Reduction', 'Movement']]
    result_df = sb_df.mean().reset_index()
    result_df['Total M'] = sb_df.sum()['Movement'].values
    result_df['n'] = sb_df.size().values
    result_df['Sample SD'] = sb_df.std()['Average EM'].values
    result_df['Z score'] = result_df['Average EM'] / (result_df['Sample SD'] / np.sqrt(result_df['n']))
    result_df['EM_norm'] = (result_df['Average EM'] / result_df['Uncertainty Reduction']) + 1
    result_df['p value'] = ((1 - norm.cdf(np.abs(result_df['Z score']))) * 2)
    display_df = result_df[['Sportsbook', 'n', 'Total M', 'Average EM', 'EM_norm', 'Z score', 'p value']]
    display_df.set_index('Sportsbook')
    table = tabulate(display_df, headers='keys', tablefmt='psql', floatfmt = ".5f")
    if to_print:
        print(table)
    else:
        return display_df

if __name__ == '__main__':
    df = pd.read_parquet('data_processing/processed_df.parquet')
    perform_test(df)
