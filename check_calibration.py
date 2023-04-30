import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from scipy import stats
import sys
#This file generates a plot of the initial (middle if start=False) belief vs result, averaged over all streams
def calibrate(group, start):
    last_obs = group.iloc[-1]
    result = int(round(last_obs, 0))
    if start:
        initial_obs = group.iloc[0]       
        return pd.DataFrame({"Initial Implied Odds" : [initial_obs], "Result" : [result]})
    else:
        n = len(group)
        middle_obs = group.iloc[int(n/2)]
        return pd.DataFrame({"Middle Implied Odds" : [middle_obs], "Result" : [result]})
#the value of start controls whether we're getting the initial implied odds or middle implied odds   
start_vals = [True, False]
for start in start_vals:   
    df = pd.read_parquet('data_processing/processed_df.parquet')
    grouped = df.groupby(['id', 'Sportsbook'])['Team 1 Belief']
    if start:
        group_col = 'Initial Implied Odds'
    else:
        group_col = 'Middle Implied Odds'

    calibrate_df = grouped.apply(calibrate, start = start)

    group_size = calibrate_df.groupby(group_col)['Result'].size()
    sd_result = calibrate_df.groupby(group_col)['Result'].std()
    sd_result = sd_result.fillna(0)
    sd_means = sd_result / np.sqrt(group_size)
    mean_result = calibrate_df.groupby(group_col)['Result'].mean()

    slope, intercept, r_value, p_value, std_err = stats.linregress(mean_result.index, mean_result)

    fig, ax = plt.subplots()
    ax.scatter(mean_result.index, mean_result)
    ax.errorbar(mean_result.index, mean_result, yerr = sd_means, fmt = 'none', ecolor = 'black', capsize=5, capthick=1)
    ax.plot(mean_result.index, mean_result.index, '--', color='gray', label = 'Identity')
    ax.plot(mean_result.index, intercept + slope*mean_result.index, '-', color = 'red', label = 'OLS')
    x_label = ' '.join(group_col.split(' ')[1:])
    ax.set_xlabel(x_label)
    ax.set_ylabel('Mean Result')
    ax.set_title(group_col +  ' vs Mean Result')
    plt.legend()
    plt.savefig('figures/' + group_col + ' vs Mean Result')
    #Shows clear heterogeneity in markup depending on favorite vs underdog with underdogs receiving a higher spread
