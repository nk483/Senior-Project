import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from scipy import stats
def apply_func(group, start):
    last_obs = group['Team 1 Belief'].iloc[-1]
    result = int(round(last_obs, 0))
    if start:
        initial_obs = group['Team 1 Belief'].iloc[0]       
        return {"Initial Belief" : initial_obs, "Result" : result}
    else:
        n = len(group)
        middle_obs = group['Team 1 Belief'].iloc[int(n/2)]
        return {"Middle Belief" : middle_obs, "Result" : result}
    
df = pd.read_pickle('data_processing/processed_df.pkl')
grouped = df.groupby(['id', 'Sportsbook'])
start = False
result = grouped.apply(apply_func, start = start)
if start:
    group_col = 'Initial Belief'
else:
    group_col = 'Middle Belief'
mean_result = result.groupby(group_col)['Result'].mean()
slope, intercept, r_value, p_value, std_err = stats.linregress(mean_result.index, mean_result)
# plot the mean 'Result' for each unique 'Initial Belief' value as a scatter plot with error bars
fig, ax = plt.subplots()
ax.scatter(mean_result.index, mean_result)
ax.plot(mean_result.index, mean_result.index, '--', color='gray', label = 'Identity')
ax.plot(mean_result.index, intercept + slope*mean_result.index, '-', color = 'red', label = 'OLS')
ax.set_xlabel(group_col)
ax.set_ylabel('Mean Result')
ax.set_title(group_col +  ' vs Mean Result')
plt.legend()
plt.savefig(group_col + ' vs Mean Result')
#Shows clear heterogeneity in markup depending on favorite vs underdog with underdogs receiving a higher spread