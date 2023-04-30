import pandas as pd
import numpy as np
import sys
from scipy.stats import norm
from tabulate import tabulate
import matplotlib.pyplot as plt
#Implements the main AR statistical test on our data and prints the results as a table
df = pd.read_parquet('data_processing/processed_df.parquet')
df['EM'] = df['Movement'] - df['Uncertainty Reduction']
sportsbooks_df = df.groupby('Sportsbook')
sb_df = sportsbooks_df[['EM', 'Uncertainty Reduction']]
result_df = sb_df.mean().reset_index()
result_df['n'] = sb_df.size().values
result_df['Sample SD'] = sb_df.std()['EM'].values
result_df['Z score'] = result_df['EM'] / (result_df['Sample SD'] / np.sqrt(result_df['n']))
result_df['EM_norm'] = (result_df['EM'] / result_df['Uncertainty Reduction']) + 1
result_df['p value'] = ((1 - norm.cdf(result_df['Z score'])) * 2)
display_df = result_df[['Sportsbook', 'n', 'EM', 'EM_norm', 'Z score', 'p value']]
display_df.set_index('Sportsbook')
table = tabulate(display_df, headers='keys', tablefmt='pipe', floatfmt = ".5f")

fig, ax = plt.subplots()

# hide the axes and ticks
ax.axis('off')

# create the table and add it to the axes
table = ax.table(cellText=display_df.values, colLabels=display_df.columns, loc='center')

# set the font size of the table
table.auto_set_font_size(False)
table.set_fontsize(12)

# save the figure as a PNG file
fig.savefig("table.png", dpi=300, bbox_inches='tight')

