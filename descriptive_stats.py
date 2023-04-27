import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import sys
#This file creates a bar chart of average M and R per match by sportsbook
# and plots average M and R over the course of a game for each sport

df = pd.read_parquet('data_processing/processed_df.parquet')

mean_df = df.groupby(['Sportsbook', 'id'])[['Movement', 'Uncertainty Reduction']].sum().reset_index()
mean_df = mean_df.groupby('Sportsbook').mean().reset_index()
#plot bar chart
fig,ax = plt.subplots(figsize=(8,9))
ax = mean_df.plot(x='Sportsbook', y=['Movement', 'Uncertainty Reduction'], kind='bar', ax=ax)
ax.set_xlabel('Sportsbook')
ax.set_ylabel('Average M and R')
ax.tick_params(axis='x', which='major', labelsize=8)
ax.set_yticks(np.arange(0, .25, .025))
newticks = mean_df['Sportsbook'].values
newticks[0] = 'Barstool'
newticks[-1] = 'Hill'
newticks[-5] = 'PointsBet'
ax.set_xticklabels(newticks)
plt.savefig('figures/Average Mov and UR by Sportsbook')

# result = grouped.apply(calc_mov_ur)
# result = result.apply(lambda x: pd.Series(x))
# grouped = result.reset_index().groupby(['Sportsbook'])
# avg_mov = grouped['Movement'].mean().reset_index()
# avg_mov = avg_mov.rename(columns = {"Movement" : "Average Movement"})
# avg_ur = grouped['Uncertainty Reduction'].mean()
# combined = avg_mov.assign(Average_UR=avg_ur.values)
# # plot bar chart
# fig,ax = plt.subplots(figsize=(8,9))
# ax = combined.plot(x='Sportsbook', y=['Average Movement', 'Average_UR'], kind='bar', ax=ax)
# ax.set_xlabel('Sportsbook')
# ax.set_ylabel('Average M and R')
# ax.tick_params(axis='x', which='major', labelsize=8)
# newticks = combined['Sportsbook'].values
# newticks[0] = 'Barstool'
# newticks[-1] = 'Hill'
# newticks[-5] = 'PointsBet'
# ax.set_xticklabels(newticks)
# plt.savefig('Average Mov and UR by Sportsbook')


#Movement and UR over time plot
game_lengths = df.groupby(['Sport', 'id', 'Sportsbook'])['Minute Mark'].max()
avg_lengths = game_lengths.groupby('Sport').mean()
chunk_sizes = avg_lengths / 24
grouped = df.groupby(['Sport', 'Chunk'])[['Movement', 'Uncertainty Reduction']].mean().reset_index()
grouped['Size'] = df.groupby(['Sport', 'Chunk']).size().values
# plot the results
fig, axes = plt.subplots(nrows=len(grouped['Sport'].unique()) // 2, ncols=2, figsize=(8, 12))

# iterate over each unique sport and create a separate plot for each one
i = 0
for sport, data in grouped.groupby('Sport'):
    chunk_size = chunk_sizes.loc[sport]
    j = i % 2
    ax = axes[i // 2][j]
    ax.plot(chunk_size*data['Chunk'], data['Movement'])
    ax.plot(chunk_size*data['Chunk'], data['Uncertainty Reduction'])
    ax.set_title(sport)
    ax.set_xlabel('Minute Mark')
    ax.set_ylabel('M and R')
    i += 1
    
# adjust the layout and save the figure
fig.tight_layout()
fig.savefig('figures/Movement and UR vs. Time by Sport.png')






