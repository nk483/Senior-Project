import pandas as pd
import numpy as np
import sys
#Processes the dataframe containing all the data
df = pd.read_parquet('data_processing/combined_df.parquet')
#Ensure df is sorted first by matchid, then within a match by sportsbook, and then within a stream by timestamp
df = df.sort_values(['id', 'Sportsbook', 'Time'])
print(df.shape[0])
#Remove Betfair and MyBookie
df = df[~df['Sportsbook'].isin(['MyBookie.ag', 'Betfair'])]

print(df.shape[0])
#Remove PointsBet observations from when bets are frozen
condition = (df['Sportsbook'] == 'PointsBet (US)') & (df['Team 1 Belief'] == 1) & (df['Team 2 Belief'] == 1)
df = df[~condition]

print(df.shape[0])
#Keep only major sports
sports = ['MLS', 'NBA', 'NCAAB', 'NFL', 'NCAAF', 'MLB']
df = df[df['Sport'].isin(sports)]
print(df.shape[0])
#Only keep rows that reflect changes in beliefs
same_belief_one = df['Team 1 Belief'] == df['Team 1 Belief'].shift(1) 
same_belief_two = df['Team 2 Belief'] == df['Team 2 Belief'].shift(1) 
same_matchid = df['id'] == df['id'].shift(1)
# use boolean indexing to filter out the consecutive rows that meet the condition
df = df[~(same_belief_one & same_belief_two & same_matchid)]
print(df.shape[0])
#Only use matches with 20+ unique quotes
grouped = df.groupby(['id', 'Sportsbook'])
#Keep only streams that are resolved (meaning one final belief > .9)
df = grouped.filter(lambda x: max(x['Team 1 Belief'].iloc[-1], x['Team 2 Belief'].iloc[-1]) > .9)
print(df.shape[0])
grouped = df.groupby(['id', 'Sportsbook'])
# use filter() method to keep only streams with 20 or more rows
df = grouped.filter(lambda x: len(x) >= 20)
print(df.shape[0])
def calc_movement(group):
    return (group - group.shift(1)) ** 2
def calc_ur(group):
    lagged_group = group.shift(1)
    return (lagged_group*(1-lagged_group) - group*(1-group))
grouped = df.groupby(['id', 'Sportsbook'])['Team 1 Belief']
df['Movement'] = grouped.transform(calc_movement)
df['Uncertainty Reduction'] = grouped.transform(calc_ur)
df = df.reset_index(drop=True)
df = df.set_index(df.groupby(['id', 'Sportsbook']).cumcount())
df['Start_Time'] = pd.to_datetime(df['Start_Time'])
df['Time'] = pd.to_datetime(df['Time'])
df['Minute Mark'] = round((df['Time'] - df['Start_Time']) / pd.Timedelta(minutes=1)).astype(int)
game_lengths = df.groupby(['Sport', 'id', 'Sportsbook'])['Minute Mark'].max()
avg_lengths = game_lengths.groupby('Sport').mean()
chunk_sizes = avg_lengths / 24
sport_group = df.groupby('Sport')['Minute Mark']
def calc_chunk(group):
    chunk_size = chunk_sizes.loc[group.name]
    chunk = np.floor(group / chunk_size).astype(int)
    chunk = chunk.apply(lambda x: min(x, 23))
    chunk = chunk.apply(lambda x: max(x,0))
    return chunk
df['Chunk'] = sport_group.transform(calc_chunk)
df.to_parquet('data_processing/processed_df.parquet')
