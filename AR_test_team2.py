import pandas as pd
import numpy as np
from AR_test import perform_test
df = pd.read_parquet('data_processing/processed_df.parquet')
#Overwrite movement and uncertainty reduction columns
def calc_movement(group):
    return (group - group.shift(1)) ** 2


def calc_ur(group):
    lagged_group = group.shift(1)
    return (lagged_group*(1-lagged_group) - group*(1-group))


grouped = df.groupby(['id', 'Sportsbook'])['Team 2 Belief']
df['Movement'] = grouped.transform(calc_movement)
df['Uncertainty Reduction'] = grouped.transform(calc_ur)
perform_test(df)

