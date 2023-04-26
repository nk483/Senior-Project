import pandas as pd
import numpy as np
# Load the pickle file into a DataFrame
df = pd.read_pickle('combined_df.pkl')
np.random.seed(42)
sample_df = df.sample(n=50)
same_belief_draw = sample_df['Draw'] == sample_df['Draw'].shift(1) 
print(same_belief_draw.head(n=10))
sample_df.to_csv("sample_df.csv")