import pandas as pd
import os
def crossTrackColumns(path):

    df = pd.read_csv(path)

    #adding columns to the dataframe.
    new_row = pd.DataFrame([df.columns], columns=df.columns)

    df = pd.concat([new_row, df], ignore_index=True)

    df.columns = ['id', 'vid', 'url']

    return df

df = crossTrackColumns(path='crosstask_release/videos_val.csv')
df.to_csv('crossTrackVal_df.csv')