"""
This python script takes in the original external_clusters.csv from Sanger,
cleans it to be of needed format.
It removes the GPSC column and rename merge_history column to Cluster.

To run script type the following: `python3 clean_external_clusters_csv`
"""

import pandas as pd

file_path = "./beebop/resources/GPS_v9_external_clusters.csv"
df = pd.read_csv(file_path)

# Remove the "GPSC" column
df.drop("GPSC", axis=1, inplace=True)

# Rename the "merge_history" column to "Cluster"
df.rename(columns={"merge_history": "Cluster"}, inplace=True)

# Save the updated data to a new CSV file
df.to_csv(file_path, index=False)
