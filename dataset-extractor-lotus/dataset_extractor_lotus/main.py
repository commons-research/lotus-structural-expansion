# Description:
# extract a small LOTUS dataset to sample N lines from M members of taxa level T.

import polars as pl


#########################
# Variables to decleare #
#########################

# The data can be found here: https://zenodo.org/doi/10.5281/zenodo.5794106
path_to_file = "/home/popeye/OneDrive/2024_Master_Bioinformatics/lotus-structural-expansion/dataset-extractor-lotus/data/230106_frozen_metadata.csv"

# Declare the taxa level [string] - possible options see below:
# ['organism_taxonomy_01domain', 
# 'organism_taxonomy_02kingdom', 
# 'organism_taxonomy_03phylum', 
# 'organism_taxonomy_04class', 
# 'organism_taxonomy_05order', 
# 'organism_taxonomy_06family', 
# 'organism_taxonomy_07tribe', 
# 'organism_taxonomy_08gdfenus', 
# 'organism_taxonomy_09species', 
# 'organism_taxonomy_10varietas']
taxalevel = "organism_taxonomy_07tribe"


# Declare the amount of members from the taxa level to sample randomly [int]
members_of_taxalevel = 3

# Declare how many times a sample should be taken from one member [int]
samplesize_per_member = 4


###############
# Constructor #
###############
seed_pl = 42


#############
# Functions #
#############




########
# Main #
########

# read the data in
df = pl.read_csv(path_to_file, separator=",", infer_schema_length=1000000)  


# get all possible options for members
members_list = df[taxalevel].unique()

# drop all the NAs
# Check if necessary

# sample M members from the taxa
very_important_members = members_list.sample(n=members_of_taxalevel)


# empty final dataset
data_toy = df.clear()

# empty list for amount stored
data_size_after = []
data_size_before = []


# sample from each "very important member" N lines
for important_member in very_important_members:

    # filter the "important member" 
    data_vim = df.filter(pl.col(taxalevel) == important_member).sample(n=samplesize_per_member)

    # store this dataframe in the final
    data_toy.extend(data_vim)


# print(members_list, len(members_list))
# print(very_important_members)
print(data_toy)