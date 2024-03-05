# Description:
# extract a small LOTUS dataset to sample N lines from M members of taxa level T.

import polars as pl  # for data manipulation
import numpy as np  # specialy for NaN
import sys  # for command line arguments
import getopt  # for checking command line arguments
import datetime  # for naming the output file

#########################
# Variables to decleare #
#########################

# The data can be found here: https://zenodo.org/doi/10.5281/zenodo.5794106
path_to_file = "/home/popeye/OneDrive/2024_Master_Bioinformatics/lotus-structural-expansion/dataset-extractor-lotus/data/230106_frozen_metadata.csv"


###############
# Constructor #
###############


#############
# Functions #
#############


def read_arg(argv):
    # Default argument values (output will be done in the end of the function)
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
    # 'organism_taxonomy_10varietas',
    # 'structure_taxonomy_npclassifier_01pathway',
    # 'structure_taxonomy_npclassifier_02superclass',
    # 'structure_taxonomy_npclassifier_03class',
    # 'structure_taxonomy_classyfire_chemontid',
    # 'structure_taxonomy_classyfire_01kingdom',
    # 'structure_taxonomy_classyfire_02superclass',
    # 'structure_taxonomy_classyfire_03class',
    # 'structure_taxonomy_classyfire_04directparent']
    arg_taxalevel = "organism_taxonomy_07tribe"

    arg_members_of_taxalevel = 3
    arg_samplesize_per_member = 5
    arg_output_path = ""
    arg_help = "{0} -o <output_path> -t <taxalevel[default=07tribe]> -m <members_of_taxalevel[default=3]> -s <samplesize_per_member[default=5]>".format(
        argv[0]
    )

    # If argument help is given or no values for arguments are given, print the help message
    try:
        opts, args = getopt.getopt(
            argv[1:],
            "ho:t:m:s:",
            [
                "help",
                "output_path=",
                "taxalevel=",
                "members_of_taxalevel=",
                "samplesize_per_member",
            ],
        )
    except:
        print(arg_help)
        sys.exit(2)

    # If argument values given, overwrite the default values
    for opt, arg in opts:
        if opt in ("-h", "--help"):
            print(arg_help)  # print the help message
            sys.exit(2)
        elif opt in ("-o", "--output_path"):
            arg_output_path = arg
        elif opt in ("-t", "--taxalevel"):
            arg_taxalevel = arg
        elif opt in ("-m", "--members_of_taxalevel"):
            arg_members_of_taxalevel = arg
        elif opt in ("-s", "--samplesize_per_member"):
            arg_samplesize_per_member = arg

    # Output file naming
    datestemp = datetime.datetime.now().strftime("%Y%m%d")
    filename = (
        datestemp
        + "_taxlevel_"
        + arg_taxalevel.split("_")[2]
        + "_samplesize_"
        + str(int(arg_members_of_taxalevel) * int(arg_samplesize_per_member))
        + ".csv"
    )
    arg_output_file = arg_output_path + filename

    # # Check the output
    # print('output_path:', arg_output_path, type(arg_output_path))
    # print('taxalevel:', arg_taxalevel, type(arg_taxalevel))
    # print('members_of_taxalevel:', arg_members_of_taxalevel, type(arg_members_of_taxalevel))
    # print('samplesize_per_member:', arg_samplesize_per_member, type(arg_samplesize_per_member))

    return (
        arg_output_file,
        arg_taxalevel,
        int(arg_members_of_taxalevel),
        int(arg_samplesize_per_member),
    )


########
# Main #
########

if __name__ == "__main__":
    output_path, taxalevel, members_of_taxalevel, samplesize_per_member = read_arg(
        sys.argv
    )
    outputfilename = output_path
    taxalevel = taxalevel

    # Declare the amount of members from the taxa level to sample randomly [int]
    members_of_taxalevel = members_of_taxalevel

    # Declare how many times a sample should be taken from one member [int]
    samplesize_per_member = samplesize_per_member

    # import the data
    df = pl.read_csv(
        path_to_file,
        dtypes={
            "structure_xlogp": pl.Float32,
            "structure_cid": pl.UInt32,
            "organism_taxonomy_ncbiid": pl.UInt32,
            "organism_taxonomy_ottid": pl.UInt32,
            "structure_stereocenters_total": pl.UInt32,
            "structure_stereocenters_unspecified": pl.UInt32,
        },
        separator=",",
        infer_schema_length=50000,
        null_values=["", "NA"],
    )

    df = df.with_columns(
        pl.col("organism_taxonomy_gbifid")
        .map_elements(lambda x: np.nan if x.startswith("c(") else x)
        .cast(pl.UInt32)
        .alias("organism_taxonomy_gbifid")
    )

    # get all possible options for members and check the amount of members
    members_list = df[taxalevel].unique()

    if len(members_list) < members_of_taxalevel:
        print("The amount of members is less than the amount of members to sample.")
        print("Please choose a smaller amount of members to sample.")
        sys.exit(2)

    # sample M members from the taxa
    very_important_members = members_list.sample(n=members_of_taxalevel)

    # empty final dataset
    data_toy = df.clear()

    # sample from each "very important member" N lines
    for important_member in very_important_members:

        # filter the "important member" and check, if the amount of possible samples is less than the amount of samplesize
        data_vim = df.filter(pl.col(taxalevel) == important_member)
        if len(data_vim) < samplesize_per_member:
            print(
                "The amount of possible samples in ",
                important_member,
                "(",
                len(data_vim),
                ") is less than the amount of `samplesize per member` (",
                samplesize_per_member,
                ").",
            )
            print("Please choose a smaller amount of samplesize.")
            sys.exit(2)
        else:
            print(
                "Taxa:",
                important_member,
                "-",
                round(samplesize_per_member / len(data_vim), 2),
                "-",
                samplesize_per_member,
                "/",
                len(data_vim),
            )

        # sample N lines from the "important member"
        data_vim = data_vim.sample(n=samplesize_per_member)

        # store this dataframe in the final dataframe
        data_toy.extend(data_vim)

    # save the final dataframe in a *.csv file
    data_toy.write_csv(outputfilename)
