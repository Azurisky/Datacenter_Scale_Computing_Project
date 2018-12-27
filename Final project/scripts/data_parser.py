import pandas as pd

## argument for CTD_chem_gene_ixns
# file_to_parse = 'CTD_chem_gene_ixns.tsv' 
# output_name = 'Chemical_Gene_IXNS.tsv'
# list_of_columns = ['CasRN', 'GeneSymbol', 'InteractionActions']
# dropby = 'CasRN'

## argument for CTD_chem_pathways_enriched
file_to_parse = 'CTD_chem_pathways_enriched.tsv' 
output_name = 'Chemical_Pathways.tsv'
list_of_columns = ['CasRN', 'PathwayID', 'CorrectedPValue']
dropby = 'CasRN'

df = pd.read_csv(file_to_parse, sep='\t')

df = df[list_of_columns].dropna(subset=[dropby])
df = df.reset_index(drop=True)

## for checking
# print(df.shape)
# print(df.head())

df.to_csv(output_name, index=True, sep='\t')
