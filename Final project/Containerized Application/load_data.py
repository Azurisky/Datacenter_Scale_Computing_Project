import os
from pymongo import MongoClient
import pandas as pd
import json
import time

mng_client = MongoClient(os.environ['DB_PORT_27017_TCP_ADDR'], 27017)


def import_content(filepath):
    db_name = 'chemical'
    mng_db = mng_client[db_name]
    collection_name = 'pathway'
    db_cm = mng_db[collection_name]
    data = pd.read_csv(filepath, sep='\t')
    data_json = json.loads(data.to_json(orient='records'))
    db_cm.delete_many({})
    db_cm.insert_many(data_json)
    #print(db_cm.find({}).count())

def constructTrainingTable(Gene, Pathway, Toxicity):
    gene = pd.read_csv(Gene, sep='\t')
    pathway = pd.read_csv(Pathway, sep='\t')
    toxicity = pd.read_csv(Toxicity, sep='\t')
    name = Toxicity.split(' ')[1]

    chem = set(toxicity['C.CASRN'].values)
    gene = gene[gene['CasRN'].apply(lambda x : x in chem)]
    # print(gene[gene['CasRN'] == chem])
    pathway = pathway[pathway['CasRN'].apply(lambda x : x in chem)]
    gene_inter = gene[['GeneSymbol', 'InteractionActions']].values
    # print(gene[['GeneSymbol', 'InteractionActions']].values)
    col = []
    for line in gene_inter:
        tmp = line[1].split('|')
        for item in tmp:
            col.append('{} {}'.format(line[0], item))

    col = list(set(col))
    pathwayID = set(pathway['PathwayID'].values)
    col.extend(pathwayID)
    col.append('GHS Label')
    # chem = set(gene['CasRN'].values)
    # chem.add(i for i in pathway['CasRN'].values.tolist())
    index = list(chem)

    trainingTable = pd.DataFrame(0, index=col, columns=index)
    # trainingTable.fillna(0)
    # print(trainingTable)

    for cas in index:
        # print(cas)
        GHS = max(toxicity[toxicity['C.CASRN'] == cas]['cGHSToxCat'].values.tolist())
        # print(GHS)
        tmp_gene = gene[gene['CasRN'] == cas]
        tmp_pathway = pathway[pathway['CasRN'] == cas]
        replace = []
        for row in tmp_gene.index:
            line = tmp_gene.ix[row].values.tolist()
            # print(tmp_gene.ix[row].values.tolist())
            X = line[3].split('|')
            for i in X:
                replace.append('{} {}'.format(line[2], i))
        replace = list(set(replace))
        # print(list(set(replace)))
        trainingTable[cas][replace] = 1

        replace = []
        pValue = []
        for row in tmp_pathway.index:
            line = tmp_pathway.ix[row].values.tolist()
            # print(tmp_pathway.ix[row].values.tolist())
            replace.append(line[2])
            pValue.append(line[3])
        trainingTable[cas][replace] = pValue
        trainingTable[cas]['GHS Label'] = GHS

    trainingTable.T.to_csv('load_data/data/trainingTableFor{}.tsv'.format(name), index=True, sep='\t')



if __name__ == "__main__":
    filepath = '/load_data/data/Chemical_Pathways.tsv'
    import_content(filepath)

    start = time.time()
    Gene = '/load_data/data/Chemical_Gene_IXNS.tsv'
    Pathway = '/load_data/data/Chemical_Pathways.tsv'
    ToxicityMix = ['/load_data/data/Acute Dermal Toxicity - for project.tsv', '/load_data/data/Acute Inhalation Toxicity - for project.tsv', '/load_data/data/Acute Oral Toxicity - for project.tsv']
    for t in ToxicityMix:
        print("\n*10")
        print(t)
        print("\n*10")
        constructTrainingTable(Gene, Pathway, t)
    end = time.time()
    print("Total time: {}s.".format(end-start))
