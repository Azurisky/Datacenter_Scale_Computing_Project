#!/usr/bin/env python
import pandas as pd
import json
import time

def constructTrainingTable(Gene, Pathway, Toxicity):
    gene = pd.read_csv(Gene, sep='\t')
    pathway = pd.read_csv(Pathway, sep='\t')
    toxicity = pd.read_csv(Toxicity, sep='\t')
    name = Toxicity.split(' ')[1]

    chem = set()
    tmp = list(set(toxicity['C.CASRN'].values))
    for item in tmp:
        if (item in set(gene['CasRN'].values)) or (item in set(pathway['CasRN'].values)):
            chem.add(item)
    # print(len(chem))
    # print(len(tmp))
    # print(len(set(gene['CasRN'].values)))
    # print(len(set(pathway['CasRN'].values)))

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
    print(trainingTable.shape)
    trainingTable.T.to_csv('../data/trainingTableFor{}.tsv'.format(name), index=True, sep='\t')
    

def constructTestData(Gene, Pathway, Toxicity):
    gene = pd.read_csv(Gene, sep='\t')
    pathway = pd.read_csv(Pathway, sep='\t')
    toxicity = pd.read_csv(Toxicity, sep='\t')
    name = Toxicity.split(' ')[1]

    chem = set(toxicity['C.CASRN'].values)
    index_gene = gene[gene['CasRN'].apply(lambda x : x not in chem)]
    index_pathway = pathway[pathway['CasRN'].apply(lambda x : x not in chem)]

    index = set(index_gene['CasRN'].values)
    for i in set(index_pathway['CasRN'].values):
        index.add(i)

    col_gene = gene[gene['CasRN'].apply(lambda x : x in chem)]
    # print(gene[gene['CasRN'] == chem])
    col_pathway = pathway[pathway['CasRN'].apply(lambda x : x in chem)]
    gene_inter = col_gene[['GeneSymbol', 'InteractionActions']].values
    # print(gene[['GeneSymbol', 'InteractionActions']].values)
    col = []
    for line in gene_inter:
        tmp = line[1].split('|')
        for item in tmp:
            col.append('{} {}'.format(line[0], item))

    col = list(set(col))
    pathwayID = set(col_pathway['PathwayID'].values)
    col.extend(pathwayID)

    TestData = pd.DataFrame(0, index=col, columns=index)
    count = 0
    for cas in index:
        count += 1
        print(count)
        tmp_gene = index_gene[index_gene['CasRN'] == cas]
        tmp_pathway = index_pathway[index_pathway['CasRN'] == cas]
        replace = []
        for row in tmp_gene.index:
            line = tmp_gene.ix[row].values.tolist()
            # print(tmp_gene.ix[row].values.tolist())
            X = line[3].split('|')
            for i in X:
                tmp = '{} {}'.format(line[2], i)
                # if tmp in col:
                replace.append(tmp)
        replace = list(set(replace))
        # print(list(set(replace)))
        new = []
        for i in replace:
            if i in col:
                new.append(i)
        TestData[cas][new] = 1

        replace = []
        pValue = []
        for row in tmp_pathway.index:
            line = tmp_pathway.ix[row].values.tolist()
            # print(tmp_pathway.ix[row].values.tolist())
            # if line[2] in col:
            replace.append(line[2])
            pValue.append(line[3])
        new = []
        pNew = []
        for i, val in enumerate(replace):
            if val in col:
                new.append(val)
                pNew.append(pValue[i])

        TestData[cas][new] = pNew
    
    TestData.T.to_csv('../data/TestDataFor{}.tsv'.format(name), index=True, sep='\t')
    


if __name__ == "__main__":
    start = time.time()
    Gene = '../data/Chemical_Gene_IXNS.tsv'
    Pathway = '../data/Chemical_Pathways.tsv'
    ToxicityMix = ['../data/Acute Dermal Toxicity - for project.tsv', '../data/Acute Inhalation Toxicity - for project.tsv', '../data/Acute Oral Toxicity - for project.tsv']
    for t in ToxicityMix:
        print(t)
        constructTrainingTable(Gene, Pathway, t)
        # constructTestData(Gene, Pathway, t)
    end = time.time()
    print("Total time: {}s.".format(end-start))
