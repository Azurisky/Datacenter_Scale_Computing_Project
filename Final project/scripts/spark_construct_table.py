from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import time
from pyspark.sql.types import *

def constructTrainingTable(Gene, Pathway, Toxicity):
    spark = SparkSession \
        .builder \
        .appName("Table") \
        .config("spark.mongodb.output.uri", "mongodb://localhost:27017") \
        .getOrCreate()

    gene = spark.read.csv(Gene, sep='\t', header=True)
    pathway = spark.read.csv(Pathway, sep='\t', header=True)
    toxicity = spark.read.csv(Toxicity, sep='\t', header=True)
    # 'Dermal' 'Inhalation' 'Oral'
    name = Toxicity.split(' ')[1]

    # Get unique chemical from CAS
    chem = set(toxicity.select("`C.CASRN`").rdd.flatMap(lambda x: x).collect())
    gene = gene.where(col('CasRN').isin(chem))
    pathway = pathway.where(col('CasRN').isin(chem))
    gene_inter = gene.select('GeneSymbol', 'InteractionActions')

    # Split interactions by |
    column = []
    for row in gene_inter.rdd.collect():
        tmp = row[1].split('|')
        for item in tmp:
            column.append('{} {}'.format(row[0], item))

    # Construct dataframe columns
    column = list(set(column))
    field_chem = [StructField('Chemical', StringType())]
    field_gene = [StructField(x, IntegerType()) for x in column]
    pathwayID = set(pathway.select('PathwayID').rdd.flatMap(lambda x: x).collect())
    field_pathwayID = [StructField(x, DoubleType()) for x in pathwayID]
    column.extend(pathwayID)
    column.append('GHS Label')
    field_ghs = [StructField('GHS Label', IntegerType())]
    index = list(chem)
    
    field = []
    field.extend(field_chem)
    field.extend(field_gene)
    field.extend(field_pathwayID)
    field.extend(field_ghs)
    schema = StructType(field)
    sc = spark.sparkContext
    # Create training data table
    df_train = spark.createDataFrame(sc.emptyRDD(), schema)
    
    # Loop through all the chemicals
    for cas in index:
        GHS = toxicity.where(col("`C.CASRN`") == cas).agg({"cGHSToxCat": "max"}).collect()[0][0]
        tmp_gene = gene.where(col('CasRN') == cas)
        tmp_pathway = pathway.where(col('CasRN') == cas)
        data = {column[i]: 0 for i in range(len(column))}
        gene_data = []
        for row in tmp_gene.rdd.collect():
            X = row[3].split('|')
            for i in X:
                gene_data.append('{} {}'.format(row[2], i))
        gene_data = list(set(gene_data))
        # Mark GeneSymbol and Interaction pair as 1
        for i in range(len(gene_data)):
            data[gene_data[i]] = 1

        # Mark PathwayID as PValue
        for row in tmp_pathway.rdd.collect():
            data[row[2]] = float(row[3])

        # Label GHS toxicity category
        data['GHS Label'] = int(GHS)
        # Write chemical
        data['Chemical'] = cas
        # Create a new row and append it to the dataframe
        df = spark.createDataFrame([data]).collect()
        df_train.union(spark.createDataFrame(df))

    df_train.write.format("com.mongodb.spark.sql.DefaultSource").mode("append").option("database", "toxicity").option("collection", name).save()

    
if __name__ == "__main__":
    start = time.time()
    Gene = '../data/Chemical_Gene_IXNS.tsv'
    Pathway = '../data/Chemical_Pathways.tsv'
    ToxicityMix = ['../data/Acute Dermal Toxicity - for project.tsv', '../data/Acute Inhalation Toxicity - for project.tsv', '../data/Acute Oral Toxicity - for project.tsv']
    for t in ToxicityMix:
        print(t)
        constructTrainingTable(Gene, Pathway, t)
    end = time.time()
    print("Total time: {}s.".format(end-start))
