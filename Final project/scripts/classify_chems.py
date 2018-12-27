import numpy as np
import pandas as pd
import supervised_learning
from sklearn.model_selection import train_test_split
from sklearn.discriminant_analysis import LinearDiscriminantAnalysis
import sys

import warnings
warnings.filterwarnings("ignore")

TRAINING_FILE = sys.argv[1]

chem_df = pd.read_csv(TRAINING_FILE, sep="\t")
X_train_df = chem_df.drop(['GHS Label','Unnamed: 0'], axis=1)
y_train_df = chem_df['GHS Label']

lda = LinearDiscriminantAnalysis(n_components=100)
X_train_r = lda.fit(X_train_df, y_train_df).transform(X_train_df)

training_file_prefix = TRAINING_FILE.split('/')[-1]
print("%s training set dimensions:" % training_file_prefix)
print(X_train_df.shape)

# Get cross-validated performance
supervised_learning.classify_RF(True,
                                X_train_r,
                                None,
                                y_train_df,
                                None,
                                False,
                                'gini',
                                1000,
                                training_file_prefix)

if len(sys.argv) > 2:
    TEST_FILE = sys.argv[2]

    X_test_df = pd.read_csv(TEST_FILE, sep="\t")
    X_test_df = X_test_df.drop(['Unnamed: 0'], axis=1)
    print("%s test set dimensions:" % TEST_FILE)
    print(X_test_df.shape)
    X_test_r = lda.transform(X_test_df)

    y_pred = supervised_learning.classify_RF(   False,
                                                X_train_r,
                                                X_test_r,
                                                y_train_df,
                                                None,
                                                False,
                                                'gini',
                                                1000,
                                                training_file_prefix)

    np.save("%s_predictions.npy" % TEST_FILE.split('/')[-1].split('.')[:-1][0], y_pred)

