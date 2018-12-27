import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.ensemble import AdaBoostClassifier
from sklearn.naive_bayes import MultinomialNB
from sklearn.linear_model import LogisticRegression
from sklearn.svm import SVC
from sklearn.metrics import confusion_matrix, f1_score, classification_report, roc_curve, auc, accuracy_score
from sklearn.model_selection import cross_val_score
from sklearn.model_selection import GridSearchCV
import pickle

# to prevent display weirdness when running in Pando:
import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
plt.ioff()


OUTPUT_PREFIX = './'


def classify_RF(cross_validation, my_X_train, my_X_test, my_y_train, my_y_test, bootstrap, criterion, n_estimators, test_file_prefix):
    if cross_validation:
        clf = RandomForestClassifier(bootstrap=bootstrap, criterion=criterion, n_estimators=n_estimators)   # Determined by hyperparameter optimization
        # 5-fold cross-validation
        scores = cross_val_score(clf, my_X_train, my_y_train, cv=5, scoring="f1_weighted")
        print(scores)
        print("-------------------------------------------------------------------  ")
        print("Mean weighted F1-score: %f" % np.mean(scores))
        print("Std weighted F1-score: %f" % np.std(scores))
        print("-------------------------------------------------------------------  ")
    else:
        clf = RandomForestClassifier(bootstrap=bootstrap, criterion=criterion, n_estimators=n_estimators)   # Determined by hyperparameter optimization
        print(clf)
        clf.fit(my_X_train, my_y_train)
        y_pred = clf.predict(my_X_test)

        if my_y_test:
            print("-------------------------------------------------------------------")
            print("Random Forests Confusion Matrix:")
            print(confusion_matrix(my_y_test, y_pred))
            print("-------------------------------------------------------------------")
            print("F1-score (macro): %.3f" % f1_score(my_y_test, y_pred, average='macro'))
            print("F1-score (micro): %.3f" % f1_score(my_y_test, y_pred, average='micro'))
            print("F1-score (weighted): %.3f\n" % f1_score(my_y_test, y_pred, average='weighted'))
            print(classification_report(my_y_test, y_pred))
            print("-------------------------------------------------------------------")
            print("Accuracy: {}".format(accuracy_score(my_y_test, y_pred)))

        # Store the classifier to reuse later
        pickle.dump(clf, open("%s/%s_RF_clf.pickle" % (OUTPUT_PREFIX, test_file_prefix), 'wb'))

        return(y_pred)


def classify_SVM(cross_validation, my_X_train, my_X_test, my_y_train, my_y_test, kernel, C, gamma, test_file_prefix):
    # Try Support Vector Machines
    if cross_validation:
        clf = SVC(C=C, kernel=kernel, gamma=gamma)
        # 5-fold cross-validation
        scores = cross_val_score(clf, my_X_train, my_y_train, cv=5, scoring="f1_weighted")
        print(scores)
        print("-------------------------------------------------------------------  ")
        print("Mean weighted F1-score: %f" % np.mean(scores))
        print("Std weighted F1-score: %f" % np.std(scores))
        print("-------------------------------------------------------------------  ")
    else:
        clf = SVC(C=C, kernel=kernel, gamma=gamma)
        print(clf)
        clf.fit(my_X_train, my_y_train)
        y_pred = clf.predict(my_X_test)

        if my_y_test:
            print("-------------------------------------------------------------------")
            print("SVM Confusion Matrix:")
            print(confusion_matrix(my_y_test, y_pred))
            print("-------------------------------------------------------------------")
            print("F1-score (macro): %.3f" % f1_score(my_y_test, y_pred, average='macro'))
            print("F1-score (micro): %.3f" % f1_score(my_y_test, y_pred, average='micro'))
            print("F1-score (weighted): %.3f\n" % f1_score(my_y_test, y_pred, average='weighted'))
            print(classification_report(my_y_test, y_pred))
            print("-------------------------------------------------------------------")
            print("Accuracy: {}".format(accuracy_score(my_y_test, y_pred)))

        # Store the classifier to reuse later
        pickle.dump(clf, open("%s/%s_SVM_clf.pickle" % (OUTPUT_PREFIX, test_file_prefix), 'wb'))

        return(y_pred)


def classify_AdaBoost(cross_validation, my_X_train, my_X_test, my_y_train, my_y_test, n_estimators, algorithm, test_file_prefix):
    if cross_validation:
        clf = AdaBoostClassifier(n_estimators=n_estimators, algorithm=algorithm)
        # 5-fold cross-validation
        scores = cross_val_score(clf, my_X_train, my_y_train, cv=5, scoring="f1_weighted")
        print(scores)
        print("-------------------------------------------------------------------  ")
        print("Mean weighted F1-score: %f" % np.mean(scores))
        print("Std weighted F1-score: %f" % np.std(scores))
        print("-------------------------------------------------------------------  ")
    else:
        clf = AdaBoostClassifier(n_estimators=n_estimators, algorithm=algorithm)
        print(clf)
        clf.fit(my_X_train, my_y_train)
        y_pred = clf.predict(my_X_test)

        if my_y_test:
            print("-------------------------------------------------------------------")
            print("AdaBoost Confusion Matrix:")
            print(confusion_matrix(my_y_test, y_pred))
            print("-------------------------------------------------------------------")
            print("F1-score (macro): %.3f" % f1_score(my_y_test, y_pred, average='macro'))
            print("F1-score (micro): %.3f" % f1_score(my_y_test, y_pred, average='micro'))
            print("F1-score (weighted): %.3f\n" % f1_score(my_y_test, y_pred, average='weighted'))
            print(classification_report(my_y_test, y_pred))
            print("-------------------------------------------------------------------")
            print("Accuracy: {}".format(accuracy_score(my_y_test, y_pred)))

        # Store the classifier to reuse later
        pickle.dump(clf, open("%s/%s_AdaBoost_clf.pickle" % (OUTPUT_PREFIX, test_file_prefix), 'wb'))

        return(y_pred)


def classify_NB(cross_validation, my_X_train, my_X_test, my_y_train, my_y_test, test_file_prefix):
    if cross_validation:
        clf = MultinomialNB()
        # 5-fold cross-validation
        scores = cross_val_score(clf, my_X_train, my_y_train, cv=5, scoring="f1_weighted")
        print(scores)
        print("-------------------------------------------------------------------  ")
        print("Mean weighted F1-score: %f" % np.mean(scores))
        print("Std weighted F1-score: %f" % np.std(scores))
        print("-------------------------------------------------------------------  ")
    else:
        clf = MultinomialNB()
        print(clf)
        clf.fit(my_X_train, my_y_train)
        y_pred = clf.predict(my_X_test)

        if my_y_test:
            print("-------------------------------------------------------------------")
            print("Naive Bayes Confusion Matrix:")
            print(confusion_matrix(my_y_test, y_pred))
            print("-------------------------------------------------------------------")
            print("F1-score (macro): %.3f" % f1_score(my_y_test, y_pred, average='macro'))
            print("F1-score (micro): %.3f" % f1_score(my_y_test, y_pred, average='micro'))
            print("F1-score (weighted): %.3f\n" % f1_score(my_y_test, y_pred, average='weighted'))
            print(classification_report(my_y_test, y_pred))
            print("-------------------------------------------------------------------")
            print("Accuracy: {}".format(accuracy_score(my_y_test, y_pred)))

        # Store the classifier to reuse later
        pickle.dump(clf, open("%s/%s_NB_clf.pickle" % (OUTPUT_PREFIX, test_file_prefix), 'wb'))

        return(y_pred)


