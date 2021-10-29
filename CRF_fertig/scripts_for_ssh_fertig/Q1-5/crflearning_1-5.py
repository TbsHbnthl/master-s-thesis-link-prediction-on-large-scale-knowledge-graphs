from py2neo import Graph
import py2neo as p2n
import os
import pydicom
from glob import glob
from configparser import ConfigParser
import time
import csv
from sklearn.model_selection import train_test_split
import sklearn_crfsuite
from sklearn_crfsuite import metrics
import numpy as np
from multiprocessing import Pool
import multiprocessing

attributes = set()
# create feature dictionary
features = {}


# this method reads all possible attributes of the nodes by reading the config file and creating a
# suitable dictionary from it.
# It is then stored in the dictionary "features" above
def read_list_of_all_attributes():
    # print out all attributes
    parser = ConfigParser()
    parser.read('dev.ini')

    sections = parser.sections()
    print(sections)
    for sec in sections:
        b = parser[sec]['attributes'].split(',')
        for element in b:
            attributes.add(element)


    with open("attributes.csv", 'w') as csvfile:
        attributes_writer = csv.writer(csvfile)
        for a in attributes:
            attributes_writer.writerow([a])

    for a in attributes:
        features[a] = ""

    features['Source'] = ""
    features['File'] = ""
    features['Usage'] = ""
    features['Sex'] = ""
    del features[""]

# read in graph from neo4j
graph = Graph("bolt:///localhost:7474/", auth=("neo4j", "0000"))



# create empty dictionary to store starting nodes and the corresponding label
sentsDic = {}
# create list to store sentences/paths
sents = []


# creates sentences (paths) from the graph and stores them in an analogous way to the tutorial
def create_sents():

    # get paths from query (sorted)
    x = 'dateNode'
    Q1 q_path = graph.run(f"MATCH (p:Patient)-[]-(a) RETURN p.nodeUID as patientNode, a.nodeUID as labelNode, gds.alpha.linkprediction.commonNeighbors(p,a) AS score ORDER BY p.nodeUID,score DESC,a.nodeUID").data()
    #Q2 q_path = graph.run(f"MATCH (p:Patient)-[]-(a) RETURN p.nodeUID as {x}, a.nodeUID as labelNode, gds.alpha.linkprediction.totalNeighbors(p,a) AS score ORDER BY p.nodeUID,score,a.nodeUID").data()
    #Q3 q_path = graph.run(f"MATCH (p:General_Image)-[]-(a) RETURN p.nodeUID as imageNode, a.nodeUID as labelNode, gds.alpha.linkprediction.totalNeighbors(p,a) AS score ORDER BY p.nodeUID,score,a.nodeUID").data()
    #Q4 q_path = graph.run(f"MATCH (p:General_Image)-[]-(a) RETURN p.nodeUID as imageNode, a.nodeUID as labelNode ORDER BY p.nodeUID,a.nodeUID").data()
    #Q5 q_path = graph.run(f"MATCH (p:Date)-[]-(a) RETURN p.nodeUID as dateNode, a.nodeUID as labelNode ORDER BY p.nodeUID,a.nodeUID").data()
    # create sents from q_path

    # get stat nodes
    # go through each element of q_path, store it as a key in a dictionary and the list of labels as its value
    for element in q_path:

        start_node = element[x]

        # automatically only uses the first one. As the query is ordered by score only the node with the highest score is being picked
        if start_node not in sentsDic:
            sentsDic[start_node] = str(element['labelNode'])

    # for each (start)node in q_path store it together with the list of labels as a small list. Append the whole
    # sentence as a list to sents (here sentence = 1 word = 1 node)
    for key in sentsDic:
        sents.append([[key, sentsDic[key]]])


def create_sents_from_import():
    q_path_import = []
    row_counter = 0
    with open('exportQ3.csv', newline='') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            row_counter = row_counter + 1
            if row_counter > 1:
                q_path_import.append(row)

    duplicates = set()

    for element in q_path_import:
        sent = []
        node1 = element[0]
        if node1 not in duplicates:
            neighOfp1 = element[1]
            sc1 = element[2]
            sent = [(node1, neighOfp1)]
            sents.append(sent)



print('Die sents werden erstellt.')
# create sentences from given q_path
tic1 = time.time()
create_sents()
toc1 = time.time()
t1 = (toc1-tic1)
print(f"Die Erstellung der sents hat {t1} Sekunden gedauert.")
# this is what an element from sents looks like:
print(f"Beispiel aus sents (Liste): {sents[0]}")

print("--------------------------------------------------------------------------------------------------------------")

# split data:
print("Die Daten werden in Test- und Trainingsmenge (train_sents und test_sents) geteilt")
tic2 = time.time()
train_sents, test_sents = train_test_split(sents, test_size=0.2)
toc2 = time.time()
t2 = toc2-tic2
print(f"Die Teilung der Daten hat {t2} Sekunden gedauert.")

print("--------------------------------------------------------------------------------------------------------------")

#print(f"Länge von sents: {len(sentsDic)}")
print('Die Features werden ausgelesen und in einem Dictionary gespeichert.')
# create empty features
tic3 = time.time()
read_list_of_all_attributes()
toc3 = time.time()
t3 = toc3-tic3
#print(f"Folgende Features wurden gefunden: {features}")
print(f"Die Erstellung der Features hat {t3} Sekunden gedauert.")

print("--------------------------------------------------------------------------------------------------------------")

# gets a sentence (path) s and an iteration character i for sent2labels
# then finds all features for the nodes in the sentence via graph query
# non-existant features will be set to null
def nodes_to_features(s, i):

    node = s[i][0]

    ft = features

    for key in ft:
        temp1 = '{'
        temp2 = '}'
        # old
        #ft[key] = graph.run(f"MATCH (n {temp1}nodeUID:'{node}'{temp2}) RETURN n.{key}").data()
        # new
        ft[key] = str(list(graph.run(f"MATCH (n {temp1}nodeUID:'{node}'{temp2}) RETURN n.{key}").data()[0].items())[0][1])
        #SO_EIN_MIST_1 = list(graph.run(f"MATCH (n {temp1}nodeUID:'{node}'{temp2}) RETURN n.{key}").data()[0].items())[0][1]

    return ft

# returns the sentence with features found for each node in it
def sent2features(sent):
    return [nodes_to_features(sent, i) for i in range(len(sent))]

# returns the labels
def sent2labels(sent):
    return [set_of_labels for node, set_of_labels in sent]


print(f"Ein Beispiel aus der Menge train_sents: {train_sents[0]}.")
print(f"train_sents hat die Länge {len(train_sents)}.")
print(f"Ein Beispiel aus der Menge test_sents: {test_sents[0]}.")
print(f"test_sents hat die Länge {len(test_sents)}.")

print("Ein Beispiel für die Daten, nachdem sie an sent2features und sent2labels gesendet wurden:")
print(f"y_train an Stelle 0: {sent2labels(train_sents[0])[0]}")
print(f"X_train an Stelle 0: {sent2features(train_sents[0])[0]}")

print("--------------------------------------------------------------------------------------------------------------")


# training part 1: setting up the data
print(f"Sende die Knoten jedes sents aus X_train an die nodes_to_features-Methode, die dann die Features für jeden Knoten des sents ausliest")
tic4 = time.time()
X_train = [sent2features(s) for s in train_sents]
toc4 = time.time()
t4 = toc4-tic4
print(f"Die Methode sent_to_features hat für X_train {t4} Sekunden gebraucht.")

print("--------------------------------------------------------------------------------------------------------------")


print(f"Für jeden sent in y_train wird das Label für jeden enthaltenen Knoten gefunden.")
tic5 = time.time()
y_train = [sent2labels(s) for s in train_sents]
toc5 = time.time()
t5 = toc5-tic5
print(f"Die Methode sent_to_labels hat {t5} Sekunden gedauert.")

print("--------------------------------------------------------------------------------------------------------------")


print(f"Sende die Knoten jedes sents aus X_test an die nodes_to_features-Methode, die dann die Features für jeden Knoten des sents ausliest")
tic6= time.time()
X_test = [sent2features(s) for s in test_sents]
toc6 = time.time()
t6 = toc6-tic6
print(f"Die Methode sent_to_features hat für X_test {t6} Sekunden gebraucht.")

print("--------------------------------------------------------------------------------------------------------------")

print(f"Für jeden sent in y_test wird das Label für jeden enthaltenen Knoten gefunden.")
tic7 = time.time()
y_test = [sent2labels(s) for s in test_sents]
toc7 = time.time()
t7 = toc7-tic7
print(f"Die Methode sent_to_labels hat {t7} Sekunden gedauert.")

print("--------------------------------------------------------------------------------------------------------------")
print("--------------------------------------------------------------------------------------------------------------")


# training part 2: actual crf
print("Das eigentliche Training mithilfe der CRFs wird durchgeführt.")
print("Training...")
tic8 = time.time()
crf = sklearn_crfsuite.CRF(algorithm='lbfgs', c1=0, c2=1.0, max_iterations=100, all_possible_transitions=True)
crf.fit(X_train, y_train)
toc8 = time.time()
t8 = toc8-tic8
print(f"Das Training ist beendet. Es hat {t8} Sekunden gebraucht.")

print("--------------------------------------------------------------------------------------------------------------")


print("Evaluation des Trainings:")
# print out labels
labels = list(crf.classes_)
print(f"Die Knotenlabels:")
print(labels)

print("Prediction-Vektor wird erstellt.")
tic9 = time.time()
y_pred = crf.predict(X_test)
toc9 = time.time()
t9 = toc9-tic9
print(f"Die Erstellung des Prediction-Vektors hat {t9} Sekunden gedauert.")

print("--------------------------------------------------------------------------------------------------------------")

print("Der F1-Score wird für alle Label berechnet")
tic10 = time.time()
m1 = metrics.flat_f1_score(y_test, y_pred, average='weighted', labels=labels, zero_division=1)
print(m1)
toc10 = time.time()
t10 = toc10-tic10
print(f"Die Berechnung des F1-Scores hat {t10} Sekunden gedauert.")

print("--------------------------------------------------------------------------------------------------------------")

print("Betrachte einzelne Labels im Detail:")
tic11 =  time.time()
sorted_labels = sorted(labels, key = lambda name: (name[1:], name[0]))
print(metrics.flat_classification_report(y_test, y_pred, labels=sorted_labels, digits=4, zero_division=1))
toc11 = time.time()
t11 = toc11-toc11
print(f"Die Betrachtung der Labels im Detail hat {t11} Sekunden gedauert.")
