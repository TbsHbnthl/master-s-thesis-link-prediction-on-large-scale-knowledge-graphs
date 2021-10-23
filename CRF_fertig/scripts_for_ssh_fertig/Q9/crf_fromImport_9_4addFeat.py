from py2neo import Graph
from configparser import ConfigParser
import time
import csv
from sklearn.model_selection import train_test_split
import sklearn_crfsuite
from sklearn_crfsuite import metrics
import multiprocessing as mp
from multiprocessing import Pool
from functools import partial
import pickle

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

    features = {}
    for a in attributes:
        features[a] = ""

    features['Source'] = ""
    features['File'] = ""
    features['Usage'] = ""
    features['Sex'] = ""
    try:
        del features[""]
    except KeyError:
        pass
    # add previous and next for paths with more nodes
    features['previous'] = ""
    features['next'] = ""

    return features


def create_sents():

    # get paths from query (sorted)
    x = 'pathNode1'
    y = 'pathNode2'
    #Q1 q_path = graph.run(f"MATCH (p:Patient)-[]-(a) RETURN p.nodeUID as patientNode, a.nodeUID as labelNode, gds.alpha.linkprediction.commonNeighbors(p,a) AS score ORDER BY p.nodeUID,score DESC,a.nodeUID").data()
    #Q2 q_path = graph.run(f"MATCH (p:Patient)-[]-(a) RETURN p.nodeUID as {x}, a.nodeUID as labelNode, gds.alpha.linkprediction.totalNeighbors(p,a) AS score ORDER BY p.nodeUID,score,a.nodeUID").data()
    #Q3 q_path = graph.run(f"MATCH (p:General_Image)-[]-(a) RETURN p.nodeUID as {x}, a.nodeUID as labelNode, gds.alpha.linkprediction.totalNeighbors(p,a) AS score ORDER BY p.nodeUID,score,a.nodeUID").data() (FUNKTIONIERT NICHT)
    #Q4 q_path = graph.run(f"MATCH (p:General_Image)-[]-(a) RETURN p.nodeUID as imageNode, a.nodeUID as labelNode ORDER BY p.nodeUID,a.nodeUID")
    #05 q_path = graph.run(f"MATCH (p:Date)-[]-(a) RETURN p.nodeUID as dateNode, a.nodeUID as labelNode ORDER BY p.nodeUID,a.nodeUID").data()
    #Q9
    q_path = graph.run(f"MATCH (n1)-[]-(p1:Patient)-[]-(p2:General_Study)-[]-(n2) RETURN p1.nodeUID as pathNode1, p2.nodeUID as pathNode2, n1 as NeighbourOfp1, n2 as NeighbourOfp2 ORDER BY p1.nodeUID, p2.nodeUID, n1.nodeUID, n2.nodeUID").data()
    #Q10 q_path = graph.run(f"MATCH (n1)-[]-(p1:Patient)-[]-(p2:General_Study)-[]-(n2) RETURN p1.nodeUID as pathNode1, p2.nodeUID as pathNode2, n1 as NeighbourOfp1, n2 as NeighbourOfp2, gds.alpha.linkprediction.commonNeighbors(p1,n1) as scoren1, gds.alpha.linkprediction.commonNeighbors(p2,n2) as scoren2 ORDER BY p1.nodeUID, p2.nodeUID, scoren1 DESC, scoren2 DESC").data()
    # create sents from q_path:
    # regulate length of paths from query for later creation of sents
    #for element in q_path:
    #    print(element)
    path_Nodes = []
    path_length = 2
    for i in range(path_length):
        path_Nodes.append('pathNode'+str(i+1))

    # go through each element of q_path (i.e. through each dictionary, which is equivalent to a full path, in the list)
    for element in q_path:

        nodeconcat = ""
        # stores sentence from nodes
        s = []
        sentsDic = {}
        # path_Nodes has elements like pathNode1, pathNode2, etc.a
        # so it loops through every available path node
        for node in path_Nodes:
            nodeconcat = nodeconcat + str(element[node])
            sentsDic[element[node]] = element['NeighbourOfp' + node[-1]]['nodeUID']

        if nodeconcat not in check_for_doubles:
            check_for_doubles.add(nodeconcat)
            # for each node/word the node and its label are appended as a tuple to the sentence
            for key in sentsDic:
                s.append((key, sentsDic[key]))
            sents.append(s)


def create_sents_from_import():

    q_path_import = []
    row_counter = 0
    with open('exportQ9.csv', newline='') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            row_counter = row_counter + 1
            if row_counter > 1:
                q_path_import.append(row)

    duplicates = set()


    for element in q_path_import:
        sent = []
        node1 = element[0]
        node2 = element[1]
        nodeconcat = str(node1) + str(node2)
        if nodeconcat not in duplicates:
            #duplicates.add(nodeconcat)
            neighOfp1 = element[2]
            neighOfp2 = element[3]
            #sc1 = element[4]
            #sc2 = element[5]
            tup1 = (node1, neighOfp1)
            tup2 = (node2, neighOfp2)
            sent = [tup1, tup2]
            sents.append(sent)



# returns the labels
def sent2labels(sent):
    return [set_of_labels for node, set_of_labels in sent]


def sent2features(sent, d):
    return [nodes2features(sent, i, d) for i in range(len(sent))]


def nodes2features(s, i, d):

    node = s[i][0]
    try:
        nxt = s[i + 1][0]
    except:
        nxt = 'this_is_the_end_node'
    if i > 0:
        prev = s[i - 1][0]
    else:
        prev = 'this_is_the_start_node'

    ft = d

    graph = Graph("bolt:///localhost:7474/", auth=("neo4j", "0000"))
    temp1 = '{'
    temp2 = '}'

    for key in ft:
        if key != 'previous' and key != 'next':
            ft[key] = str(list(graph.run(f"MATCH (n {temp1}nodeUID:'{node}'{temp2}) RETURN n.{key}").data()[0].items())[0][1])

    ft['next'] = nxt
    ft['previous'] = prev

    # add feature of node degree
    ft['ndeg'] = graph.run(f"MATCH (n {temp1}nodeUID:'{node}'{temp2})-[r]-() RETURN COUNT(r)").data()[0]['COUNT(r)']
    # add feature of node type
    ft['ntype'] = graph.run(f"MATCH (n {temp1}nodeUID:'{node}'{temp2}) RETURN labels(n)").data()[0]['labels(n)'][0]
    # type of the most common outgoing relationship
    ft['routtype'] = graph.run(f"MATCH(n {temp1}nodeUID:'{node}'{temp2})-[r]->() RETURN type(r) as type, COUNT(r) as score ORDER BY score DESC LIMIT 1").data()[0]['type']
    # type of the most common incoming relationship
    ft['rintype'] = graph.run(f"MATCH(n {temp1}nodeUID:'{node}'{temp2})<-[r]-() RETURN type(r) as type, COUNT(r) as score ORDER BY score DESC LIMIT 1").data()[0]['type']

    return ft



def main(t_sents, ft):

    pool = Pool(mp.cpu_count())
    prt = partial(sent2features, d=ft)
    p = pool.map(prt, t_sents)

    pool.join
    return p


if __name__ == '__main__':
    # create feature dictionary
    features = {}
    list_of_shared_vars = []


    print('starting main')

    attributes = set()

    # read in graph from neo4j
    graph = Graph("bolt:///localhost:7474/", auth=("neo4j", "0000"))



    # create empty dictionary to store starting nodes and the corresponding label
    # sentsDic = {}
    sentsDicList = []
    # create list to store sentences/paths
    sents = []

    check_for_doubles = set()

    # create sentences from given q_path:
    print('Die sents werden erstellt.')
    p1 = 'Die sents werden erstellt.'
    tic1 = time.time()
    create_sents_from_import()
    toc1 = time.time()
    t1 = (toc1 - tic1)
    print(f"Die Erstellung der sents hat {t1} Sekunden gedauert.")
    p2 = f"Die Erstellung der sents hat {t1} Sekunden gedauert."
    # this is what an element from sents looks like:
    print(f"Beispiel aus sents (Liste): {sents[0]}")
    print("--------------------------------------------------------------------------------------------------------------")

    # split data:
    print("Die Daten werden in Test- und Trainingsmenge (train_sents und test_sents) geteilt")
    tic2 = time.time()
    train_sents, test_sents = train_test_split(sents, test_size=0.2)
    toc2 = time.time()
    t2 = toc2 - tic2
    print(f"Die Teilung der Daten hat {t2} Sekunden gedauert.")
    print("--------------------------------------------------------------------------------------------------------------")

    # store features:
    print('Die Features werden ausgelesen und in einem Dictionary gespeichert.')
    # create empty features
    tic3 = time.time()
    features = read_list_of_all_attributes()
    toc3 = time.time()
    t3 = toc3 - tic3
    print(f"Folgende Features wurden gefunden: {features}")
    print(f"Die Erstellung der Features hat {t3} Sekunden gedauert.")
    print("--------------------------------------------------------------------------------------------------------------")

    print(f"Ein Beispiel aus der Menge train_sents: {train_sents[0]}.")
    print(f"train_sents hat die Laenge {len(train_sents)}.")
    print(f"Ein Beispiel aus der Menge test_sents: {test_sents[0]}.")
    print(f"test_sents hat die Laenge {len(test_sents)}.")
    print("Ein Beispiel fuer die Daten, nachdem sie an sent2features und sent2labels gesendet wurden:")
    print(f"train_sents[0]: {train_sents[0]}")
    # don't print because it's too long
    #print(f"y_train an Stelle 0: {sent2labels(train_sents[0])[0]}")
    #print(f"X_train an Stelle 0: {sent2features(train_sents[0])[0]}")
    print("--------------------------------------------------------------------------------------------------------------")

    # training part 1: setting up the data
    print(f"Sende die Knoten jedes sents aus X_train an die nodes_to_features-Methode, die dann die Features fuer jeden Knoten des sents ausliest")
    print(len(train_sents))
    tic4 = time.time()
    X_train = main(train_sents, features)
    toc4 = time.time()
    t4 = toc4 - tic4
    print(f"Die Methode sent_to_features hat fuer X_train {t4} Sekunden gebraucht.")
    print(len(X_train))
    #print(X_train)
    print("--------------------------------------------------------------------------------------------------------------")

    print(f"Fuer jeden sent in y_train wird das Label fuer jeden enthaltenen Knoten gefunden.")
    tic5 = time.time()
    y_train = [sent2labels(s) for s in train_sents]
    toc5 = time.time()
    t5 = toc5 - tic5
    print(f"Die Methode sent_to_labels hat {t5} Sekunden gedauert.")
    print("--------------------------------------------------------------------------------------------------------------")

    print(f"Sende die Knoten jedes sents aus X_test an die nodes_to_features-Methode, die dann die Features fuer jeden Knoten des sents ausliest")
    tic6 = time.time()
    X_test = main(test_sents, features)
    toc6 = time.time()
    t6 = toc6 - tic6
    print(f"Die Methode sent_to_features hat fuer X_test {t6} Sekunden gebraucht.")
    print("--------------------------------------------------------------------------------------------------------------")

    print(f"Fuer jeden sent in y_test wird das Label fuer jeden enthaltenen Knoten gefunden.")
    tic7 = time.time()
    y_test = [sent2labels(s) for s in test_sents]
    toc7 = time.time()
    t7 = toc7 - tic7
    print(f"Die Methode sent_to_labels hat {t7} Sekunden gedauert.")
    print("--------------------------------------------------------------------------------------------------------------")


    print("--------------------------------------------------------------------------------------------------------------")
    # training part 2: actual crf
    print("Das eigentliche Training mithilfe der CRFs wird durchgefuehrt.")
    print("Training...")
    tic8 = time.time()
    crf = sklearn_crfsuite.CRF(algorithm='lbfgs', c1=0.1, c2=0.1, max_iterations=100, all_possible_transitions=True)
    crf.fit(X_train, y_train)
    toc8 = time.time()
    t8 = toc8 - tic8
    print(f"Das Training ist beendet. Es hat {t8} Sekunden gebraucht.")
    print("--------------------------------------------------------------------------------------------------------------")

    print("Evaluation des Trainings:")
    # print out labels
    labels = list(crf.classes_)
    print(f"Beispiel fuer ein Knotenlabel:")
    print(labels[0])
    print("Prediction-Vektor wird erstellt.")
    tic9 = time.time()
    y_pred = crf.predict(X_test)
    toc9 = time.time()
    t9 = toc9 - tic9
    print(f"Die Erstellung des Prediction-Vektors hat {t9} Sekunden gedauert.")
    print("--------------------------------------------------------------------------------------------------------------")

    print("Der F1-Score wird fuer alle Label berechnet")
    tic10 = time.time()
    m1 = metrics.flat_f1_score(y_test, y_pred, average='weighted', labels=labels, zero_division=0)
    print(m1)
    toc10 = time.time()
    t10 = toc10 - tic10
    print(f"Die Berechnung des F1-Scores hat {t10} Sekunden gedauert.")
    print("--------------------------------------------------------------------------------------------------------------")

    print("Betrachte einzelne Labels im Detail:")
    tic11 = time.time()
    sorted_labels = sorted(labels, key=lambda name: (name[1:], name[0]))
    print(metrics.flat_classification_report(y_test, y_pred, labels=sorted_labels, digits=4, zero_division=0))
    toc11 = time.time()
    t11 = toc11 - toc11
    print(f"Die Betrachtung der Labels im Detail hat {t11} Sekunden gedauert.")
