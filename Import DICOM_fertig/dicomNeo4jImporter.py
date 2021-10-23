import os
import pydicom
from glob import glob
from configparser import ConfigParser
import time
import csv

# ---------------------------------------------------------------------------------------------------------------------

# import the config ini and extract the information

# import config ini (must be done before changing directories
parser = ConfigParser()
parser.read('dev.ini')

# get the sections from the ini
sections = parser.sections()


# get all possible relation types
allRelationTypes = []
for section in sections:
    allRelationTypes = allRelationTypes + (parser[section]['relationtypes'].split(','))
allRelationTypes = list(dict.fromkeys(allRelationTypes))

allRelationTypes_with_static = allRelationTypes + ['hasPatient', 'hasFile', 'belongsToPatient', 'hasSex', 'wasBornOn', 'hasDate', 'hasInstanceCreationDate', 'hasAcquisitionDate']

# create empty dictionary to later store node uids to avoid doubles
UIDs = {}
for section in sections:
    UIDs[section] = set()


# ---------------------------------------------------------------------------------------------------------------------

# create "static" nodes (not dependant on input data)

# create sex nodes
class Sex:
    def __init__(self, s):
        self.ID = s
        self.Identifier = s

sexNodes = [Sex('M'), Sex('F')]

# create usage nodes
class Usage:
    def __init__(self, _usage):
        self.ID = _usage
        self.Identifier = _usage

usageNodes = [Usage('Mandatory'), Usage('Conditional'), Usage('UserOptional')]

class Date:
    def __init__(self, d):
        self.__dict__ = d

class File:
    def __init__(self, f):
        self.ID = f
        self.Identifier = f


class Source:
    def __init__(self, s):
        self.ID = s
        self.Source = s


# store all source nodes
all_sources = []

# store all files (use set to avoid duplicates)
file_names = set()
file_relations = []

# store all sources
sources = set()
source_file_relations = []
source_patient_relations = []



# store date ids
dates = set()

# ---------------------------------------------------------------------------------------------------------------------
# dic for getting fitting node ID

# ---------------------------------------------------------------------------------------------------------------------


# get all keys for the sections from the ini-file
# all sections have the same keys, therefore only one is needed to extract those keys (for more complicated ini-files
# his has to be done for the different sections)
#keys = []
#print(parser['Patient'])
#for key in parser['Patient']:
#    keys.append(key)
#print(keys)

# store created nodes
storedNodes = dict()
for section in sections:
    storedNodes[section] = []
storedNodes['Date'] = []
storedNodes['File'] = []

# store created relations for each relation type
storedRelations = dict()
for rType in allRelationTypes:
    storedRelations[rType] = []
storedRelations['hasSex'] = []
# store relationship types from X --> Date and Time
additional_Relations_to_DateAndTime = set()
# get all rel typed to the node Date and Time
for element in sections:
    if parser[element]['Date'] != '':
        temp = parser[element]['Date'].split(',')
        add_rel_Type = temp[1]
        additional_Relations_to_DateAndTime.add(add_rel_Type)
# add a key for each of these types to store the relations
for element in additional_Relations_to_DateAndTime:
    storedRelations[element] = []

class Node:
    def __init__(self, dicOfAttr):
        self.__dict__ = dicOfAttr


class Relation:
    def __init__(self, _start, _end, _type, _prov):
        self.start_id = _start
        self.end_id = _end
        self.rel_type = _type
        self.provenance = _prov


# ---------------------------------------------------------------------------------------------------------------------

def print_static(static_nodes, static_type):
    # create new dir if necessary
    export_nodes_to = '/Users/tobias/Library/Mobile Documents/com~apple~CloudDocs/Documents/Uni/Masterarbeit/Datengrundlage/Cancer Imaging Archive/Import/nodes'

    if not os.path.exists(export_nodes_to):
        os.makedirs(export_nodes_to)

    os.chdir(export_nodes_to)

    # write file
    with open(f"{static_type}.csv", 'w') as csvfile:
        csv_node_writer = csv.writer(csvfile)
        for n in static_nodes:
            csv_node_writer.writerow([n.ID, n.Identifier])


print_static(usageNodes, 'Usage')
print_static(sexNodes, 'Sex')

# ---------------------------------------------------------------------------------------------------------------------

# find root directory
rootdir = '/Users/tobias/Library/Mobile Documents/com~apple~CloudDocs/Documents/Uni/Masterarbeit/Datengrundlage/Cancer Imaging Archive/manifest-cgqtDj7Y2699835271585651107/SPIE-AAPM Lung CT Challenge'
# for testing purposes:
#rootdir = '/Users/tobias/Library/Mobile Documents/com~apple~CloudDocs/Documents/Uni/Masterarbeit/Datengrundlage/Cancer Imaging Archive/manifest-cgqtDj7Y2699835271585651107/SPIE-AAPM Lung CT Challenge COPY'
# change to root directory
os.chdir(rootdir)
# get all subdirectories that contain dcm files (specified by the given data structure)
paths = glob(f'{rootdir}/*/*/*')


tic = time.time()

# use each subdirectory
for elem in paths:
    # change directory to subdirectory
    os.chdir(elem)
    # list all files in there
    listOfFiles = os.listdir('.')
    # go through all dicom files in the subdirectory
    for file in listOfFiles:
        # exclude not-dicom files
        if file.endswith('.dcm'):
            # read file
            # file_names.add(file)

            f = str(file)

            ds = pydicom.dcmread(file)

            provenance = str(ds['PatientID'].value) + str(file)
            if f not in file_names:
                fnode = File(f)
                storedNodes['File'].append(fnode)
                file_names.add(file)

            try:
                sources.add(ds[0x013, 0x1010].value)
            except KeyError:
                pass
            # go through all sections and create the node if it's not already existing
            for element in sections:
                # make sure only one edge from patient to sex is created (sex does not change)
                # if patient already exists, then sex already exists
                sex_boolean = 0
                if ds['PatientID'].value in UIDs['Patient']:
                    sex_boolean = 1
                # boolean value: equal to 1 if date node and relation exist
                date_boolean = 0
                # find usage and sex for later:
                if parser[element]['Class'] == 'IOD Module':
                    temp_usage = parser[element]['Usage']
                temp_sex = ds['PatientSex'].value
                # find uid for the node that might be created
                # either it's a General Image node, then a uid is created by a Combination of the Series UID
                # and the Instance Number
                # also find the Node ID for later
                if parser[element]['Date'] != '':
                    tempDate = parser[element]['Date'].split(',')
                    # consider relation
                    # set boolean variable to 1 to indicate that a relation needs to be created
                    date_boolean = 1
                    # get the relation type (varies due to different date types)
                    date_relation_type = tempDate[1]
                    # get the date value
                    date_from_dicom = ds[tempDate[0]].value
                    # tempDate[0] = attribute of ds
                    # tempDate[1] = relation to the date node
                    tempTime = parser[element]['Time']
                    # get the time value
                    if tempTime != '':
                        time_from_dicom = ds[tempTime].value
                    else:
                        time_from_dicom = ''
                    date_time_id = str(date_from_dicom) + str(time_from_dicom)
                    if date_time_id not in dates:
                        # if not existing add id
                        dates.add(date_time_id)
                        # create dic
                        date_time_dic = {'ID': date_time_id, 'Date': date_from_dicom, 'Time': time_from_dicom}
                        # create date node
                        current_date_node = Date(date_time_dic)
                        # save date node
                        storedNodes['Date'].append(current_date_node)

                if element == 'General_Image':
                    uid = f"series{ds.SeriesInstanceUID}instance{ds.InstanceNumber}"

                # check which Attribute shall be used as the unique ID for the node
                elif parser[element]['uidfromds'] == '1':
                    uidToTest = parser[element]['uid'] # gets back what shall be used as an uid
                    if parser[element]['sequenceboolean'] == '0':
                        try:
                            uid = ds[uidToTest].value
                        except KeyError:
                            continue
                    else:
                        seq = parser[element]['sequence'].split(',')
                        # list needs integer value
                        try:
                            uid = ds[seq[0]][int(seq[1])][uidToTest].value
                        except KeyError:
                            continue
                else:
                    uid = parser[element]['uid']
                    # relationFrom = uid
                # check if uid already used (via uid dictionary)
                uid = str(uid)

                if uid not in UIDs[element]:
                    # write ID in dictionary (only possible if not already existing)
                    UIDs[element].add(uid)

                    attributesNeededForCurrentNode = parser[element]['attributes'].split(',')
                    # attach values to these attributes in a temporary dictionary
                    # create dictionary and add the ID to it
                    d = {
                        'ID': uid,
                         }
                    # then the rest of the attributes
                    if parser[element]['sequenceboolean'] == '0':
                        if attributesNeededForCurrentNode != ['']:
                            for attr in attributesNeededForCurrentNode:
                                #print(attributesNeededForCurrentNode)
                                d[attr] = ds[attr].value
                                #print(d)
                        # create Node and pass d to it
                    # if attribute is part of a sequence
                    else:
                        # get the sequence and save it in d
                        seq = parser[element]['sequence'].split(',')
                        d[parser[element]['attributes']] = ds[seq[0]][int(seq[1])][uidToTest].value
                    currentNode = Node(d)
                    # store node object for later printing to csv
                    storedNodes[element].append(currentNode)

                # create all edges and store them
                # get relation targets
                relationsTo = parser[element]['relationsto'].split(',')
                # create empty dictionary with UID for each node that shall be created from reading this file
                # here the uids for the edge creation are stored
                if relationsTo != ['']:
                    filewise_uid_dic = {}
                    for rT in relationsTo:
                        if rT == 'General_Image':
                            filewise_uid_dic[rT] = f"series{ds.SeriesInstanceUID}instance{ds.InstanceNumber}"
                        elif rT == 'Usage':
                            filewise_uid_dic[rT] = temp_usage
                        elif rT == 'Sex':
                            # skip as it's handled later
                            continue
                            #filewise_uid_dic[rT] = temp_sex
                        elif rT == 'Date And Time':
                            # skip as it's handled later
                            continue
                        elif parser[rT]['uidfromds'] == '1':
                            u = parser[rT]['uid']  # gets back what shall be used as an uid
                            if parser[rT]['sequenceboolean'] == '0':
                                try:
                                    filewise_uid_dic[rT] = ds[u].value
                                except KeyError:
                                    continue
                            else:
                                sequ = parser[rT]['sequence'].split(',')
                                # list needs integer value
                                try:
                                    filewise_uid_dic[rT] = ds[sequ[0]][int(sequ[1])][u].value
                                except KeyError:
                                    continue
                            # relationFrom = uid
                        else:
                            filewise_uid_dic[rT] = parser[rT]['uid']



                # get relation types
                relationTypes = parser[element]['relationtypes'].split(',')
                # create relation for each target and save it in list
                for i in range(len(relationsTo)):
                    try:
                        currentRel = Relation(uid, filewise_uid_dic[relationsTo[i]], relationTypes[i], provenance)
                        storedRelations[relationTypes[i]].append(currentRel)
                    except KeyError:
                        continue
                # create relations from IOD module to date and time (if necessary)
                if date_boolean == 1:
                    date_relation = Relation(uid, date_time_id, date_relation_type, provenance)
                    storedRelations[date_relation_type].append(date_relation)
                if sex_boolean == 0:
                    sex_relation = Relation(ds['PatientID'].value, ds['PatientSex'].value, 'hasSex', provenance)
                    storedRelations['hasSex'].append(sex_relation)

            # relation from file to patient
            file_rel = Relation(file, ds['PatientID'].value, 'belongsToPatient', provenance)
            file_relations.append(file_rel)
            # relation from source to file
            source_file_rel = Relation(ds[0x013,0x1010].value, file, 'hasFile', provenance)
            source_file_relations.append(source_file_rel)
            # relation from source to patient
            source_patient_rel = Relation(ds[0x013,0x1010].value, ds['PatientID'].value, 'hasPatient', provenance)
            source_patient_relations.append(source_patient_rel)

# ---------------------------------------------------------------------------------------------------------------------

# create source nodes
for srs in sources:
    new_source_node = Source(srs)
    all_sources.append(new_source_node)

# ---------------------------------------------------------------------------------------------------------------------

toc = time.time()

t = (toc-tic)/60

print(tic)
print(toc)
print(f"Import Zeit: {t} Minuten")

# ---------------------------------------------------------------------------------------------------------------------

# print nodes
def print_nodes(nodes_to_print, nodeType):
    # create new dir if necessary
    export_nodes_to = '/Users/tobias/Library/Mobile Documents/com~apple~CloudDocs/Documents/Uni/Masterarbeit/Datengrundlage/Cancer Imaging Archive/Import/nodes'

    if not os.path.exists(export_nodes_to):
        os.makedirs(export_nodes_to)

    os.chdir(export_nodes_to)

    # write file
    with open(f"{nodeType}.csv", 'w') as csvfile:
        csv_node_writer = csv.writer(csvfile)
        for n in nodes_to_print:
            csv_node_writer.writerow([n.__dict__[m] for m in n.__dict__])


def print_edges(edges_to_print, edgeType):

    # create new dir if necessary
    export_nodes_to = '/Users/tobias/Library/Mobile Documents/com~apple~CloudDocs/Documents/Uni/Masterarbeit/Datengrundlage/Cancer Imaging Archive/Import/relations'

    if not os.path.exists(export_nodes_to):
        os.makedirs(export_nodes_to)

    os.chdir(export_nodes_to)

    # write file
    with open(f"{edgeType}.csv", 'w') as csvfile:
        csv_edge_writer = csv.writer(csvfile)
        for e in edges_to_print:
            csv_edge_writer.writerow([e.start_id, e.end_id, e.rel_type, e.provenance])


# print header for nodes
def print_variable_node_headers(var_node):

    temp_dir = '/Users/tobias/Library/Mobile Documents/com~apple~CloudDocs/Documents/Uni/Masterarbeit/Datengrundlage/Cancer Imaging Archive/Import/CSVHeaderFiles/nodes'

    if not os.path.exists(temp_dir):
        os.makedirs(temp_dir)

    os.chdir(temp_dir)

    with open(f"{var_node}-header.csv", 'w') as csvfile:
        hd1 = [':ID']
        hd2 = parser[var_node]['attributes'].split(',')
        if hd2 == ['']:
            hd = hd1
        else:
            hd = hd1 + hd2
        csv_header_writer = csv.writer(csvfile)
        csv_header_writer.writerow(hd)


def print_static_node_headers():

    temp_dir = '/Users/tobias/Library/Mobile Documents/com~apple~CloudDocs/Documents/Uni/Masterarbeit/Datengrundlage/Cancer Imaging Archive/Import/CSVHeaderFiles/nodes'

    if not os.path.exists(temp_dir):
        os.makedirs(temp_dir)

    os.chdir(temp_dir)

    # source node
    with open('Source-header.csv', 'w') as csvfile:
        csv_header_writer = csv.writer(csvfile)
        csv_header_writer.writerow([':ID', 'Source'])

    # file node
    with open('File-header.csv', 'w') as csvfile:
        csv_header_writer = csv.writer(csvfile)
        csv_header_writer.writerow([':ID', 'File'])

    # sex node
    with open('Sex-header.csv', 'w') as csvfile:
        csv_header_writer = csv.writer(csvfile)
        csv_header_writer.writerow([':ID', 'Sex'])

    # sex node
    with open('Usage-header.csv', 'w') as csvfile:
        csv_header_writer = csv.writer(csvfile)
        csv_header_writer.writerow([':ID', 'Usage'])

    # Date And Time node
    with open('DateAndTime-header.csv', 'w') as csvfile:
        csv_header_writer = csv.writer(csvfile)
        csv_header_writer.writerow([':ID', 'Date', 'Time'])


def print_variable_edge_headers(edge_type):
    temp_dir = '/Users/tobias/Library/Mobile Documents/com~apple~CloudDocs/Documents/Uni/Masterarbeit/Datengrundlage/Cancer Imaging Archive/Import/CSVHeaderFiles/relations'

    if not os.path.exists(temp_dir):
        os.makedirs(temp_dir)

    os.chdir(temp_dir)

    with open(f"{edge_type}-header.csv", 'w') as csvfile:
        csv_header_writer = csv.writer(csvfile)
        csv_header_writer.writerow([':START_ID', ':END_ID', 'type', 'provenance'])



ticc = time.time()

for variable_node in sections:
    print_variable_node_headers(variable_node)

for r in allRelationTypes_with_static:
    print_variable_edge_headers(r)

print_static_node_headers()

# print nodes to csv
for s in sections:
    if s != 'Pixel Data':
        #print(s)
        print_nodes(storedNodes[s], s)

print_nodes(storedNodes['Date'], 'Date')
print_nodes(storedNodes['File'], 'File')
print_nodes(all_sources, 'Source')



# print relations to csv
for rt in storedRelations:
    print_edges(storedRelations[rt], rt)
print_edges(source_patient_relations, 'hasPatient')
print_edges(source_file_relations, 'hasFile')
print_edges(file_relations, 'belongsToPatient')


tocc = time.time()
tt = (tocc-ticc)/60
print(f"Export Zeit fuer Knoten: {tt} Minuten")

print('done')




# print header for relations

# print nodes
# print relations
