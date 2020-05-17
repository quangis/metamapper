import rdflib
import csv

from time import sleep

gtypes = {}
dtypes = {}
atypes = {}

g = rdflib.Graph()
g.parse("http://geographicknowledge.de/vocab/CoreConceptData.rdf#")
g.parse("./ontology.ttl", format="ttl")

sleep(.5)

results = g.query("""
    prefix skos: <http://www.w3.org/2004/02/skos/core#>
    prefix ccd: <http://geographicknowledge.de/vocab/CoreConceptData.rdf#>
    prefix dcat: <https://www.w3.org/TR/vocab-dcat#>
    prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>

    select ?dataset ?type
    where {
      ?dataset a dcat:Dataset , ?type .

      filter (
        ?type in ( ccd:PointDataSet, ccd:RegionDataSet, ccd:VectorTessellation, ccd:LineDataSet )
      )
    }
""")

for result in results:
    uri, geometry_type = result
    gtypes[str(uri)] = str(geometry_type).split('#')[1]

results = g.query("""
    prefix skos: <http://www.w3.org/2004/02/skos/core#>
    prefix ccd: <http://geographicknowledge.de/vocab/CoreConceptData.rdf#>
    prefix dcat: <https://www.w3.org/TR/vocab-dcat#>
    prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>

    select ?dataset ?type
    where {
      ?dataset a dcat:Dataset , ?type .
      ?type rdfs:subClassOf+ ccd:CoreConceptDataSet .
    }
""")

for result in results:
    uri, dtype = result
    dtypes[str(uri)] = str(dtype).split('#')[1]


results = g.query("""
    prefix skos: <http://www.w3.org/2004/02/skos/core#>
    prefix ccd: <http://geographicknowledge.de/vocab/CoreConceptData.rdf#>
    prefix ada: <http://geographicknowledge.de/vocab/AnalysisData.rdf>
    prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>

    select ?dataset ?label ?type
    where {
      ?attribute ada:ofDataSet ?dataset ;
                     skos:exactMatch ?concept ;
                     rdfs:label ?label .

      optional {
        ?concept a ?type .
        ?type rdfs:subClassOf+ ccd:Attribute .
      }
    }
    group by ?dataset ?label ?type
""")

for result in results:
    dataset, label, atype = result
    key = (str(dataset), str(label))
    if atype is None and key not in atypes:
        atypes[key] = ""
    elif atype is not None:
        atypes[key] = str(atype).split('#')[1]


test_gtypes = {}
test_dtypes = {}
test_atypes = {}

with open("./datasets/annotations_datasets.csv", 'r') as fin:
    reader = csv.reader(fin)
    next(reader)
    for row in reader:
        test_gtypes[row[0]] = row[1]
        test_dtypes[row[0]] = row[2]

with open("./datasets/annotations_attributes.csv", 'r') as fin:
    reader = csv.reader(fin)
    next(reader)
    for row in reader:
        test_atypes[(row[0],row[1])] = row[2]

tp = 0
total = 0
fn = len(test_gtypes)
for k, v in gtypes.items():
    if k not in test_gtypes: # skip some extra test datasets
        continue
    total += 1

    if test_gtypes[k] == v:
        tp += 1
        fn -= 1

p = tp / total
r = tp / (tp + fn)
f = 2 *  ((p * r) / (p + r))

print("Geometry type scores:")
print(f"P: {p} , R: {r} , F: {f}")


tp = 0
total = 0
fn = len(test_dtypes)
for k, v in dtypes.items():
    if k not in test_dtypes:
        continue
    total += 1

    if test_dtypes[k] == v:
        tp += 1
        fn -= 1

p = tp / total
r = tp / (tp + fn)
f = 2 *  ((p * r) / (p + r))

print("Dataset type scores:")
print(f"P: {p} , R: {r} , F: {f}")


filter_nontypes = True
if filter_nontypes:
    test_atypes = {k: v for k, v in test_atypes.items() if v != ""}
    atypes = {k: v for k, v in atypes.items() if v != ""}

tp = 0
total = 0
fn = len(list(filter(lambda x: x != "", test_atypes.values())))
for k, v in atypes.items():
    if k not in test_atypes:
        continue

    if v != "":
        total += 1

    if test_atypes[k] == v:
        tp += 1
        fn -= 1
    elif v == "BooleanA" and test_atypes[k] == "NominalA": # boolean is "more" correct
        tp += 1
        fn -= 1
    else:
        print(k, v, test_atypes[k])

p = tp / total
r = tp / (tp + fn)
f = 2 *  ((p * r) / (p + r))

print("Attribute type scores:")
print(f"P: {p} , R: {r} , F: {f}")


