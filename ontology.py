import hashlib
import psycopg2
import uuid

from psycopg2 import sql


def ccd_mapper(data_type):
    """
    Guess the attribute type

    TODO: pass in data to correctly distinguish between the most common ccd attributes
    """

    mapper = {
        "text": "NominalA",
        "double_precision": "IntervalA",
        "integer": "IntervalA"
    }

    return mapper.get(data_type)

def export():
    publishers = []
    datasets = {}
    dataset_types = {}
    concepts = {}


    with psycopg2.connect("host=localhost") as conn:
        with conn.cursor() as cur:
            cur.execute("select uri, value as download_url from metamapper where field = 'download_url'")
            for row in cur.fetchall():
                access_url, download_url = row
                source = hashlib.md5(download_url.encode("utf-8")).hexdigest()

                publishers.append({
                    "access_url": access_url,
                    "download_url": download_url,
                    "source": source
                })

            for publisher in publishers:
                attributes = {}

                cur.execute("select column_name, data_type::text from information_schema.columns where table_name = %s and column_name != 'geom'", [ publisher["source"] ])
                for row in cur.fetchall():
                    column_name, data_type = row
                    attributes[column_name] = {
                        "column_name": column_name,
                        "data_type": data_type
                    }

                cur.execute("select column_name, uri from concepts__data where table_name = %s group by uri, column_name", [ publisher["source"] ])
                for row in cur.fetchall():
                    column_name, uri = row
                    attributes[column_name] = {**attributes[column_name], **{ "concept_uri": uri }}

                datasets[publisher["source"]] = attributes

                cur.execute(sql.SQL("select geometrytype(geom) from {} where geom is not null limit 1").format(sql.Identifier(publisher["source"])))
                geom_type, = cur.fetchone()

                dataset_types[publisher["source"]] = geom_type

            cur.execute("select uri, name, data_type from concepts")
            for row in cur.fetchall():
                uri, name, data_type = row
                concepts[uri] = {
                    "uri": uri,
                    "name": name,
                    "data_type": data_type
                }


    ttl = """
@prefix dc: <http://purl.org/dc/elements/1.1/> .
@prefix dct: <http://purl.org/dc/terms/> .
@prefix geo: <http://www.opengis.net/ont/geosparql#> .
@prefix owl: <http://www.w3.org/2002/07/owl#>.
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>.
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>.
@prefix xsd: <http://www.w3.org/2001/XMLSchema#>.
@prefix skos: <http://www.w3.org/2004/02/skos/core#> .

# Data types ontology
@prefix exm: <http://geographicknowledge.de/vocab/ExtensiveMeasures.rdf#> .
@prefix ccd: <http://geographicknowledge.de/vocab/CoreConceptData.rdf#> .
@prefix ada: <http://geographicknowledge.de/vocab/AnalysisData.rdf> .

# Data quality vocab
@prefix dqv: <https://www.w3.org/TR/vocab-dqv/> .
@prefix dcat: <https://www.w3.org/TR/vocab-dcat#> .


########## CONCEPTS ##########
"""

    for concept in concepts.values():
        uri = concept["uri"]
        name = concept["name"]
        data_type = ccd_mapper(concept["data_type"])

        ttl += f"""
<{uri}>
  a skos:Concept, ccd:{data_type} ;
  skos:prefLabel "{name}" .
"""

    ttl += "\n\n########## GENERATED CONCEPTS ##########\n"

    for attributes in datasets.values():
        for k, attribute in attributes.items():
            concept_uri = attribute.get("concept_uri")

            if not concept_uri:
                uri = "http://example.com/" + uuid.uuid4().hex
                data_type = ccd_mapper(attribute["data_type"])

                attributes[k] = {**attribute, **{ "concept_uri": uri }}
                ttl += f"\n<{uri}>\n  a skos:Concept, ccd:{data_type} .\n"

    ttl += "\n\n########## PUBLISHERS ##########\n"

    access_urls = {}
    for publisher in publishers:
        source = publisher["source"]
        access_url = publisher["access_url"]
        download_url = publisher["download_url"]
        access_urls[source] = access_url

        ttl += f"""
_:{source}_distribution
  a dcat:Distribution ;
  dcat:accessURL <{access_url}> ;
  dcat:downloadURL <{download_url}> ;
  dct:title "" .
"""

    ttl += "\n\n########## DATASETS ##########\n"

    for source, attributes in datasets.items():
        access_url = access_urls[source]
        dtype = 'PointDataSet' if dataset_types[source] == 'POINT' else 'RegionDataSet'

        ttl += f"""
<{access_url}>
  a ccd:{dtype}, dcat:Dataset ;
  dcat:distribution _:{source}_distribution .
"""
        for attribute in attributes.values():
            column_name = attribute["column_name"].replace(" ", "_")
            concept_uri = attribute.get("concept_uri")

            ttl += f"""
_:{column_name}
  a ccd:{data_type} ;
  ada:ofDataSet <{access_url}> ;
"""
            if concept_uri:
                ttl += f"  skos:exactMatch <{concept_uri}> ;\n"

            ttl += f"""  rdfs:label "{column_name}" .\n"""

    with open("ontology.ttl", "w") as fout:
        fout.write(ttl)


if __name__ == "__main__":
    export()
