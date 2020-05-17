import hashlib
import psycopg2
import uuid
import urllib.parse

from psycopg2 import sql

GUESS = False

def ccd_mapper(data_type, concept=None, broader=None):
    """
    Naively guess the attribute type
    """

    mapper = {
        "double precision": "IntervalA",
        "bigint": "IntervalA",
        "integer": "IntervalA"
    }

    if data_type in mapper:
        return mapper.get(data_type)

    with psycopg2.connect("host=localhost") as conn:
        with conn.cursor() as cur:

            if broader:
                cur.execute("select narrower from concepts where uri = %s", [ broader ])
                concept = cur.fetchone()[0]

            cur.execute("select count(distinct value), count(*) from concepts__data where uri = %s", [ concept ])
            unique, total = cur.fetchone()

            is_categorical = unique < 20 and unique != total
            is_bool = unique == 2

    if is_bool:
        return "BooleanA"
    elif is_categorical: # Cannot known ordinal
        return "NominalA"


def ds_mapper(conn, source):
    print(source)

    with conn.cursor() as cur:
        cur.execute("create table if not exists datasets (table_name text primary key, gtype text, dtype text);")
        cur.execute("select gtype, dtype from datasets where table_name = %s", [ source ])
        res = cur.fetchone()
        if res:
            return res

        cur.execute(sql.SQL("select geometrytype(geom) from {} where geom is not null limit 1").format(sql.Identifier(source)))
        geom_type, = cur.fetchone()


        # Rule out EventDS and TrackDS. We cannot do the inverse and ensure an EventDS or TrackDS because a temporal column
        # may just be that, and not relate to an object in time.
        cur.execute("select distinct data_type::text from information_schema.columns where table_name = %s", [ source ])
        res = cur.fetchall()
        is_time = False
        for row in res:
            if row[0] in ("timestamp", "date"):
                is_time = True
                break


        ## Test for field datasets

        # Currently no support for raster
        is_raster = False

        # Next, check if we can detect a CoverageDS. If each polygon has the same dimensions it is probalby vector tessellation.
        # We cannot do the same for a PatchDS because it has an irregular shape and may actually be an ObjectDS
        if geom_type in ("POLYGON", "MULTIPOLYGON"):
            cur.execute(sql.SQL("select distinct abs((st_xmax(geom) - st_xmin(geom)) - avg(st_xmax(geom) - st_xmin(geom)) over ()) <= 1E-6 from {}").format(sql.Identifier(source)))
            res = cur.fetchall()
            equal_width = len(res) == 1 and res[0][0]

            cur.execute(sql.SQL("select distinct abs((st_ymax(geom) - st_ymin(geom)) - avg(st_ymax(geom) - st_ymin(geom)) over ()) <= 1E-6 from {}").format(sql.Identifier(source)))
            res = cur.fetchall()
            equal_len = len(res) == 1 and res[0][0]

            is_coverage = equal_width and equal_len
        else:
            is_coverage = False

        ## Test for objects

        # Test if the geometries resemble an object in space. We test each geometry against a database of known places, such as amenities or administrative
        # regions. The test is only as good as the coverage of this database, but as soon as we have a match we can be pretty sure it is an object.
        # This means that the number of false positives is probably quite low, but we cannot (ever) know if we have a false negative.
        cur.execute(sql.SQL("""
            select count(*) / (select count(*)::float8 from {source} limit 100) as n
            from (
                select distinct on (b.geom) a.geom as ageom, b.geom as bgeom
                from places a
                join {source} b on (st_buffer(a.geom, 15E-5) && b.geom and st_geometrytype(st_multi(a.geom)) = st_geometrytype(st_multi(b.geom)))
                where b.ctid in (select ctid from {source} limit 100)
                order by b.geom, st_hausdorffdistance(a.geom, b.geom)
            ) x
            where st_hausdorffdistance(st_transform(st_setsrid(ageom, 4326), 28992), st_transform(st_setsrid(bgeom, 4326), 28992)) < 15;
        """).format(**{
            "source": sql.Identifier(source)
        }))
        res = cur.fetchone()[0]

        # Because the places database is not complete by a longshot, this rarely returns a full match. However, the opposite is also true
        # and a random spread (continuous data) of points or (especially) polygons generally hovers closer to zero because the Hausdorff algorithm
        # is quite brutal when even just one vertex is off.
        is_object = res >= 0.2

        # An object may still be a lattice if it covers the entire extent. We cannot just get the extent directly however because not all shapes
        # are rectangular. Instead we need to compare the areas of the individual shapes so that we can detect possible gaps.
        if is_object:
            cur.execute(sql.SQL("""
                select sum(st_area(a.geom)) / nullif(st_area(b.geom), 0)
                from {source} a
                join (
                    select st_concavehull((st_dump(geom)).geom, 0.9) as geom
                    from (
                        select st_union(geom) as geom
                        from {source}
                    ) _
                ) b on (st_intersects(a.geom, b.geom))
                group by b.geom
            """).format(**{
                "source": sql.Identifier(source)
            }))
            res = cur.fetchall()
            is_lattice = len(list(filter(lambda x: x[0] and abs(1 - x[0]) <= 0.01, res))) == len(res)
        else:
            is_lattice = False


        if is_raster:
            resp = ("Raster", "FieldRasterDS")

        elif is_coverage:
            resp = ("VectorTessellation", "CoverageDS")

        elif is_lattice:
            resp = ("VectorTessellation", "LatticeDS")

        elif is_object:
            if geom_type == "POINT":
                resp = ("PointDataSet", "ObjectDS")
            elif geom_type == "LINESTRING":
                resp = ("LineDataSet", "ObjectDS")
            else:
                resp = ("RegionDataSet", "ObjectDS")

        # We have not yet returned, anything beyond here is a guess
        elif GUESS and geom_type == "LINESTRING":
            resp = ("LineDataSet", "NetworkDS")
        elif GUESS and is_time and geom_type == "POINT":
            resp = ("PointDataSet", "EventDS")
        elif GUESS and is_time and geom_type == "LINESTRING":
                resp = ("LineDataSet", "TrackDS")

        elif geom_type == "POINT":
            resp = ("PointDataSet", None)
        elif geom_type == "LINESTRING":
            resp = ("LineDataSet", None)
        else:
            resp = ("RegionDataSet", None)

        cur.execute("insert into datasets values (%s, %s, %s) on conflict do nothing", [ source, resp[0], resp[1] ])
        return resp


def export():
    publishers = []
    datasets = {}
    dataset_types = {}
    concepts = {}


    with psycopg2.connect("host=localhost") as conn:
        conn.autocommit = True

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

                dataset_types[publisher["source"]] = ds_mapper(conn, publisher["source"])

            cur.execute("select uri, name, data_type, narrower, measurement, property, dataset from concepts where narrower is not null")
            for row in cur.fetchall():
                uri, name, data_type, narrower, measurement, property_type, dataset = row
                concepts[narrower] = {
                    "uri": uri,
                    "name": name,
                    "data_type": data_type,
                    "narrower": narrower,
                    "measurement": measurement,
                    "property": property_type,
                    "dataset": dataset
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
        subclassof = ["skos:Concept"]
        uri = concept["uri"]
        name = concept["name"]
        data_type = ccd_mapper(concept["data_type"], broader=uri)

        measurement = concept["measurement"]
        if measurement:
            data_type = f"{measurement}A"

        if data_type:
            subclassof.append(f"ccd:{data_type}")

        property_type = concept["property"]
        if property_type and data_type in ("RatioA", "IntervalA"):
            subclassof.append("exm:ERA" if property_type == "Extensive" else "exm:IRA")
        elif GUESS and data_type in ("RatioA", "IntervalA"):
            subclassof.append("exm:ERA")

        ttl += f"\n<{uri}>\n  a {', '.join(subclassof)} ;\n"


        ttl += f"""  skos:prefLabel "{name}" .\n"""

    ttl += "\n\n########## GENERATED CONCEPTS ##########\n"

    for attributes in datasets.values():
        for k, attribute in attributes.items():
            subclassof = ["skos:Concept"]
            concept_uri = attribute.get("concept_uri")
            data_type = ccd_mapper(attribute["data_type"], concept=concept_uri)
            if data_type:
                subclassof.append(f"ccd:{data_type}")
            if GUESS and data_type in ("RatioA", "IntervalA"):
                subclassof.append("exm:ERA")
            if concept_uri in concepts:
                ttl += f"\n<{concept_uri}>\n  a skos:Concept ;\n"
                ttl += f"  skos:broader <{concepts[concept_uri]['uri']}> .\n"
            else:
                ttl += f"\n<{concept_uri}>\n  a {', '.join(subclassof)} .\n"

    ttl += "\n\n########## PUBLISHERS ##########\n"

    access_urls = {}
    for publisher in publishers:
        source = publisher["source"]
        access_url = publisher["access_url"]
        download_url = urllib.parse.quote(publisher["download_url"])
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
        gtype, dtype = dataset_types[source]

        # Check if we have an annotated dataset
        for attribute in attributes.values():
            concept_uri = attribute.get("concept_uri")

            dataset_type = ""
            if concept_uri in concepts:
                dataset = concepts[concept_uri].get("dataset")
                dataset_type = f" ccd:{dataset},"
                break

        if dataset_type == "" and dtype is not None:
            dataset_type = f" ccd:{dtype},"

        ttl += f"""
<{access_url}>
  a ccd:{gtype},{dataset_type} dcat:Dataset ;
  dcat:distribution _:{source}_distribution .
"""
        for attribute in attributes.values():
            column_name = attribute["column_name"]
            concept_uri = attribute.get("concept_uri")

            ttl += f"""
_:{source}_{column_name.replace(" ", "_")}
  ada:ofDataSet <{access_url}> ;
"""
            if concept_uri:
                ttl += f"  skos:exactMatch <{concept_uri}> ;\n"

            ttl += f"""  rdfs:label "{column_name}" .\n"""

    with open("ontology.ttl", "w") as fout:
        fout.write(ttl)


if __name__ == "__main__":
    export()
