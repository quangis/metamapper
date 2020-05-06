import csv
import io
import gzip
import hashlib
import magic
import os
import osgeo
import psycopg2
import tempfile
import tqdm
import urllib.request
import zipfile

from ast import literal_eval
from contextlib import suppress
from dateutil import parser
from osgeo import ogr
from psycopg2 import sql


def uncompress(file_path, dname):
    """
    Uncompress archive and return path to main content
    """

    supported_types = [ "Shapefile", "CSV" ]

    if not os.path.isdir(dname):
        with zipfile.ZipFile(file_path, 'r') as zout:
            zout.extractall(dname)

    filepaths = {}
    for root, dirs, files in os.walk(dname):
        for f in files:
            path = os.path.join(dname, root, f)
            filepaths[path] = magic.from_file(path)

    for f, t in filepaths.items():
        for supported_type in supported_types:
            if supported_type in t:
                return f, supported_type


def import_ogr(path, source):
    datasource = osgeo.ogr.Open(path)
    layer = datasource.GetLayer(0)
    layer_defn = layer.GetLayerDefn()
    feature = layer.GetNextFeature()

    type_mapping = {
        0: "int",
        1: "int[]",
        2: "float8",
        3: "float8[]",
        4: "text",
        5: "text[]",
        6: "text",
        7: "text[]",
        8: "bytea",
        9: "date",
        10: "time",
        11: "timestamp",
        12: "bigint",
        13: "bigint[]"
    }

    header = [layer_defn.GetFieldDefn(i).GetName() for i in range(0, feature.GetFieldCount())]
    datatypes = [type_mapping.get(layer_defn.GetFieldDefn(i).GetType(), "text") for i in range(0, feature.GetFieldCount())]

    with psycopg2.connect("host=localhost") as conn:
        with conn.cursor() as cur:
            cur.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier(source)))
            cur.execute(sql.SQL("CREATE TABLE {} (" + ','.join(["{} %s" % dtype for dtype in datatypes]) + ", geom geometry)").format(
                sql.Identifier(source),
                *[sql.Identifier(_) for _ in header]))

            nr_rows = layer.GetFeatureCount()
            print(f"Inserting {nr_rows} rows into \"{source}\"")

            for i in tqdm.tqdm(range(0, nr_rows)):
                feature = layer.GetFeature(i)
                if not feature:
                    continue

                fields = {}
                for j in range(0, feature.GetFieldCount()):
                    name = layer_defn.GetFieldDefn(j).GetName()
                    value = feature.GetField(j)

                    fields[name] = value

                geom = feature.GetGeometryRef().ExportToWkt()

                # Not very fast, but easier than bulk inserts
                cur.execute(sql.SQL("INSERT INTO {} VALUES (" + ','.join(("%s",) * (len(header)+1)) + ")").format(
                    sql.Identifier(source)
                ), list(fields.values()) + [geom])


def detect_column_type(data):
    types = set()

    for value in data:
        with suppress(ValueError, SyntaxError):
            types.add(type(literal_eval(value.strip())).__name__)

    if len(types) == 1:
        return types.pop()

    # If it's not numeric, it may still be a date
    with suppress(parser._parser.ParserError):
        precision = 0
        for value in data:
            pvalue = parser.parse(value)
            precision += pvalue.hour + pvalue.minute + pvalue.second

        return 'timestamp' if precision else 'date'

    return 'text'


def import_csv(path, source):
    header = []
    datatypes = []

    sample_size = 10
    with open(path, 'r') as fhandle:
        # Find a sensible offset
        offset = sum([len(line) for line in fhandle.readlines(sample_size)])
        fhandle.seek(0)

        # Sniff out the file format
        dialect = csv.Sniffer().sniff(fhandle.read(offset))
        fhandle.seek(0)

        # Grab the first line (our header)
        header = fhandle.readline().rstrip().split(dialect.delimiter)
        offset = fhandle.tell()

        reader = csv.reader(fhandle, dialect=dialect)

        # Grab a sample to detect the datatypes
        data = [row for i, row in enumerate(reader) if i <= sample_size]
        for i in range(0, len(data[0])):
            datatypes.append(detect_column_type([row[i] for row in data]))

        fhandle.seek(0)
        with psycopg2.connect("host=localhost") as conn:
            with conn.cursor() as cur:
                cur.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier(source)))
                cur.execute(sql.SQL("CREATE TABLE {} (" + ','.join(["{} %s" % dtype for dtype in datatypes]) + ")").format(
                    sql.Identifier(source),
                    *[sql.Identifier(_) for _ in header]))

                # Stream the contents through stdin
                cur.copy_expert(sql.SQL("COPY {} FROM STDIN CSV HEADER DELIMITER {} QUOTE {}").format(
                    sql.Identifier(source),
                    sql.Literal(dialect.delimiter),
                    sql.Literal(dialect.quotechar)
                ), fhandle)


def ingest(url = "https://cmshare.eea.europa.eu/s/n5L8Lrs9aYD775S/download"):
    tmpdir = tempfile.gettempdir()
    source = hashlib.md5(url.encode("utf-8")).hexdigest()
    source_file = os.path.join(tmpdir, source)

    if not os.path.isfile(source_file):
        with open(os.path.join(tmpdir, source), "wb") as fout:
            response = urllib.request.urlopen(url)
            if response.info().get("Content-Encoding") == "gzip":
                buf = io.BytesIO(response.read())
                data = gzip.GzipFile(fileobj=buf).read()
            else:
                data = response.read()
            fout.write(data)

    magic_file_type = magic.from_file(source_file)
    if "Zip" in magic_file_type or "zip" in magic_file_type:
        dname = os.path.join(tmpdir, hashlib.md5(source_file.encode("utf-8")).hexdigest())
        path, file_type = uncompress(source_file, dname=dname)

    elif "CSV" in magic_file_type or "text" in magic_file_type:
        path, file_type = (source_file, "CSV")

    elif "JSON" in magic_file_type:
        path, file_type = (source_file, "OGR")

    else: # Fallback using file extensions
        ext = os.path.splitext(source_file)[1]

        if ext in (".shp", ".json", ".geojson"):
            path, file_type = (source_file, "OGR")

        raise ValueError(f"Unknown file type: \"{magic_file_type}\"")


    if file_type == "CSV":
        import_csv(path, source)
    else: # handles most types
        import_ogr(path, source)

    return source


if __name__ == "__main__":
    table = ingest(url = "https://opendata.cbs.nl/CsvDownload/csv/71227ned/UntypedDataSet?dl=1A066")

