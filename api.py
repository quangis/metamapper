import psycopg2

from flask import Flask, request, jsonify
from flask_cors import CORS, cross_origin
from psycopg2 import sql

from ingest import ingest
from annotate import Annotate
from extract import WebDriver

app = Flask(__name__)
cors = CORS(app)

annotate = Annotate()
webdriver = WebDriver()
webdriver.visit("http://localhost:3000")


def setup():
    with psycopg2.connect("host=localhost") as conn:
        with conn.cursor() as cur:
            cur.execute("""
                create table if not exists metamapper (
                    uri text,
                    field text,
                    xpath text,
                    value text,

                    primary key (uri, field)
                );
            """)


@app.route('/sample', methods=['GET'])
def sample():
    source = request.args.get("source")

    data = []
    with psycopg2.connect("host=localhost") as conn:
        with conn.cursor() as cur:
            cur.execute(sql.SQL("select * from {} limit 10").format(sql.Identifier(source)))
            columns = [d[0] for d in cur.description]
            realdata = [dict(zip(columns, row)) for row in cur.fetchall()]

            for row in realdata: # drop geoms
                if "geom" in row:
                    del row["geom"]
                    data.append(row)

    return jsonify(data)


@app.route('/ingest', methods=['GET'])
def trigger_ingest():
    url = request.args.get("url")
    ingest(url)

    return jsonify({"status": "ok"})

@app.route('/suggest', methods=['GET'])
def suggest():
    source = request.args.get("source")
    field = request.args.get("field")

    # Table names are hashed uris
    #source = hashlib.md5(url.encode("utf-8")).hexdigest()

    suggestion = annotate.suggest_concept(source, field)
    return jsonify({"concept": suggestion})


@app.route('/concepts', methods=['GET', 'POST'])
def concepts():
    if request.method == 'POST':
        return post_concept(request.json)
    else:
        return get_concepts()


def post_concept(data):
    source = data.get("source")
    field = data.get("field")
    concept = data.get("concept")

    if not source or not field or not concept:
        raise Exception("Missing required params")

    #source = hashlib.md5(url.encode("utf-8")).hexdigest()

    annotate.generate_concept(source, field, concept, verified=True)
    return jsonify({"status": "ok"})


def get_concepts():
    resp = []

    with psycopg2.connect("host=localhost") as conn:
        with conn.cursor() as cur:
            cur.execute("select uri, name from concepts")
            res = cur.fetchall()

            for row in res:
                resp.append({"uri": row[0], "name": row[1]})

    return jsonify(resp)


@app.route('/extract', methods=['GET'])
def extract():
    url = request.args.get("url")
    results = webdriver.extract(url)

    with psycopg2.connect("host=localhost") as conn:
        with conn.cursor() as cur:
            for k, v in results.items():
                cur.execute("""
                    insert into metamapper (uri, field, xpath, value) values
                        (%(uri)s, %(field)s, %(xpath)s, %(value)s)
                    on conflict on constraint metamapper_pkey do update set
                        xpath = excluded.xpath,
                        value = excluded.value;
                """, {
                    "uri": url,
                    "field": k,
                    "xpath": v[0],
                    "value": v[1]
                })

    return get_publisher(url)


@app.route('/publishers', methods=['GET', 'POST', 'DELETE'])
def publishers():
    if request.method == 'POST':
        return post_publisher(request.json)
    elif request.method == 'DELETE':
        return delete_publisher(request.args.get("url"))
    else:
        return get_publisher(request.args.get("url"))


def delete_publisher(url):
    with psycopg2.connect("host=localhost") as conn:
        with conn.cursor() as cur:
            cur.execute("""
                delete from metamapper
                where uri = %(uri)s
            """, { "uri": url });

    return jsonify({"status": "ok"})

def get_publisher(url):
    resp = []

    with psycopg2.connect("host=localhost") as conn:
        with conn.cursor() as cur:
            cur.execute("""
                select uri, array_agg(json_build_object(field, value))
                from metamapper
                where %(filter)s or uri = %(uri)s
                group by uri
            """, { "filter": url is None, "uri": url })

            res = cur.fetchall()
            for row in res:
                resp.append({k: v for a in row[1] for k, v in a.items()})

    return jsonify(resp)

def post_publisher(data):
    url = data.get("access_url")
    if not url:
        raise Exception("Access url is required")


    with psycopg2.connect("host=localhost") as conn:
        with conn.cursor() as cur:
            cur.execute("""
                insert into metamapper (uri, field, value) values
                    (%(uri)s, %(field)s, %(value)s)
                on conflict on constraint metamapper_pkey do update
                    set value = excluded.value;
            """, {
                "uri": url,
                "field": "access_url",
                "value": url
            })

    return jsonify({"status": "ok"})


setup()
app.run(host="localhost", port=8000, debug=False)


