import psycopg2
import re
import statistics
import uuid

from collections import defaultdict
from functools import partial
from psycopg2 import sql
from psycopg2.extras import Json
from tdda import rexpy

import numpy as np
from sklearn.pipeline import Pipeline
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.naive_bayes import MultinomialNB
from scipy import stats

psycopg2.extensions.register_type(psycopg2.extensions.new_type(psycopg2.extensions.DECIMAL.values, 'DEC2FLOAT', lambda value, curs: float(value) if value is not None else None))


BASE_URI = "http://example.com/%s"

class Annotate:
    def __init__(self):
        self.conn = None
        self.text_clf = None
        self.categories = None
        self.numeric_data = None

        if not self.conn:
            self.conn = psycopg2.connect("host=localhost")
            self.conn.autocommit = True

        self.setup()

    def setup(self):
        with self.conn.cursor() as cur:
            cur.execute("""
                create table if not exists concepts (
                    uri text primary key,
                    name text,
                    data_type text,
                    verified bool default false,
                    narrower text references concepts(uri) on update cascade on delete cascade
                );

                create table if not exists concepts__data (
                    uri text references concepts on update cascade on delete cascade,
                    table_name text,
                    column_name text,
                    value text
                );
                create index if not exists "concepts__data_uri_idx" on concepts__data (uri);
            """)

        self.generate_all_rules()


    def generate_all_rules(self):
        self.generate_numeric_rules()
        self.generate_date_rules()
        self.generate_text_rules()


    def generate_numeric_rules(self):
        data_types = ("integer", "double precision")
        self.numeric_data = defaultdict(list)

        with self.conn.cursor() as cur:
            cur.execute(f"select b.value::float8, a.uri from concepts a join concepts__data b on a.uri = b.uri where a.narrower is null and a.data_type in %s and b.value is not null", [ data_types ])
            for v, k in cur.fetchall(): self.numeric_data[k].append(v)


    def generate_date_rules(self):
        pass

    def generate_text_rules(self):
        """
        Train a text categorization model

        Grabs all the known concepts and their instances and passes it
        into a very simple sklearn pipeline.

        When the number of categories is still small, we pull some demo
        data to create a dummy category. The model tends to always recommend
        a category, which means that bootstrapping the pipeline is impossible
        without a dummy (e.g. n=1 categories would always return the only
        available category because it does not know better).
        """

        with self.conn.cursor() as cur:
            cur.execute(f"select b.value, a.uri from concepts a join concepts__data b on a.uri = b.uri where a.narrower is null and a.data_type = 'text' and b.value is not null")

            if not cur.rowcount >= 1:
                return

            train = []
            all_categories = []
            for row in cur.fetchall():
                train.append(row[0])
                all_categories.append(row[1])

            self.categories = list(set(all_categories))
            target = list(map(lambda x: self.categories.index(x), all_categories))

        # When the list of categories is still small we add a dummy category
        if len(self.categories) < 10:
            from sklearn.datasets import fetch_20newsgroups

            train_dummy = set()
            news = fetch_20newsgroups(subset='train', shuffle=True).data
            for message in news:
                for line in message.split("\n"):
                    for word in line.split(" "):
                        train_dummy.add(word)

            train.extend(list(train_dummy))
            self.categories.append('dummy')
            target.extend(list((self.categories.index('dummy'),) * len(train_dummy)))

        self.text_clf = Pipeline([
            ('vect', CountVectorizer()),
            ('tfidf', TfidfTransformer()),
            ('clf', MultinomialNB()),
        ])

        self.text_clf.fit(train, target)


    def refresh_concept_data(self, uri, data_type, table_name, column_name):
        with self.conn.cursor() as cur:
            cur.execute("select 1 from concepts__data where table_name = %s and column_name = %s limit 1", [ table_name, column_name ])

            if cur.rowcount == 1:
                return

            cur.execute(sql.SQL("insert into concepts__data select %s, %s, %s, {} from {}").format(
                sql.Identifier(column_name),
                sql.Identifier(table_name)
            ), [
                uri,
                table_name,
                column_name
            ])

            # Refresh the appropiate rules when new data is added
            if data_type in ("integer", "double precision"):
                self.generate_numeric_rules()
            elif data_type in ("date", "timestamp"):
                self.generate_date_rules()
            else:
                self.generate_text_rules()


    def auto_generate_concept(self, table_name, column_name):
        concept_name = uuid.uuid4().hex
        uri = BASE_URI % concept_name

        with self.conn.cursor() as cur:
            cur.execute("select data_type::text from information_schema.columns where table_name = %s and column_name = %s", [ table_name, column_name ])
            data_type, = cur.fetchone()

            cur.execute("insert into concepts (uri, name, data_type) values (%s, %s, %s)", [ uri, concept_name, data_type ])

        return uri


    def generate_concept(self, table_name, column_name, concept_name, narrower, verified=False):
        uri = BASE_URI % concept_name

        with self.conn.cursor() as cur:
            cur.execute("select data_type::text from information_schema.columns where table_name = %s and column_name = %s", [ table_name, column_name ])
            data_type, = cur.fetchone()

            cur.execute("insert into concepts (uri, name, data_type, verified, narrower) values (%s, %s, %s, %s, %s)", [ uri, concept_name, data_type, verified, narrower ])


    def test_numeric_rules(self, data, a=0.05):
        candidates = {}
        for k, v in self.numeric_data.items():
            equal_var = stats.levene(v, data).pvalue > a
            t = stats.ttest_ind(v, data, equal_var=equal_var).pvalue

            if t > a:
                candidates[k] = t

        # Sort the candidates, even though it does not mean much
        candidates = [k for k, v in sorted(candidates.items(), key=lambda x: x[1], reverse=True)]

        return candidates

    def test_date_rules(self):
        return []


    def test_text_rules(self, data, min_score=0.5):
        if not self.text_clf:
            return []

        best_candidates = []
        candidates = defaultdict(int)

        results = self.text_clf.predict_proba(data)

        # Only pick categories that got a good score
        for result in results:
            for i, candidate in enumerate(result):
                if candidate >= min_score:
                    candidates[self.categories[i]] += 1

        # Filter candidates for consistency
        for k, v in candidates.items():
            if v / len(data) >= min_score and k != 'dummy':
                best_candidates.append(k)

        return best_candidates


    def test_header(self, col1, col2):
        simplify = lambda x: x \
            .replace(" ", "") \
            .replace("_", "") \
            .replace("-", "") \
            .lower()

        return simplify(col1) == simplify(col2)


    def suggest_concept(self, table_name, column_name, compare_headers=False, autogenerate=True):
        """
        Find a suitable concept for the given column

        1. If the table / column combo already has a concept, we can return immediately
        2. Retrieve the data type and use the corresponding test to identify a shortlist
        of candidates. These candidates will consist of generated concepts and do not imply
        semantic similarity, but will help with annotating the right data attributes.
        3. If no candidates were found, a new concept is generated instead.
        """

        with self.conn.cursor() as cur:
            cur.execute("select uri from concepts__data where table_name = %s and column_name = %s limit 1", [ table_name, column_name ])
            res = cur.fetchone()

            # This table / column combo already has a concept
            if res is not None and res[0] is not None:
                return res[0]


        with self.conn.cursor() as cur:
            cur.execute("select data_type::text from information_schema.columns where table_name = %s and column_name = %s", [ table_name, column_name ])
            data_type, = cur.fetchone()

            cur.execute(sql.SQL(f"select {{}}::{data_type} from {{}} where {{}} is not null").format(
                sql.Identifier(column_name),
                sql.Identifier(table_name),
                sql.Identifier(column_name)
            ))
            data = [row[0] for row in cur.fetchall()]


        # Get a list of potential concepts based on the data
        if data_type in ("integer", "double precision"):
            candidates = self.test_numeric_rules(data)
        elif data_type in ("date", "timestamp"):
            candidates = self.test_date_rules(data)
        else:
            candidates = self.test_text_rules(data)

        if compare_headers:
            _candidates = []

            # Verify if the columns names are somewhat similar
            with self.conn.cursor() as cur:
                for candidate in candidates:
                    cur.execute("select column_name from concepts__data where uri = %s group by uri, column_name", [ candidate ])
                    cols = [row[0] for row in cur.fetchall()]

                    if len(list(filter(lambda x: self.test_header(column_name, x), cols))) > 0:
                        _candidates.append(candidate)

            candidates = _candidates

        concept = candidates[0] if len(candidates) > 0 else None

        if autogenerate and concept is None:
            concept = self.auto_generate_concept(table_name, column_name)

        self.refresh_concept_data(concept, data_type, table_name, column_name)

        return concept


if __name__ == "__main__":
    annotate = Annotate()

    suggestion = annotate.suggest_concept("1d59473b66f4e8dea22706ee45dce9a3", "Oppervlak")
    print(suggestion)

