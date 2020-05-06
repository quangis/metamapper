# MetaMapper
The MetaMapper module provides various tools to enable automated extraction, ingestion, and annotation of online spatial data sources. Each method is exposed as an API to be used together with [metamapper-web](https://github.com/quangis/metamapper-web), which provides a front-end to assist with the annotation process.


## Dependencies
* [Geckodriver](https://github.com/mozilla/geckodriver/releases) for use with Selenium. Other webdrivers may also be used with some small adjustments.
* PostgreSQL + PostGIS
* [metamapper-web](https://github.com/quangis/metamapper-web)


## Use
This project uses `pipenv` to manage dependencies.
```
pipenv install
```

After setting up the environment, it can be run directly with:
```
pipenv run python api.py
```

Running `api.py` will open a Selenium window in which it will attempt to load metamapper-web by visiting port 3000 locally. Make sure it is running beforehand.
