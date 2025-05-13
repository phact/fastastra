# fastastra

[![commits](https://img.shields.io/github/commit-activity/m/phact/fastastra)](https://github.com/phact/fastastra/commits/main)
[![Github Last Commit](https://img.shields.io/github/last-commit/phact/fastastra)](https://github.com/phact/fastastra/commits/main)
[![PyPI version](https://badge.fury.io/py/fastastra.svg)](https://badge.fury.io/py/fastastra)
[![Discord chat](https://img.shields.io/static/v1?label=Chat%20on&message=Discord&color=blue&logo=Discord&style=flat-square)](https://discord.gg/MEFVXUvsuy)
[![Stars](https://img.shields.io/github/stars/phact/fastastra?style=social)](https://github.com/phact/fastastra/stargazers)

fastastra is modeled after [fastlite](https://github.com/AnswerDotAI/fastlite) and it allows you to use [FastHTML](https://github.com/AnswerDotAI/fasthtml) with AstraDB (Cassandra). 

## Installation

    poetry add fastastra

or 

    pip install fastastra


## To connect:

    from fastastra.fastastra import AstraDatabase
    db = AstraDatabase(token, dbid) # get your token and dbid from https://astra.datastax.com

## Basic usage

### List tables
    db.t
    
### Create a table
    cats = db.t.cats
    if cats not in db.t:
        cats.create(cat_id=uuid.uuid1, name=str, partition_keys='cat_id')


### Insert a row
    cat_timeuuid = uuid.uuid1()
    cats.update(cat_id=cat_timeuuid, name="fluffy")

### List all rows
    rows = cats()

### ANN / vector search
    db = AstraDatabase(token, dbid, embedding_model="embed-english-v3.0") # supports all embedding models in LiteLLM using env vars
    dogs = db.t.dogs
    if dogs not in db.t:
        #dogs.create(id=int, name=str, good_boy=bool, embedding=(list[float], 2), pk='id') # specify dimensions in create
        dogs.create(id=int, name=str, good_boy=bool, embedding=list[float], pk='id') # infer dimensions from db.embedding_model
        dogs.c.good_boy.index()
        dogs.c.embedding.index()

    dogs.insert(id=2, good_boy=True, name="spike", embedding=[0.1, 0.2])

    index_lookukp = dogs.xtra(good_boy=True)
    ann_matches = dogs.xtra(embedding=[0.2, 0.2])

### Get dataclass and pydantic model
    dataclass = cats.dataclass()
    model = cats.pydantic_model()

### Get a row
    print(cats[cat_timeuuid])

# Delete a row
    cats.delete(cat_timeuuid)

 or

    cats.delete(str(cat_timeuuid))


## Run a FastHTML example:

This example was taken almost verbatim from [the FastHTML examples repo](https://github.com/AnswerDotAI/fasthtml-example). The only change was the dependency, the db connection string, and changing the id from `int` to `uuid1`.

    uv sync

    uv run python examples/todo.py
