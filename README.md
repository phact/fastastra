# fastastra

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

    poetry install

    poetry run python examples/todo.py