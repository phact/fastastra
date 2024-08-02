import os
import uuid

import dotenv

from fastastra.fastastra import AstraDatabase

dotenv.load_dotenv("./.env")
dotenv.load_dotenv("../.env")


def test_fastastra():
    token = os.environ["ASTRA_DB_APPLICATION_TOKEN"]
    dbid = os.environ["DBID"]

    db = AstraDatabase(token, dbid)

    # Create a new table using Fastlite-compatible syntax
    cats = db.t.cats
    if cats not in db.t:
        cats.create(cat_id=uuid.uuid1, title=str, done=bool, partition_keys='cat_id')
    dogs = db.t.dogs
    if dogs not in db.t:
        dogs.create(id=int, owner=str, country=str, name=str, good_boy=bool, partition_keys=['country', 'owner'], clustering_columns=['id'])
    ponies = db.t.ponies
    if ponies not in db.t:
        ponies.create(id=uuid.uuid4, owner=str, country=str, name=str, good_boy=bool, pk="id")


    print("Tables:", list(db.t))

    # List columns of the new table using .c property
    print("Cats table columns:", [attr for attr in dir(cats.c)])

    print("Dogs table columns:", [attr for attr in dir(dogs.c)])

    print("Dogs table columns:", [attr for attr in dir(ponies.c)])

    # Upsert rows
    cat_timeuuid = uuid.uuid1()
    cats.insert(cat_id=cat_timeuuid, title="hi")
    cats.update(cat_id=uuid.uuid1(), title="hello")
    cat = cats.update(title="hello")
    print(cat.cat_id)

    # Upsert rows
    dogs.insert(id=1, owner="seb", country="usa")
    dogs.insert(id=2, owner="seb", country="usa", name="spike")
    dogs.update(id=3, owner="seb", country="usa")
    dogs.update(id=4, owner="seb", country="usa", good_boy=True)

    # Upsert rows
    pony_uuid = uuid.uuid4()
    ponies.insert(id=pony_uuid, owner="seb", country="usa")
    ponies.insert(id=uuid.uuid4(), owner="seb", country="usa", name="spike")
    ponies.update(id=uuid.uuid4(), owner="seb", country="usa")
    ponies.update(id=uuid.uuid4(), owner="seb", country="usa", good_boy=True)



    # List all rows
    rows = cats()
    print(rows)

    # List all rows
    rows = dogs()
    print(rows)

    # List all rows
    rows = ponies()
    print(rows)

    dataclass = cats.dataclass()
    model = cats.pydantic_model()


    print(dataclass)
    print(model)
    print(dataclass(cat_id=cat_timeuuid))

    dataclass = dogs.dataclass()
    model = dogs.pydantic_model()
    print(dataclass)
    print(model)

    dog_dataclass = dataclass(id=1, owner="seb", country="usa")
    dogs.insert(dog_dataclass)

    dataclass = ponies.dataclass()
    model = ponies.pydantic_model()
    print(dataclass)
    print(model)
    print(dataclass(id=pony_uuid))

    print(cats[cat_timeuuid])
    print(dogs[["usa","seb", 1]])
    print(dogs[["usa","seb"]])
    print(ponies[pony_uuid])
    print(ponies[str(pony_uuid)])

    # Delete a row
    dogs.delete(id=1, owner="seb", country="usa")
    ponies.delete(pony_uuid)
    cats.delete(str(cat_timeuuid))
