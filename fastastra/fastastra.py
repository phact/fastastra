import dataclasses
import uuid
from dataclasses import make_dataclass, field
from typing import Dict, Tuple, Any, Optional, List, Iterator

from pydantic import BaseModel, create_model

from datastore.cassandra_util import get_pydantic_type, python_to_cassandra, CassandraType, DDLModel, Column
from datastore.simple_cassandra_datastore import CassandraDataStore


class LoginPayload(BaseModel):
    db_id: str

datastores = {}

def get_datastore_from_cache(token) -> CassandraDataStore:
    global datastores
    if token in datastores:
        return datastores[token]
    raise Exception(detail="Must login to a database first")

def db_login(payload: LoginPayload, token: str):
    global datastores
    datastore = datastores.get(token)
    if datastore is None:
        datastore = CassandraDataStore()
    if payload.db_id == "":
        return Exception(detail='{"msg": "db_id is required."}')
    datastore.setupSession(token, payload.db_id)
    datastores[token] = datastore


class Table:
    def __init__(self, db, table_name):
        self.db = db
        self.table_name = table_name
        self.keyspace = db.keyspace
        self.setup(table_name)


    def setup(self, table_name):
        self.raw_columns = self.db.client.get_columns(self.keyspace, table_name)
        columns = []
        for row in self.raw_columns:
            columns.append(row["column_name"])

        self.columns = columns
        self.partition_keys = [column['column_name'] for column in self.raw_columns if column['kind'] == 'partition_key']
        self.clustering_columns = [column['column_name'] for column in self.raw_columns if column['kind'] == 'clustering']

        model_name = self.table_name.capitalize()

        model_fields: Dict[str, Tuple[Any, Any]] = {}
        dataclass_fields = []  # Fields for dataclass
        
        for col in self.raw_columns:
            column_name = col["column_name"]
            pydantic_type = get_pydantic_type(col["type"])
            if not (col['kind'] != 'partition_key' and col['kind'] != 'clustering'):
                #model_fields[col["column_name"]] = (pydantic_type, ...)
                model_fields[col["column_name"]] = (Optional[pydantic_type], None)
                dataclass_fields.append(
                    #(column_name, pydantic_type)
                    (column_name, Optional[pydantic_type], field(default=None))
                )

        for col in self.raw_columns:
            column_name = col["column_name"]
            pydantic_type = get_pydantic_type(col["type"])
            if col['kind'] != 'partition_key' and col['kind'] != 'clustering':
                model_fields[column_name] = (Optional[pydantic_type], None)
                dataclass_fields.append(
                    (column_name, Optional[pydantic_type], field(default=None))
                )

        ResponseModel = create_model(model_name, **model_fields)
        Dataclass = make_dataclass(model_name, dataclass_fields)

        self._model = ResponseModel
        self._dataclass = Dataclass

    def __getitem__(self, item: Any) -> BaseModel|List[BaseModel]:

        keys, args = self._get_keys_and_args(item)

        rows = self.db.client.select_from_table_by_keys(
            keyspace=self.keyspace,
            table=self.table_name,
            keys=keys,
            args=args
        )
        objs = []
        for row in rows:
            objs.append(self._dataclass(**row))
        if len(objs) == 0:
            raise KeyError(f"No record found with id: {item}")
        if len(objs) == 1:
            return objs[0]
        else:
            return objs

    def _get_keys_and_args(self, item):
        keys = []
        args = {}
        if not isinstance(item, List):
            if len(self.partition_keys) > 1:
                raise Exception(
                    f"There is more than one partition key, expected a SORTED list of values for partition keys [and optionally some or all of the clustering columns]: List[Any]. They must be in the right order:\nPartition keys: {self.partition_keys}\nClustering Columns: {self.clustering_columns}")
            else:
                keys = self.partition_keys
                args[self.partition_keys[0]] = item
        else:
            if len(item) < len(self.partition_keys):
                raise Exception(
                    f"Expected at least {len(self.partition_keys)}, received {item} which contains only {len(item)}. Primary key is:\nPartition keys: {self.partition_keys}\nClustering Columns: {self.clustering_columns}")

            if len(item) > len(self.partition_keys) + len(self.clustering_columns):
                raise Exception(
                    f"Expected at most {len(self.partition_keys) + len(self.clustering_columns)}, received {item} which contains {len(item)}. Primary key is:\nPartition keys: {self.partition_keys}\nClustering Columns: {self.clustering_columns}")
            i = 0
            for arg in item:
                if i < len(self.partition_keys):
                    partition_key = self.partition_keys[i]
                    args[partition_key] = arg
                    keys.append(partition_key)
                else:
                    clustering_column = self.clustering_columns[i - len(self.partition_keys)]
                    args[clustering_column] = arg
                    keys.append(clustering_column)
                i += 1
        args = self._cast_args(args, keys)
        return keys, args

    def _cast_args(self, args, keys):
        for key in keys:
            for column in self.raw_columns:
                if column['column_name'] == key:
                    type = get_pydantic_type(column['type'])
                    value = args[key]
                    if not isinstance(value, type):
                        args[key] = type(value)
                    break
        return args

    def __call__(self):
        return self.all()

    def exists(self) -> bool:
        tables = self.db.client.get_tables(self.keyspace)
        return self.table_name in tables

    def __repr__(self) -> str:
        return "<Table {}{}>".format(
            self.table_name,
            (
                " (does not exist yet)"
                if not self.exists()
                else " ({})".format(", ".join(c for c in self.columns))
            ),
        )

    def pydantic_model(self):
        return self._model


    def dataclass(self):
        return self._dataclass


    def all(self) -> List[dataclass]:
        rows = self.db.client.select_all_from_table(self.keyspace, self.table_name)
        return [self._dataclass(**row) for row in rows]

    def create(
            self,
            pk: str = None, # this translates into a simple single partition_key with no clustering columns
            partition_keys: str | List[str] = None,
            clustering_columns: str | List[str] = None,
            columns: Dict[str, Any] = None,
            **kwargs
    ):
        if pk:
            if partition_keys or clustering_columns:
                Exception("Cannot provide pk AND partition_keys / clustering_columns. If you provide a pk it will be treated as a single partition_key with no clustering_columns")
            else:
                partition_keys = [pk]
        if not columns:
            columns={}
        columns = {**columns, **kwargs}
        column_list = []
        if not partition_keys:
            partition_keys = []
        if isinstance(partition_keys, str):
            partition_keys = [partition_keys]
        if not clustering_columns:
            clustering_columns = []
        if isinstance(clustering_columns, str):
            clustering_columns = [clustering_columns]
        for column_name, column_type in columns.items():
            column_list.append(Column(name=column_name, type=python_to_cassandra(column_type)))
        ddl_model = DDLModel(
            keyspace_name=self.db.keyspace,
            table_name=self.table_name,
            columns=column_list,
            partition_key=partition_keys,
            clustering_columns=clustering_columns,
            thoughts=None
        )
        self.db.client.execute(ddl_model.to_string())
        self.setup(table_name=self.table_name)

    @property
    def c(self):
        return self.columns

    def insert(self, request_object: any = None, **kwargs):
        is_base_model = True
        if request_object is None:
            request_object = self._model(**kwargs)
        request_dict = None
        if isinstance(request_object, BaseModel):
            request_dict = request_object.dict()
        elif dataclasses.is_dataclass(request_object):
            request_dict = dataclasses.asdict(request_object)
            is_base_model = False
        else:
            raise Exception("insert() requires a pydantic model or dataclass object")

        for key in self.partition_keys:
            if request_dict[key] is None:
                for column in self.raw_columns:
                    if column['column_name'] == key:
                        if column['type'] == CassandraType.UUID.value:
                            request_dict[key] = uuid.uuid4()
                        elif column['type'] == CassandraType.TIMEUUID.value:
                            request_dict[key] = uuid.uuid1()
                        else:
                            raise Exception(f"insert() requires a value for {key}, got {request_object}")
                        break
        self.db.client.upsert_table_from_dict(self.keyspace, self.table_name, request_dict)
        if is_base_model:
            return self._model(**request_dict)
        else:
            return self._dataclass(**request_dict)


    def update(self, request_object: BaseModel = None, **kwargs):
        return self.insert(request_object, **kwargs)

    def delete(self, request_object: Any = None, **kwargs):
        request_dict = None
        if request_object is None:
            request_object = self._model(**kwargs)
            request_dict = request_object.dict()
        else:
            if isinstance(request_object, BaseModel):
                request_dict = request_object.dict()
            elif dataclasses.is_dataclass(request_object):
                request_dict = dataclasses.asdict(request_object)
            else:
                if len(self.partition_keys) == 1:
                    for column in self.raw_columns:
                        if column["column_name"] == self.partition_keys[0]:
                            request_dict = { column["column_name"] : request_object }
                else:
                    raise Exception("insert() requires a pydantic model, dataclass object for compound keys.")

        keys = []
        args = {}
        for key, value in request_dict.items():
            if value is not None:
                keys.append(key)
                args[key] = value

        args = self._cast_args(args, keys)

        self.db.client.delete_from_table_by_keys(
            keyspace=self.keyspace,
            table=self.table_name,
            keys=keys,
            args=args
        )


class DynamicTables:
    def __init__(self, db, tables):
        self.db = db
        self._tables = tables

    def __getattr__(self, table_name) -> Table:
        for table in self._tables:
            if table.table_name == table_name:
                return table
        table = Table(self.db, table_name)
        return table

    def __dir__(self):
        table_names = []
        for table in self._tables:
            table_names.append(table.table_name)
        return self.db.client.get_tables(self.db.keyspace) + table_names

    def __iter__(self) -> Iterator[Table]:
        return iter(self._tables)



class Database:
    def __init__(self, token, dbid):
        login_payload = LoginPayload(db_id=dbid)
        db_login(login_payload, token)
        datastore = get_datastore_from_cache(token)
        self.client = datastore.client
        self.keyspace = "default_keyspace"
        self._tables = None

    def __del__(self):
        pass

    @property
    def t(self):
        if self._tables is None:
            rows = self.client.get_tables(self.keyspace)
            tables = []
            for row in rows:
                tables.append(row)
            table_objects = []
            for table in tables:
                table_obj = Table(self, table)
                table_objects.append(table_obj)
            self._tables = table_objects
        return DynamicTables(self, self._tables)

