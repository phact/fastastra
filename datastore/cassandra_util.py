from pydantic import BaseModel, validator, Field
from enum import Enum
from datetime import date, time, datetime, timedelta
from typing import List, Dict, Type, Any, Union
from cassandra.cqltypes import cqltype_to_python
import uuid

class CassandraColumn(BaseModel):
    name: str = Field(..., description="Name of the column")
    type: str = Field(..., description="Cassandra data type of the column. Must be either a base type: \nascii, bigint, blob, boolean, counter, date, decimal, double, duration, float, inet, int, smallint, text, time, timestamp, timeuuid, tinyint, uuid, varchar, varint\nOr a collection where text could be replaced with a base type set<text>, list<text>, map<text, text>\n or finally a vector type which is always float and takes the number of dimensions: vector<float, 1024>")
    @validator('type')
    def validate_type(cls, v):
        try:
            cls._validate_cassandra_type(v)
        except ValueError:
            raise ValueError(f"Unsupported type {v}. Must be a valid Cassandra type.")
        return v

    @staticmethod
    def _validate_cassandra_type(type_str: str):
        python_type = cqltype_to_python(type_str)
        if len(python_type) > 1:
            if python_type[0] == 'set' or python_type[0] == 'list':
                CassandraColumn._validate_cassandra_type(python_type[1][0])
            elif python_type[0] == 'map':
                CassandraColumn._validate_cassandra_type(python_type[1][0])
                CassandraColumn._validate_cassandra_type(python_type[1][1])
            elif python_type[0] == 'vector':
                # No need for further validation; vectors are lists of floats
                return
            else:
                raise ValueError(f"Unsupported collection type {type_str}")
        elif python_type[0] not in {item.value for item in CassandraType}:
            raise ValueError(f"Unsupported type {type_str}")


class DDLModel(BaseModel):
    keyspace_name: str = Field(..., description="Name of the keyspace")
    table_name: str = Field(..., description="Name of the table")
    columns: List[CassandraColumn] = Field(..., description="List of columns with their types, this includes partition key and clustering columns")
    partition_key: List[str] = Field(..., description="List of partition key column names, *NOTE ORDER MATTERS*")
    clustering_columns: List[str] = Field(..., description="List of clustering column column names, *NOTE ORDER MATTERS*")

    def to_string(self, keyspace_name: str = None, table_name: str = None):
        if keyspace_name is not None:
            self.keyspace_name = keyspace_name
        if table_name is not None:
            self.table_name = table_name
        ddl_statement = f"CREATE TABLE {self.keyspace_name}.{self.table_name} (\n" + \
                        ",\n".join([f"    {column.name} {column.type}" for column in self.columns]) + \
                        ",\n    PRIMARY KEY ((" + \
                        ", ".join(self.partition_key) + ")"
        if len(self.clustering_columns) > 0:
            ddl_statement += ", " + ", ".join(self.clustering_columns)
        ddl_statement += ")\n);"
        return ddl_statement


class CassandraType(Enum):
    ASCII = "ascii"
    BIGINT = "bigint"
    BLOB = "blob"
    BOOLEAN = "boolean"
    COUNTER = "counter"
    DATE = "date"
    DECIMAL = "decimal"
    DOUBLE = "double"
    DURATION = "duration"
    FLOAT = "float"
    INET = "inet"
    INT = "int"
    SMALLINT = "smallint"
    TEXT = "text"
    TIME = "time"
    TIMESTAMP = "timestamp"
    TIMEUUID = "timeuuid"
    TINYINT = "tinyint"
    UUID = "uuid"
    VARCHAR = "varchar"
    VARINT = "varint"
    SET = "set"
    MAP = "map"
    LIST = "list"
    VECTOR = "vector"


class PydanticType(BaseModel):
    type_str: str

    @property
    def python_type(self) -> Type[Any]:
        python_type = cqltype_to_python(self.type_str)
        if len(python_type) > 1:
            if python_type[0] == CassandraType.SET.value or python_type[0] == CassandraType.LIST.value:
                return List[get_pydantic_type(python_type[1][0])]
            if python_type[0] == CassandraType.MAP.value:
                return Dict[get_pydantic_type(python_type[1][0]), get_pydantic_type(python_type[1][1])]
            if python_type[0] == CassandraType.VECTOR.value:
                return List[float]
            else:
                print(f"Unsupported type {self.type_str}")
                return Any
        else:
            return self._simple_python_type(python_type[0])

    @staticmethod
    def _simple_python_type(cassandra_type: str) -> Type[Any]:
        return {
            CassandraType.ASCII.value: str,
            CassandraType.BIGINT.value: int,
            CassandraType.BLOB.value: bytes,
            CassandraType.BOOLEAN.value: bool,
            CassandraType.COUNTER.value: int,
            CassandraType.DATE.value: date,
            CassandraType.DECIMAL.value: float,
            CassandraType.DOUBLE.value: float,
            CassandraType.DURATION.value: timedelta,
            CassandraType.FLOAT.value: float,
            CassandraType.INET.value: str,
            CassandraType.INT.value: int,
            CassandraType.SMALLINT.value: int,
            CassandraType.TEXT.value: str,
            CassandraType.TIME.value: time,
            CassandraType.TIMESTAMP.value: datetime,
            CassandraType.TIMEUUID.value: uuid.UUID,
            CassandraType.TINYINT.value: int,
            CassandraType.UUID.value: uuid.UUID,
            CassandraType.VARCHAR.value: str,
            CassandraType.VARINT.value: int,
        }.get(cassandra_type, Any)

    @property
    def openapi_type(self) -> Dict[str, Union[str, Dict]]:
        python_type = cqltype_to_python(self.type_str)
        if len(python_type) > 1:
            if python_type[0] == CassandraType.SET.value or python_type[0] == CassandraType.LIST.value:
                return {"type": "array", "items": self._simple_openapi_type(python_type[1][0])}
            if python_type[0] == CassandraType.MAP.value:
                return {
                    "type": "object",
                    "additionalProperties": {
                        "type": self._simple_openapi_type(python_type[1][1])["type"]
                    }
                }
            if python_type[0] == CassandraType.VECTOR.value:
                return {"type": "array", "items": {"type": "number", "format": "float"}}
            else:
                print(f"Unsupported type {self.type_str}")
                return {"type": "string"}
        else:
            return self._simple_openapi_type(python_type[0])

    @staticmethod
    def _simple_openapi_type(cassandra_type: str) -> Dict[str, Union[str, Dict]]:
        return {
            CassandraType.ASCII.value: {"type": "string"},
            CassandraType.BIGINT.value: {"type": "integer", "format": "int64"},
            CassandraType.BLOB.value: {"type": "string", "format": "byte"},
            CassandraType.BOOLEAN.value: {"type": "boolean"},
            CassandraType.COUNTER.value: {"type": "integer", "format": "int64"},
            CassandraType.DATE.value: {"type": "string", "format": "date"},
            CassandraType.DECIMAL.value: {"type": "number", "format": "double"},
            CassandraType.DOUBLE.value: {"type": "number", "format": "double"},
            CassandraType.DURATION.value: {"type": "string"},
            CassandraType.FLOAT.value: {"type": "number", "format": "float"},
            CassandraType.INET.value: {"type": "string"},
            CassandraType.INT.value: {"type": "integer", "format": "int32"},
            CassandraType.SMALLINT.value: {"type": "integer", "format": "int32"},
            CassandraType.TEXT.value: {"type": "string"},
            CassandraType.TIME.value: {"type": "string", "format": "time"},
            CassandraType.TIMESTAMP.value: {"type": "string", "format": "date-time"},
            CassandraType.TIMEUUID.value: {"type": "string", "format": "uuid"},
            CassandraType.TINYINT.value: {"type": "integer", "format": "int32"},
            CassandraType.UUID.value: {"type": "string", "format": "uuid"},
            CassandraType.VARCHAR.value: {"type": "string"},
            CassandraType.VARINT.value: {"type": "integer", "format": "int64"},
        }.get(cassandra_type, {"type": "string"})


def get_pydantic_type(type_str: str) -> Type[Any]:
    return PydanticType(type_str=type_str).python_type

def get_openapi_type(type_str: str) -> Dict[str, Union[str, Dict]]:
    return PydanticType(type_str=type_str).openapi_type

def python_to_cassandra(py_type: Type[Any], embedding_dimensions: int = None) -> str:
    mapping = {
        str: CassandraType.TEXT.value,
        int: CassandraType.INT.value,
        bytes: CassandraType.BLOB.value,
        bool: CassandraType.BOOLEAN.value,
        date: CassandraType.DATE.value,
        float: CassandraType.DOUBLE.value,
        timedelta: CassandraType.DURATION.value,
        time: CassandraType.TIME.value,
        datetime: CassandraType.TIMESTAMP.value,
        uuid.uuid4: CassandraType.UUID.value,
        uuid.uuid1: CassandraType.TIMEUUID.value,
        List: CassandraType.LIST.value,
        Dict: CassandraType.MAP.value,
    }
    cass_type = mapping.get(py_type, None)
    if cass_type is None:
        if isinstance(py_type, tuple):
            try:
                if py_type[0] == List[float] or py_type[0] == list[float]:
                    dimensions = py_type[1]
                    cass_type = f"vector<float, {dimensions}>"

                else:
                    raise Exception(f"type not supported {py_type}")
            except Exception as e:
                Exception("expected (List[float], dimensions) for vector types")
        else:
            if py_type[0] == List[float] or py_type[0] == list[float]:
                if embedding_dimensions is None:
                    raise Exception("embedding_dimensions must be provided for vector types either in db.embedding_dimensions or in the type in .create (List[float], dimensions)")
                cass_type = f"vector<float, {embedding_dimensions}>"
            else:
                raise Exception(f"type not supported {py_type}")
    return cass_type