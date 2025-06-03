import os
import time
from typing import Any, Dict, List
from cassandra import ConsistencyLevel
import json
import requests
from cassandra.policies import RetryPolicy
from loguru import logger
from pydantic import BaseModel

from cassandra.cluster import Cluster, NoHostAvailable
from cassandra.io.libevreactor import LibevConnection
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement, dict_factory, named_tuple_factory, UNSET_VALUE


CASSANDRA_HOST = os.environ.get("CASSANDRA_HOST", "localhost")
CASSANDRA_PORT = int(os.environ.get("CASSANDRA_PORT", 9042))
CASSANDRA_KEYSPACE = os.environ.get("CASSANDRA_KEYSPACE", "default_keyspace")
CASSANDRA_USER = 'token'


class Payload(BaseModel):
    args: Dict[str, Any]

# class that implements the DataStore interface for Cassandra Datastore provider
class CassandraDataStore():
    def __init__(self):
        self.app = None
        # no longer create session on init, since we need a session per user
        # self.client = self.create_db_client()
        pass

    def create_db_client(self, token, dbid):
        self.client = CassandraClient(token, dbid)
        return self.client

    def getSubApp(self):
        return self.app

    def setupSession(self, token, dbid):
        self.dbid = dbid
        self.client = self.create_db_client(token, dbid)
        return self.client


def get_astra_bundle_url(dbid, token):
    # Define the URL
    url = f"https://api.astra.datastax.com/v2/databases/{dbid}/secureBundleURL"

    # Define the headers
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    # Define the payload (if any)
    payload = {}

    # Make the POST request
    response = requests.post(url, headers=headers, data=json.dumps(payload)).json()

    # Print the response
    if 'errors' in response:
        # Handle the errors
        errors = response['errors']
        if errors[0]['message']:
            if errors[0]['message'] == 'JWT not valid':
                logger.warning(
                    "Please use the word `token` and your AstraDB token as CASSANDRA_USER and CASSANDRA_PASSWORD respectively instead of client and secret (starting with `ASTRACS:` to allow dynamic astra keyspace creation")
        return False
    else:
        return response['downloadURL']

def make_keyspace(databaseID, token):
    # Define the URL
    url = f"https://api.astra.datastax.com/v2/databases/{databaseID}/keyspaces/{CASSANDRA_KEYSPACE}"

    # Define the headers
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    # Define the payload (if any)
    payload = {}

    # Make the POST request
    try:
        response = requests.post(url, headers=headers, data=json.dumps(payload)).json()
    except Exception as e:
        logger.debug(f"Failed to create keyspace {CASSANDRA_KEYSPACE} in database {databaseID}: {e}")
        logger.debug(f"Should be because it was hibernated, this should have woken it up. Sleeping 10.")
        time.sleep(10)
        return


    # Print the response
    if 'errors' in response:
        # Handle the errors
        errors = response['errors']
        if errors[0]['message']:
            if errors[0]['message'] == 'JWT not valid':
                logger.warning(
                    "Please use the word `token` and your AstraDB token as CASSANDRA_USER and CASSANDRA_PASSWORD respectively instead of client and secret (starting with `ASTRACS:` to allow dynamic astra keyspace creation")
    return


class VectorRetryPolicy(RetryPolicy):
    def on_read_timeout(self, query, consistency, required_responses,
                        received_responses, data_retrieved, retry_num):
        if retry_num < 3:
            logger.info(f"retrying timeout {retry_num}")
            logger.info(f"query: {query}")
            return RetryPolicy.RETRY, consistency  # return a tuple
        else:
            return RetryPolicy.RETHROW, consistency

    def on_request_error(self, query, consistency, error, retry_num):
        if retry_num < 3:
            logger.info(f"retrying error {retry_num}")
            logger.info(f"query: {query}")
            return RetryPolicy.RETRY, consistency  # return a tuple
        else:
            return RetryPolicy.RETHROW, consistency


    def on_unavailable(self, query, consistency, required_replicas, alive_replicas, retry_num):
        return RetryPolicy.RETHROW, consistency  # return a tuple

    def on_write_timeout(self, query, consistency, write_type,
                         required_responses, received_responses, retry_num):
        return RetryPolicy.RETHROW, consistency  # return a tuple




class CassandraClient():
    def __init__(self, token, dbid) -> None:
        super().__init__()
        self.dbid = dbid
        self.cluster =  None
        try:
            self.connect(token,dbid)
        except NoHostAvailable as e:
            logger.warning(f"No host in the cluster could be contacted: {e}")
            # sleep and retry
            time.sleep(5)
            self.connect(token,dbid)
        except Exception as e:
            logger.warning(f"Exception connecting to cluster: {e}")
            raise e
        # TODO: potentially re-enable document table creation for vector search enabled databases
        #self.create_table(token)

    def connect(self, token, dbid):
        if dbid is not None:
            # connect to Astra
            url = get_astra_bundle_url(dbid, token)
            if url:
                # Download the secure connect bundle and extract it
                r = requests.get(url)
                bundlepath = f'/tmp/{dbid}.zip'
                with open(bundlepath, 'wb') as f:
                    f.write(r.content)
                # Connect to the cluster
                cloud_config = {
                    'secure_connect_bundle': bundlepath
                }
                auth_provider = PlainTextAuthProvider(CASSANDRA_USER, token)
                # TODO - support unhibernating things
                try:
                    self.cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
                    self.cluster.connection_class = LibevConnection
                except Exception as e:
                    make_keyspace(dbid, token)
                    logger.warning(f"DB {dbid} is Hibernated, will attempt to wake it up")
                    return self.connect(token,dbid)

                self.session = self.cluster.connect()
            else:
                #time.sleep(5)
                #return self.connect(token, dbid)
                #print("Failed to get secure bundle URL for token and db")
                raise Exception("Failed to establish database connection, please check your astradb token")

    def execute(self, ddl):
        try:
            statement = SimpleStatement(
                ddl
                , consistency_level=ConsistencyLevel.QUORUM
            )
            self.session.execute(statement)
        except Exception as e:
            logger.warning(f"Exception creating table or index: {e}")
            raise Exception(f"Failed to create table or index {e}")

    def __del__(self):
        # close the connection when the client is destroyed
        if self.cluster:
            self.cluster.shutdown()


    async def select_all_from_table_async(self, keyspace, table) -> List[Dict[str, Any]]:
        return self.select_all_from_table(keyspace, table)

    def select_all_from_table(self, keyspace, table) -> List[Dict[str, Any]]:
        queryString = f"""SELECT * FROM {keyspace}.{table} limit 10"""
        statement = self.session.prepare(queryString)
        statement.consistency_level = ConsistencyLevel.QUORUM
        self.session.row_factory = dict_factory
        rows = self.session.execute(statement)
        json_rows = [dict(row) for row in rows]
        self.session.row_factory = named_tuple_factory
        return json_rows

    def delete_from_table_by_keys(self, keyspace, table, keys, args) -> List[Dict[str, Any]]:
        queryString = f"""DELETE FROM {keyspace}.{table} WHERE """
        partitionKeyValues = []
        for column in keys:
            queryString += f"{column} = ? AND "
            partitionKeyValues.append(args[column])
        # remove the last AND
        queryString = queryString[:-4]
        statement = self.session.prepare(queryString)
        statement.consistency_level = ConsistencyLevel.QUORUM
        preparedStatement = statement.bind(partitionKeyValues)
        self.session.row_factory = dict_factory
        rows = self.session.execute(preparedStatement)
        json_rows = [dict(row) for row in rows]
        self.session.row_factory = named_tuple_factory
        return json_rows

    def select_from_table_by_keys(self, keyspace, table, keys, args) -> List[Dict[str, Any]]:
        queryString = f"""SELECT * FROM {keyspace}.{table} WHERE """
        partitionKeyValues = []
        for column in keys:
            queryString += f"{column} = ? AND "
            partitionKeyValues.append(args[column])
        # remove the last AND
        queryString = queryString[:-4]
        statement = self.session.prepare(queryString)
        statement.consistency_level = ConsistencyLevel.QUORUM
        preparedStatement = statement.bind(partitionKeyValues)
        self.session.row_factory = dict_factory
        rows = self.session.execute(preparedStatement)
        json_rows = [dict(row) for row in rows]
        self.session.row_factory = named_tuple_factory
        return json_rows

    async def upsert_table_from_dict_async(self, keyspace_name: str, table_name : str, obj : Dict):
        return self.upsert_table_from_dict(keyspace_name, table_name, obj)

    def upsert_table_from_dict(self, keyspace_name: str, table_name : str, obj : Dict):
        logger.info(f"going to upsert keyspace {keyspace_name} and table {table_name} using {obj}")
        fields = ', '.join(obj.keys())
        placeholders = ', '.join(['?' for _ in range(len(obj.keys()))])

        values_list = []

        for field in obj.keys():
            value = obj.get(field)
            if value is None:
                formatted_value = UNSET_VALUE
            else:
                formatted_value = value
            values_list.append(formatted_value)

        query_string = f"""insert into {keyspace_name}.{table_name}(
                {fields}
            ) VALUES (
                {placeholders}
            );"""

        logger.info(f"Preparing query_string: {query_string}")
        statement = self.session.prepare(query_string)
        statement.consistency_level = ConsistencyLevel.QUORUM

        for value in values_list:
            if value is None:
                values_list[values_list.index(value)] = UNSET_VALUE

        try:
            response = self.session.execute(
                statement,
                tuple(values_list)
            )
        except Exception as e:
            logger.error(f"failed to upsert {table_name}: {obj}")
            raise e

    async def get_tables_async(self, keyspace):
        return self.get_tables(keyspace)

    async def get_keyspaces_async(self):
        return self.get_keyspaces()

    def get_keyspaces(self) -> List[str]:
        queryString = "SELECT DISTINCT keyspace_name FROM system_schema.tables"
        statement = self.session.prepare(queryString)
        statement.consistency_level = ConsistencyLevel.QUORUM
        rows = self.session.execute(statement)
        keyspaces = [row.keyspace_name for row in rows]
        keyspaces.remove("system_auth")
        keyspaces.remove("system_schema")
        keyspaces.remove("system")
        keyspaces.remove("data_endpoint_auth")
        keyspaces.remove("system_traces")
        keyspaces.remove("datastax_sla")
        return keyspaces


    async def get_columns_async(self, keyspace, table) -> List[Dict[str, Any]]:
        return self.get_columns(keyspace, table)

    def get_tables(self, keyspace) -> List[str]:
        queryString = f"""SELECT table_name FROM system_schema.tables WHERE keyspace_name='{keyspace}'"""
        statement = self.session.prepare(queryString)
        statement.consistency_level = ConsistencyLevel.QUORUM
        rows = self.session.execute(statement)
        tables = [row.table_name for row in rows]
        return tables

    def get_indexes(self, keyspace, table):
        queryString = f"""
        SELECT options FROM system_schema.indexes 
        WHERE keyspace_name='{keyspace}' 
        and table_name = '{table}'
        and kind = 'CUSTOM' ALLOW FILTERING;
        """
        statement = self.session.prepare(queryString)
        statement.consistency_level = ConsistencyLevel.QUORUM
        self.session.row_factory = dict_factory
        rows = self.session.execute(statement)
        indexes = [row['options'] for row in rows]
        indexed_columns = []
        for index in indexes:
            options = dict(index)
            # TODO - extract whether it's dot vs cosine for vector types
            # and extract vector type
            if 'StorageAttachedIndex' in options['class_name']:
                indexed_columns.append(options['target'])
        self.session.row_factory = named_tuple_factory
        return indexed_columns

    def get_columns(self, keyspace, table) -> List[Dict[str, Any]]:
        queryString = f"""select column_name, kind, type, position from system_schema."columns" WHERE keyspace_name = ? and table_name = ?;"""
        statement = self.session.prepare(queryString)
        bound_statement = statement.bind((keyspace, table))
        bound_statement.consistency_level = ConsistencyLevel.QUORUM
        self.session.row_factory = dict_factory
        rows = self.session.execute(bound_statement)
        json_rows = [dict(row) for row in rows]
        self.session.row_factory = named_tuple_factory
        return json_rows

    def select_from_table_by_index(self, keyspace, table, indexed_columns, vector_indexes, partition_keys, columns, args) -> List[Dict[str, Any]]:
        queryString = f"SELECT "
        bind_values = []
        for column in columns:
            if column["column_name"] in vector_indexes and column["column_name"] in args:
                # This will work for all vector tables as long as they use openapi ada-002 to make their embeddings.
                # TODO - support other vector types
                # check if column type is vector of dimension 384 and assume that means e5 embedding. IRL we should encode the embedding algo in the index name
                #if '384' in column['type']:
                #    embeddings.append(get_e5_embeddings([args[column['column_name']]]))
                # otherwise default to davinci-002
                #else:
                # TODO maybe optionally support getting embeddings
                #embeddings.append(get_embeddings([args[column['column_name']]]))
                # TODO maybe support scores one day?
                #queryString += f"similarity_cosine(?, {column['column_name']}) as {column['column_name']}_score, "
                # TODO maybe support optionally not sending back vector values
                queryString += f"{column['column_name']}, "
            else:
                queryString += f"{column['column_name']}, "
        queryString = queryString[:-2]

        has_pk_args = False
        for column in partition_keys:
            if column in args:
                has_pk_args = True

        indexed_arg = False
        for col in indexed_columns:
            if col in args:
                indexed_arg = True
        if (len(indexed_columns) > 0 and indexed_arg) or has_pk_args:
            queryString += f""" FROM {keyspace}.{table} WHERE """
        else:
            queryString += f""" FROM {keyspace}.{table} """


        for column in indexed_columns:
            if column in args:
                if args[column]:
                    # queryString += f"{column} = '{args[column]}' AND "
                    queryString += f"{column} = ? AND "
                    bind_values.append(args[column])

        # remove the last AND
        if indexed_arg and not has_pk_args:
            queryString = queryString[:-4]

        if has_pk_args:
            for column in partition_keys:
                if column in args:
                    queryString += f"{column} = ? AND "
                    bind_values.append(args[column])
        # remove the last AND
        if has_pk_args:
            queryString = queryString[:-4]

        if len(vector_indexes) > 0:
            for column in vector_indexes:
                if column in args:
                    queryString += f" ORDER BY "
                    break

        for column in vector_indexes:
            if column in args:
                queryString += f"""
                {column} ann of ?
                """
                bind_values.append(args[column])
        # TODO make limit configurable
        queryString += f"LIMIT 20"

        statement = self.session.prepare(queryString)
        statement.retry_policy = VectorRetryPolicy()
        statement.consistency_level = ConsistencyLevel.LOCAL_ONE
        boundStatement = statement.bind(bind_values)
        self.session.row_factory = dict_factory
        rows = self.session.execute(boundStatement, timeout=100)
        json_rows = [dict(row) for row in rows]
        #json_rows = [{k: v for k, v in row.items() if not k in vector_indexes} for row in json_rows]
        self.session.row_factory = named_tuple_factory
        return json_rows
