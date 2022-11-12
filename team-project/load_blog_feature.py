# -*- coding: utf-8 -*-
# 필요 패키지
# pip install azure-cosmos aiohttp
# pip install pprint
# pip install asyncio
import json
from bs4 import BeautifulSoup
import urllib
import pandas as pd

from azure.cosmos.aio import CosmosClient
from azure.cosmos import PartitionKey, exceptions
import pprint
import asyncio

# <add_uri_and_key>
endpoint = 'https://baf675-store.documents.azure.com:443/'
key = 'tHnA10FKu9Vgp3uVYx6H59AbXDPn6dYS8zut0IIvFTZiE0hc7Pt4yvc6zbCNeaCh078DMqpF4kKYNVCi3cLQgg=='
# </add_uri_and_key>

# <define_database_and_container_name>
database_name = 'BAF675Database'
container_name = 'BlogInfoContainer'
# </define_database_and_container_name>

# <create_database_if_not_exists>
async def get_or_create_db(client, database_name):
    try:
        database_obj  = client.get_database_client(database_name)
        await database_obj.read()
        return database_obj
    except exceptions.CosmosResourceNotFoundError:
        pprint.pprint("Creating database")
        return await client.create_database(database_name)
    
# Create a container
# Using a good partition key improves the performance of database operations.
# <create_container_if_not_exists>
async def get_or_create_container(database_obj, container_name):
    try:        
        todo_items_container = database_obj.get_container_client(container_name)
        await todo_items_container.read()   
        return todo_items_container
    except exceptions.CosmosResourceNotFoundError:
        print("Creating container with lastName as partition key")
        return await database_obj.create_container(
            id=container_name,
            partition_key=PartitionKey(path="/id"),
            offer_throughput=400)
    except exceptions.CosmosHttpResponseError:   
        raise
# </create_container_if_not_exists>    

# <method_query_items>
async def query_items(container_obj, query_text):
    # enable_cross_partition_query should be set to True as the container is partitioned
    # In this case, we do have to await the asynchronous iterator object since logic
    # within the query_items() method makes network calls to verify the partition key
    # definition in the container
    # <query_items>
    query_items_response = container_obj.query_items(
        query=query_text,
        enable_cross_partition_query=True
    )
    request_charge = container_obj.client_connection.last_response_headers['x-ms-request-charge']
    items = [item async for item in query_items_response]
    df = pd.DataFrame(items)
    df.to_json('read_data.json',orient='records',force_ascii=False)
    for item in items:
        pprint.pprint(item)
    # </query_items>
# </method_query_items>

# <run_sample>
async def read_blog_info(attr, value):
    # <create_cosmos_client>
    async with CosmosClient(endpoint, credential = key) as client:
    # </create_cosmos_client>
        try:
            # create a database
            database_obj = await get_or_create_db(client, database_name)
            # # create a container
            container_obj = await get_or_create_container(database_obj, container_name)
            # # generate some family items to test create, read, delete operations
            # # Query these items using the SQL query syntax. 
            # # Specifying the partition key value in the query allows Cosmos DB to retrieve data only from the relevant partitions, which improves performance
            query = "SELECT * FROM c where c.{0} = '{1}'".format(attr, value)
            await query_items(container_obj, query)
        except exceptions.CosmosHttpResponseError as e:
            print('\nrun_sample has caught an error. {0}'.format(e.message))
        finally:
            print("\nQuickstart complete")
            
# <run_sample>
async def read_all_blog_info():
    # <create_cosmos_client>
    async with CosmosClient(endpoint, credential = key) as client:
    # </create_cosmos_client>
        try:
            # create a database
            database_obj = await get_or_create_db(client, database_name)
            # # create a container
            container_obj = await get_or_create_container(database_obj, container_name)
            # # generate some family items to test create, read, delete operations
            # # Query these items using the SQL query syntax. 
            # # Specifying the partition key value in the query allows Cosmos DB to retrieve data only from the relevant partitions, which improves performance
            query = "SELECT * FROM c"
            await query_items(container_obj, query)
        except exceptions.CosmosHttpResponseError as e:
            print('\nrun_sample has caught an error. {0}'.format(e.message))
        finally:
            print("\nQuickstart complete")

if __name__=="__main__":
     
    loop = asyncio.get_event_loop()
    # db의 모든 record를 다 읽어서 read_data.json으로 쏴주는 부분, 둘 중에 하나만 실행해야 함
    # loop.run_until_complete(read_all_blog_info())
    
    
    # 특정 column의 특정 값을 가진 record만 읽어서 read_data.json으로 쏴주는 부분, 둘 중에 하나만 실행해야 함
    # "SELECT * FROM c where c.{0} = '{1}'".format(attr, value) 이런 식임
    loop.run_until_complete(read_blog_info('input_user','조현하'))
    
    

