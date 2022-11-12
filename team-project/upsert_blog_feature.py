# -*- coding: utf-8 -*-
# 필요 패키지
# pip install azure-cosmos aiohttp
# pip install pprint
# pip install asyncio
import json
import re

from azure.cosmos.aio import CosmosClient

from azure.cosmos import PartitionKey, exceptions
import pprint
import asyncio
import pandas as pd


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

# <method_populate_container_items>
async def populate_container_items(container_obj, items_to_create):
    # Add items to the container
    insert_items_to_create = items_to_create
    # <create_item>
    for item in insert_items_to_create:
        # item['id'] = re.findall('(?<=\\?)[^#]+', item['id'])[0]
        inserted_item = await container_obj.upsert_item(body=item)
        print("Inserted item for Item Id: %s" %(inserted_item['id']))
    # </create_item>
# </method_populate_container_items>

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

    return items
    for item in items:
        pprint.pprint(item)
    # </query_items>
# </method_query_items>

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
            items = await query_items(container_obj, query)
            return items
        except exceptions.CosmosHttpResponseError as e:
            print('\nrun_sample has caught an error. {0}'.format(e.message))
        finally:
            print("\nQuickstart complete")
            
# <run_sample>
async def upsert_blog_info(js):
    # <create_cosmos_client>
    async with CosmosClient(endpoint, key) as client:
        try:
            # create a database
            database_obj = await get_or_create_db(client, database_name)
            # create a container
            container_obj = await get_or_create_container(database_obj, container_name)
            # generate some family items to test create, read, delete operations
            # populate the family items in container
            for item in js:
                results = await read_blog_info('url', item['id'])
                if len(results) != 0:
                    results[0]['get_reserved_link'] = item['get_reserved_link']
                    results[0]['num_neibors'] = item['num_neibors']
                    results[0]['num_likes'] = item['num_likes']
                    results[0]['num_hashtags'] = item['num_hashtags']
                    results[0]['img_url'] = item['img_url']
                    results[0]['num_blog'] = item['num_blog']
                    await populate_container_items(container_obj, results)    
                
        except exceptions.CosmosHttpResponseError as e:
            print('\nrun_sample has caught an error. {0}'.format(e.message))
        finally:
            print("\nQuickstart complete")
            await client.close()            

if __name__=="__main__":
    # get_feature_release 파일을 통하여 얻어진 save_results.json 파일의 내용을 열어서 DB에다 쏴줌
    with open('saved_results.json') as f:
        js = json.loads(f.read())
     
    loop = asyncio.get_event_loop()
    loop.run_until_complete(upsert_blog_info(js))
    

