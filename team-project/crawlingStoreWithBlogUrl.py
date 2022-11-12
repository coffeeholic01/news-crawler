from genericpath import exists
import time
import os
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from time import sleep
from bs4 import BeautifulSoup
import re
import json
import pprint

from azure.cosmos.aio import CosmosClient as cosmos_client
from azure.cosmos import PartitionKey, exceptions
import asyncio

# <add_uri_and_key>
endpoint = "https://baf675-store.documents.azure.com:443/"
key = "tHnA10FKu9Vgp3uVYx6H59AbXDPn6dYS8zut0IIvFTZiE0hc7Pt4yvc6zbCNeaCh078DMqpF4kKYNVCi3cLQgg=="
# </add_uri_and_key>

# <define_database_and_container_name>
database_name = 'BAF675Database'
container_name = 'StoreContainer'
# </define_database_and_container_name>

# <create_database_if_not_exists>
async def get_or_create_db(client, database_name):
    try:
        database_obj  = client.get_database_client(database_name)
        await database_obj.read()
        return database_obj
    except exceptions.CosmosResourceNotFoundError:
        print("Creating database")
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
            partition_key=PartitionKey(path="/lastName"),
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
        inserted_item = await container_obj.upsert_item(body=item)
        # print("Inserted item for %s family. Item Id: %s" %(item, inserted_item['id']))
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
    for item in items:
        pprint.pprint(item)
    # </query_items>
# </method_query_items>

# <run_sample>
async def insert_store_info(store_dict):
    # <create_cosmos_client>
    async with cosmos_client(endpoint, credential = key) as client:
    # </create_cosmos_client>
        try:
            # create a database
            database_obj = await get_or_create_db(client, database_name)
            # create a container
            container_obj = await get_or_create_container(database_obj, container_name)
            # generate some family items to test create, read, delete operations
            # populate the family items in container
            await populate_container_items(container_obj, store_dict)  
        except exceptions.CosmosHttpResponseError as e:
            print('\nrun_sample has caught an error. {0}'.format(e.message))
        finally:
            print("\nQuickstart complete")
            
# <run_sample>
async def read_store_info(store_name):
    # <create_cosmos_client>
    async with cosmos_client(endpoint, credential = key) as client:
    # </create_cosmos_client>
        try:
            # create a database
            database_obj = await get_or_create_db(client, database_name)
            # # create a container
            container_obj = await get_or_create_container(database_obj, container_name)
            # # generate some family items to test create, read, delete operations
            # # Query these items using the SQL query syntax. 
            # # Specifying the partition key value in the query allows Cosmos DB to retrieve data only from the relevant partitions, which improves performance
            query = "SELECT * FROM c where c.id = '{}'".format(store_name)
            await query_items(container_obj, query)
        except exceptions.CosmosHttpResponseError as e:
            print('\nrun_sample has caught an error. {0}'.format(e.message))
        finally:
            print("\nQuickstart complete")
            
# <run_sample>
async def read_all_store_info():
    # <create_cosmos_client>
    async with cosmos_client(endpoint, credential = key) as client:
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
            
# 특정 css 찾을때 까지 대기
def time_wait(num, code):
    try:
        wait = WebDriverWait(driver, num).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, code)))
    except:
        print(code, '태그를 찾지 못하였습니다.')
        driver.quit()
    return wait     

# frame 변경 메소드
def switch_frame(frame):
    driver.switch_to.default_content()  # frame 초기화
    driver.switch_to.frame(frame)  # frame 변경
    res
    soup       
    
# 페이지 다운
def page_down(num):
    for i in range(5):
        rencentList = driver.find_elements(By.CLASS_NAME, 'CHC5F')
        for list in rencentList:
            driver.execute_script("arguments[0].scrollIntoView()", list)
    
        
def add_urls(store_list, store_info_list):
    blog_url_list = []
    for store in store_list:
        switch_frame('searchIframe')
        sleep(1)
        page = store.find_element(By.CSS_SELECTOR, 'div.CHC5F > a > div > div > span.place_bluelink.TYaxT')
        page.click()
        sleep(2)
        try:
            # 상세 페이지로 이동
            switch_frame('entryIframe')
            # time_wait(5, '._3XamX')
            # 스크롤을 맨밑으로 1초간격으로 내린다.
            for down in range(5):
                sleep(1)
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            # -----매장명 가져오기-----
            store_name = driver.find_element(By.CLASS_NAME,'Fc1rA').text
            print(store_name) 
            button = None
            try:
                keyword_list = driver.find_elements(By.CLASS_NAME, "lfH3O")
                for keyword in keyword_list:
                    if keyword.text == '블로그리뷰더보기':
                        button = keyword
                        break
            except:
                print("블로그리뷰 더보기 버튼 없음")

            if button == None:
                blog_list = driver.find_elements(By.CLASS_NAME, 'pcPAT')
                print("총블로그 개수 {}".format(len(blog_list)))
                for blog in blog_list:
                    blog_url = blog.find_element(By.TAG_NAME, 'a')
                    href = blog_url.get_attribute("href")
                    print(href)
                    blog_url_list.append(href)
                                    
            else:
                button = keyword.find_element(By.TAG_NAME, 'a')
                button.send_keys(Keys.ENTER)
                sleep(5)
                while True:
                    try:
                        blog_expand = driver.find_element(By.CLASS_NAME,'lfH3O')
                        if blog_expand is None:
                            break
                        button = blog_expand.find_element(By.TAG_NAME, 'a')
                        button.send_keys(Keys.ENTER)
                        sleep(1)
                    except:
                        break
                
                blog_list = driver.find_elements(By.CSS_SELECTOR, '#app-root > div > div > div > div:nth-child(7) > div:nth-child(2) > div.place_section.no_margin.UvmYE > div > ul >li')
                print("총블로그 개수 {}".format(len(blog_list)))
                for blog in blog_list:
                    blog_url = blog.find_element(By.TAG_NAME, 'a')
                    href = blog_url.get_attribute("href")
                    print(href)
                    blog_url_list.append(href)
        
            print(f'{store_name} ...완료')
            
            store_item = {
                "id" : store_name,
                "urls" : blog_url_list    
            }
            store_info_list.append(store_item)
            
        except:
            print('ERROR!' * 3)            

if __name__=="__main__":
    url = 'https://map.naver.com/v5/search'
    driver = webdriver.Chrome(executable_path=r'C:\Users\won\chrome\chromedriver.exe') # 웹드라이버가 설치된 경로를 지정해주시면 됩니다.
    driver.get(url)
    # 검색어 입력
    key_word = '한티역 맛집'

    # css를 찾을때 까지 30초 대기
    time_wait(30, 'div.input_box > input.input_search')

    # 검색창 찾기
    search = driver.find_element(By.CSS_SELECTOR, 'div.input_box > input.input_search')
    search.send_keys(key_word)  # 검색어 입력
    search.send_keys(Keys.ENTER)  # 엔터버튼 누르기

    res = driver.page_source  # 페이지 소스 가져오기
    soup = BeautifulSoup(res, 'html.parser')  # html 파싱하여  가져온다

    sleep(1)
    store_info_list = []
    # 시작시간
    start = time.time()
    print('[크롤링 시작...]')

    while True:
        switch_frame('searchIframe')
        page_down(40)
        sleep(3)
        next_btn = driver.find_elements(By.CSS_SELECTOR, '#app-root > div > div.XUrfU > div.zRM9F > a')
        btn_act = driver.find_elements(By.CLASS_NAME, 'mBN2s.qxokY')
        
        store_list = driver.find_elements(By.CSS_SELECTOR, '#_pcmap_list_scroll_container > ul > li')
        store_list_add = driver.find_elements(By.CSS_SELECTOR, '#_pcmap_list_scroll_container > ul > div > li')
        print('총 {}개의 가게 검색'.format(len(store_list+store_list_add)))
        blog_url_list = []
        add_urls(store_list, store_info_list)
        add_urls(store_list_add, store_info_list)
        
        switch_frame('searchIframe')
        sleep(5)
        if next_btn is None:
            break
        if len(btn_act) != 1:
            break
        if next_btn[-2].text == btn_act[0].text:
            break
        else:
            next_btn[-1].click()
        sleep(2)

    print('[데이터 수집 완료]\n소요 시간 :', time.time() - start)
    driver.quit()  # 작업이 끝나면 창을닫는다.

    os.makedirs('data', exist_ok = True)
    # json 파일로 저장

    loop = asyncio.get_event_loop()
    loop.run_until_complete(insert_store_info(store_info_list))

    with open('data/store_data.json', 'w', encoding='utf-8') as f:
        json.dump(store_info_list, f, indent=4, ensure_ascii=False)
        
