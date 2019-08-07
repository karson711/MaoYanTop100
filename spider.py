import json
import requests
import re
import pymongo
import os
from hashlib import md5
from multiprocessing import Pool
from requests.exceptions import RequestException

from config import *

client = pymongo.MongoClient(MONG_URL)
db = client[MONG_DB]


def get_one_page(url):
    try:
        response = requests.get(url)
        if response.status_code == 200:
            return response.text
        return None
    except RequestException:
        return None

def parse_one_page(html):
    # +'.*?interger">(.*?)</i>.*?fraction">(.*?)</i>.*?</dd>'
    pattern = re.compile('<dd>.*?board-index.*?>(\d+)</i>.*?data-src="(.*?)".*?name"><a.*?>(.*?)</a>.*?star">(.*?)</p>.*?releasetime">(.*?)</p>',re.S)
    items = re.findall(pattern,html)
    for item in items:
        # print('item',item)
        download_image(item[1])
        yield {
            'index': item[0],
            'image': item[1],
            'title': item[2],
            'actor': item[3].strip()[3:],
            'time': item[4].strip()[5:]
            # 'score': item[5]+item[6]
        }

def write_to_file(content):
    with open('result.txt','a',encoding='utf-8') as f:
        f.write(json.dumps(content,ensure_ascii=False) + '\n')
        f.close()

def save_to_mongo(result):
    if db[MONG_TABLE].insert(result):
        print('存储到MongoDB成功',result)
        return True
    return False

def download_image(url):
    print('正在下载',url)
    try:
        response = requests.get(url)
        if response.status_code == 200:
            save_image(response.content)
        return None
    except RequestException:
        return None

def save_image(content):
    file_path = '{0}/{1}.{2}'.format('downLoadImages',md5(content).hexdigest(),'jpg')
    if not os.path.exists(file_path):
        with open(file_path,'wb') as f:
            f.write(content)
            f.close()


def main(offset):
    url = 'https://maoyan.com/board/4?offset=' + str(offset)
    html = get_one_page(url)
    for item in parse_one_page(html):
        print(item)

        write_to_file(item)
        save_to_mongo(item)

if __name__ == '__main__':
    pool = Pool()
    pool.map(main,[i*10 for i in range(10)])



