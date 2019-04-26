import random
import time
from concurrent.futures import ThreadPoolExecutor

import requests

import pymongo
import redis
from RandomHeader import randHeader


class Bilibili():
    def __init__(self):
        # self._conn_redis = redis.Redis(host='127.0.0.1', port=6379, db=1)
        self._conn_mongo = pymongo.MongoClient('mongodb://localhost:27017/')['Bilibili']['video_data']

    def get_proxy(self):
        return requests.get("http://127.0.0.1:5010/get/").text

    def delete_proxy(self, proxy, url):
        requests.get("http://127.0.0.1:5010/delete/?proxy={}".format(proxy))
        print ('delete {}'.format(proxy))
        # self.get_data(url)

    def get_data(self, url):
        retry_count = 5
        proxy = self.get_proxy()
        header = randHeader()
        time.sleep(random.uniform(1, 2))
        # a ={'{}'.format(proxy[:proxy.index(':')]): '{}'.format(proxy)}
        while retry_count > 0:
            try:
                req = requests.get(url=url, headers=header, timeout=2, proxies={'http': 'http://{}'.format(proxy)})
                json_data = req.json()
                if json_data.get('code') != 0 or json_data.get('code'):
                    print ("{}:什么也没有".format(url))
                    return
                json_data = json_data.get('data')
                stat = json_data.get('stat')
                video_data = {
                # '_id': hash(json_data.get('aid')),
                    '_id': json_data.get('aid'),
                    'aid': json_data.get('aid'),
                    'videos': json_data.get('videos'),
                    'tname': json_data.get('tname'),
                    'pic': json_data.get('pic'),
                    'tile': json_data.get('title'),
                    'pubdate': json_data.get('pubdate'), #发布时间
                    'ctiem': json_data.get('ctime'), #不知道
                    'desc': json_data.get('desc'), #简介
                    # 'mid': json_data.get('owner')['mid'] #作者ID
                    # 'name': json_data.get('owner')['name'] # 作者名字
                    # 'face': json_data/get('owenr')['face'] #头像
                    'author_data': json_data.get('owner'), #作者信息
                    'view': stat.get('view'), # 播放数
                    'danmaku': stat.get('danmaku'), # 弹幕数
                    'reply': stat.get('reply'), # 评论数 
                    'favorite': stat.get('favorite'), #收藏数
                    'coin': stat.get('coin'), # 硬币数
                    'share': stat.get('share') # 分享
                    }
                # print (video_data)
                self._conn_mongo.insert_one(video_data)
                # del video_data
                return 
                # return req
            except Exception as e:
                print (e)
                retry_count -= 1
        self.delete_proxy(proxy, url)


if __name__ == "__main__":
    Bili = Bilibili()
    executor = ThreadPoolExecutor(max_workers=8)
    url = 'https://api.bilibili.com/x/web-interface/view?aid={}' #40000000
    for aid in range(860, 10000000):
        executor.submit(Bili.get_data, url.format(aid))

    # Bili.get_data(url.format(89238))
     
