import os
import re
import time
import json
from hashlib import md5
from concurrent import futures
from urllib.parse import urlencode
import pymongo
import requests
from logger import logger


# 连接MongoDB
client = pymongo.MongoClient(host='localhost', port=27017)
db = client.toutiao
collection = db.jiepai

# 设置图片下载后的保存基目录
basedir = os.path.abspath(os.path.dirname(__file__))  # 当前模块文件的根目录
down_path = os.path.join(basedir, 'downloads')
if not os.path.exists(down_path):
    os.mkdir(down_path)
    logger.info('Create base directory [%s]', down_path)


def get_albums(offset):
    '''获取今日头条的街拍图集
    :param offset: 获取多少个图集，默认是20个
    '''
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36'
    }
    params = {
        'offset': offset,
        'format': 'json',
        'keyword': '街拍',
        'autoload': 'true',
        'count': 20,  # 默认加载一页是20个图集
        'cur_tab': 3,  # '图集'页签。默认是1，是'综合'页签，不全是图集还有文章
        'from': 'search_tab'
    }
    url = 'http://www.toutiao.com/search_content/?' + urlencode(params)  # 用参数构造请求的URL

    # 捕获request.get方法的异常，比如连接超时、被拒绝等
    try:
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
    except requests.exceptions.HTTPError as errh:
        # In the event of the rare invalid HTTP response, Requests will raise an HTTPError exception (e.g. 401 Unauthorized)
        logger.error('HTTP Error: %s', errh)
        return
    except requests.exceptions.ConnectionError as errc:
        # In the event of a network problem (e.g. DNS failure, refused connection, etc)
        logger.error('Connecting Error: %s', errc)
        return
    except requests.exceptions.Timeout as errt:
        # If a request times out, a Timeout exception is raised. Maybe set up for a retry, or continue in a retry loop
        logger.error('Timeout Error: %s', errt)
        return
    except requests.exceptions.TooManyRedirects as errr:
        # If a request exceeds the configured number of maximum redirections, a TooManyRedirects exception is raised. Tell the user their URL was bad and try a different one
        logger.error('Redirect Error: %s', errr)
        return
    except requests.exceptions.RequestException as err:
        # catastrophic error. bail.
        logger.error('Else Error: %s', err)
        return

    # 默认获取一页加载的20个图集：标题和URL
    albums_list = []
    dict_obj = resp.json()  # 将响应的JSON数据转换为Python字典
    if dict_obj.get('data'):  # 默认resp中包含20个图集，存放在data字段下面
        for album in dict_obj.get('data'):
            # 偶尔有不是图集的需要排除，所以每页不一定是20个
            if album.get('article_url') and re.match(r'http://toutiao\.com/group/\d+?/', album.get('article_url')):
                album_title = album.get('title')  # 每个图集的标题
                album_url = album.get('article_url')  # 每个图集的URL
                album_date = album.get('datetime').split()[0]  # 每个图集的发布日期，类似'2018-06-24'
                album_author = album.get('media_name', '佚名')  # 每个图集的发布者

                album_dict = {
                    'album_title': album_title,
                    'album_url': album_url,
                    'album_date': album_date,
                    'album_author': album_author
                }
                albums_list.append(album_dict)

    return albums_list


def get_images(album):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36'
    }
    # TODO: 捕获request.get方法的异常，比如连接超时、被拒绝等
    resp = requests.get(album['album_url'], headers=headers)
    # 图集下面所有的原始大图URL都放在JavsScript代码片段 gallay: JSON.parse() 下面
    images_pattern = re.compile('gallery: JSON.parse\("(.*)"\)', re.S)
    m = re.search(images_pattern, resp.text)
    if m:  # 如果正则匹配到，匹配结果存储在第1个分组中：m.group(1)
        dict_obj = json.loads(m.group(1).replace('\\', ''))  # 去除匹配结果中的\\，自己用chrome看gallay: JSON.parse()具体是什么
        if dict_obj and 'sub_images' in dict_obj.keys():
            sub_images = dict_obj.get('sub_images')
            images = [item.get('url') for item in sub_images]  # 列表生成式，真正的图片URL列表
            data = {
                'album_title': album['album_title'],  # 图集标题
                'album_url': album['album_url'],  # 图集URL
                'album_date': album['album_date'],  # 图集发布日期
                'album_author': album['album_author'],  # 图集发布者
                'images': images  # 图集下面所有图片URL列表 图片URL类似于http://p1.pstatp.com/origin/pgc-image/15298463258670adeacc647
            }
            # 保存到MongoDB
            if not collection.find_one(data):
                if collection.insert_one(data):
                    logger.debug('Successfully saved gallery {} [{}] to MongoDB'.format(album['album_title'], album['album_url']))
                else:
                    logger.debug('Error saving gallery {} [{}] to MongoDB'.format(album['album_title'], album['album_url']))
            else:  # 如果该条记录已存在，则不存储到数据库
                logger.debug('Gallery {} [{}] has exist in MongoDB'.format(album['album_title'], album['album_url']))

            # 按日期创建目录
            date_dir = os.path.join(down_path, album['album_date'])
            if not os.path.exists(date_dir):
                os.mkdir(date_dir)
                logger.info('Create date directory [%s]', date_dir)

            # 为每个图集创建一个目录
            album_dir = os.path.join(date_dir, '[{}P] '.format(len(images)) + re.sub('[\/:*?"<>|]', '_', album['album_title']))  # 注意要去除标题的非法字符
            if not os.path.exists(album_dir):
                os.mkdir(album_dir)
                logger.info('Create gallery directory [%s]', album_dir)

            # 如果图集目录下已保存的图片数等于images列表的长度，说明整个图集已下载过，跳过本次下载
            if os.path.exists(album_dir) and len([img for img in os.listdir(album_dir) if os.path.isfile(os.path.join(album_dir, img))]) == len(images):
                logger.info('All images of gallery {} [{}] has exist, ignore download'.format(album['album_title'], album['album_url']))
                return

            # 否则，依次下载每一张图片（同步阻塞）
            for image in images:
                # TODO: 捕获request.get方法的异常，比如连接超时、被拒绝等
                resp = requests.get(image, headers=headers)
                # 以图片内容的hash值当作文件名，可以保证不重复下载。这样以后重新运行下载脚本时，如果发现本地已经保存了同一图片就不下载
                image_name = md5(resp.content).hexdigest() + '.jpg'  # 因为图片没有后缀名，所以简单指定为.jpg。 如果有后缀，要用os.path.splitext切割获取后缀名
                image_path = os.path.join(album_dir, image_name)  # 图片的保存路径
                if not os.path.exists(image_path):
                    with open(image_path, 'wb') as f:
                        f.write(resp.content)
                else:
                    logger.info('Image [{}] of gallery {} [{}] has exist, ignore download'.format(image, album['album_title'], album['album_url']))
    else:
        logger.debug('Can not find images in gallery {} [{}]'.format(album['album_title'], album['album_url']))


def download_many():
    '''多线程下载，每个线程下载一个图集'''
    pages = 3
    album_count = 0  # 统计一共下载了多少个图集，因为有的页会有不合规的会排除，并不是每页20个图集

    for offset in [x * 20 for x in range(pages)]:  # 排除一些不合规的图集URL，并不是每页20个图集
        albums = get_albums(offset)  # 返回包含1页中的所有图集列表
        album_count += len(albums)

        workers = 10
        with futures.ThreadPoolExecutor(workers) as executor:
            executor.map(get_images, albums)

    return album_count


if __name__ == '__main__':
    """
    t0 = time.time()
    count = download_many()
    msg = '{} albums downloaded in {} seconds.'
    logger.info(msg.format(count, time.time() - t0))
    """
    for offset in [x * 20 for x in range(3)]:  # 排除一些不合规的图集URL，并不是每页20个图集
        albums = get_albums(offset)
        for album in albums:
            print(album)
            get_images(album)