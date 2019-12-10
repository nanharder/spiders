import re
import requests
from bs4 import BeautifulSoup
from opencc import OpenCC
import pymysql
from datetime import datetime
import time
import configs


def get_page(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) ' +
                      'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.100 Safari/537.36',
        'Cookie': '**'
    }

    return requests.get(url, headers=headers)


def get_data(page):
    datasets = []
    try:
        text = page.text.encode('iso-8859-1').decode('GBK')
        soup = BeautifulSoup(text, 'lxml')
        posts = soup.find_all(class_='forumbox postbox')
        for post in posts:
            data = {}
            # 作者id，发帖次数，最后发帖时间
            data['uid'] = post.find(class_='author')['href'].split('=')[-1]
            id = post.find(class_='author')['id'][10:]
            data['posttime'] = datetime.strptime(post.find(id="postdate" + id).text, "%Y-%m-%d %H:%M")
            #发帖次数暂定为1,查询后再进行修改
            data['postcount'] = 1
            datasets.append(data)

            # 得到帖子内容
            content = post.find(id='postcontent' + id).text.strip()
            content = re.sub(u'\\[quote\\].*?\\[/quote\\]', '', content)
            content = re.sub(u'\\[b\\].*?\\[/b\\]', '', content)
            content = re.sub(u'\\[img\\].*?\\[/img\\]', '', content)
            content = re.sub(u'\\[url\\].*?\\[/url\\]', '', content)
            content = re.sub(u'\\[size.*?/size\\]', '', content)
            content = re.sub(u'\\[s:.*?\\]', '', content)
            content = re.sub(u'\\[.*?del\\]', '', content)
            content = re.sub(u'\\[.*?list\\]', '', content)
            content = re.sub(u'\\[.*?collapse\\]', '', content)
            if len(content) > 0:
                cc = OpenCC('t2s')
                content = cc.convert(content)
                save_content(content)
    except Exception as e:
        print("出现异常，错误为:%s" % e)
    return datasets


def save_content(content):
    with open(config['filepath'], 'a+', encoding='utf-8') as file:
        file.write(content)
        file.write('\n')


def save_data(data):
    db = pymysql.connect(host='localhost', user=config['user'], password=config['password'], port=3306, db='spiders')
    cursor = db.cursor()

    table = config['table']
    keys = ', '.join(data.keys())
    values = ', '.join(['%s'] * len(data))
    select_sql = 'SELECT * FROM {table} where uid = {id} limit 1'.format(table=table, id=data['uid'])
    try:
        if cursor.execute(select_sql):
            # 更新数据
            old_data = cursor.fetchone()
            last_time = old_data[1]
            if last_time > data['posttime']:
                data['posttime'] = last_time
            data['postcount'] = old_data[2] + 1
            sql = 'UPDATE {table} SET posttime={value}, postcount={value} where uid={uid}'.format(table=table, uid=data['uid'], value='%s')
            if cursor.execute(sql, (data['posttime'], data['postcount'])):
                print("Update Successful")
                db.commit()
        else:
            sql = 'INSERT INTO {table}({keys}) VALUES ({values})'.format(table=table, keys=keys, values=values)
            if cursor.execute(sql, tuple(data.values())):
                print("Insert Successful")
                db.commit()
    except Exception as e:
        print(e)
        db.rollback()
    db.close()


def engine(base_url):

    base_page = get_page(base_url)
    text = base_page.text.encode('iso-8859-1').decode('GBK')
    base_soup = BeautifulSoup(text, 'lxml')
    max_page = int(re.findall(u'.*?,1:(\d+),.*?', base_soup.find_all(class_='left')[0].script.text)[0])
    print('共需爬取%d页' % max_page)
    for i in range(1, max_page + 1):
        page = get_page(base_url + '&page=' + str(i))
        datasets = get_data(page)
        '''for data in datasets:
            save_data(data)'''
        print('第%d页已完成' % i)
        time.sleep(2)
    print('任务已完成')


if __name__ == '__main__':
    config = configs.spider_config
    base_url = config['base_url']
    engine(base_url)




