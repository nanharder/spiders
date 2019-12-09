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
        'Cookie': 'UM_distinctid=16e8687798e55a-0f9d7193ddd21f-30760d58-13c680-16e8687798f6ca; '
                  'taihe_bi_sdk_uid=afbc60f849ee2ecaed63bcc19c5e336f; '
                  'taihe_bi_sdk_session=71cfee589680a1787f47e5d6bcce9802; PHPSESSID=jrupa42mg195qfgrk3fcctc1b0; '
                  'ngacn0comUserInfo=%25C4%25B3%25C4%25EA%25C4%25B3%25D4%25C2234%09%25E6%259F%2590%25E5%25B9%25B4'
                  '%25E6%259F%2590%25E6%259C%2588234%0939%0939%09%0910%090%094%090%090%09; ngaPassportUid=60471380; '
                  'ngaPassportUrlencodedUname=%25C4%25B3%25C4%25EA%25C4%25B3%25D4%25C2234; '
                  'ngaPassportCid=X8q18e9uv15km98ahrmc74tahkmpr5geho5a1itd; '
                  'ngacn0comUserInfoCheck=22f20819c81c49e30aaa89ba920c28c2; ngacn0comInfoCheckTime=1574339249; '
                  'lastpath=/read.php?tid=16227300&_fp=6&page=571; lastvisit=1574409260; '
                  'bbsmisccookies=%7B%22uisetting%22%3A%7B0%3A1%2C1%3A1574820355%7D%2C%22pv_count_for_insad%22%3A%7B0'
                  '%3A-38%2C1%3A1574442074%7D%2C%22insad_views%22%3A%7B0%3A1%2C1%3A1574442074%7D%7D; '
                  'CNZZDATA30043604=cnzz_eid%3D348822524-1574214750-https%253A%252F%252Fwww.google.com%252F%26ntime'
                  '%3D1574409150; _cnzz_CV30043604=forum%7Cfid-60204499%7C0 '
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




