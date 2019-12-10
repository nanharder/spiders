# utf-8
import re
import requests
from bs4 import BeautifulSoup
import pymysql
import time


def get_urls(base_url, root_url):
    base_page = get_page(base_url)
    soup = BeautifulSoup(base_page.text, 'lxml')
    distracts = soup.find(attrs={'data-role': 'ershoufang'}).find_all(name='a')
    return [root_url + distract['href'] for distract in distracts]


def get_page(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) ' +
                      'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.100 Safari/537.36',
        'Connection': 'close'
    }

    page = requests.get(url, headers=headers)
    return page


def get_info(page):
    soup = BeautifulSoup(page.text, 'lxml')
    houses = soup.find_all(class_="info clear")
    sel_distract = soup.find(attrs={'data-role': 'ershoufang'}).find(name='a', class_='selected').text
    datasets = []
    for house in houses:
        data = {}
        try:
            data['detail_url'] = house.find(class_='title').a['href']
            data['description'] = house.find(class_='title').a.text

            data['distract'] = sel_distract
            data['block'] = house.find(class_='positionInfo').find_all(name='a')[1].text
            data['community'] = house.find(class_='positionInfo').find_all(name='a')[0].text

            house_info = house.find(class_='houseInfo').text.split('|')
            data['area'] = float(house_info[1].strip()[:-2])
            data['direction'] = house_info[2].strip()
            data['decoration'] = house_info[3].strip()

            nums = re.findall('\d+', house_info[0])
            data['numbedroom'] = int(nums[0])
            data['numlivingroom'] = int(nums[1])

            data['height'] = house_info[4].strip()[:3]
            data['buildyear'] = int(re.findall('\d+', house_info[5])[0]) if (len(house_info[5].strip()) > 0) else -1
            data['housetype'] = house_info[6].strip()

            data['totalprice'] = float(house.find(class_='priceInfo').find(class_='totalPrice').text[:-1])
            data['unitprice'] = int(house.find(class_='priceInfo').find(class_='unitPrice')['data-price'])
            datasets.append(data)
        except Exception as e:
            print(e)
    return datasets


def save_data(data):
    db = pymysql.connect(host='localhost', user='root', password='password', port=3306, db='spiders')
    cursor = db.cursor()

    table = 'hefeisecond'
    keys = ', '.join(data.keys())
    values = ', '.join(['%s'] * len(data))
    sql = 'INSERT INTO {table}({keys}) VALUES ({values})'.format(table=table, keys=keys, values=values)

    try:
        if cursor.execute(sql, tuple(data.values())):
            db.commit()
    except Exception as e:
        print('Failed')
        db.rollback()
    db.close()


def engine(base_url):
    root_url = base_url[:-12]
    distracts = get_urls(base_url, root_url)
    house_types = ['l' + str(i) for i in range(1, 7)]
    for distract in distracts:
        for house_type in house_types:
            soup = BeautifulSoup(get_page(distract + house_type).text, 'lxml')
            page_num = soup.find(class_='page-box house-lst-page-box')
            if page_num is None:
                continue
            else:
                total_page = int(re.findall(u'.*?"totalPage":(\d+),.*?', page_num['page-data'])[0])

            for i in range(1, total_page + 1):
                cur_url = distract + "pg" + str(i) + house_type
                page = get_page(cur_url)
                datasets = get_info(page)
                for data in datasets:
                    save_data(data)
                print('%s已完成' % cur_url)
                time.sleep(2)

    print("所有任务已完成")


if __name__ == '__main__':
    base_url = "https://hf.lianjia.com/ershoufang/"
    engine(base_url)
