# utf-8
import re
import requests
from bs4 import BeautifulSoup
import pymysql
import time


def get_page(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) ' +
                      'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.100 Safari/537.36'
    }

    page = requests.get(url, headers=headers)
    return page


def get_info(page, root_url):
    soup = BeautifulSoup(page.text, 'lxml')
    houses = soup.find_all(class_="content__list--item")
    datasets = []
    for house in houses:
        data = {}
        try:

            data['detail_url'] = root_url + house.a['href']
            data['housetype'] = house.a['title'].split('·')[0]
            data['price'] = int(house.find(class_="content__list--item-price").em.text)

            desc = house.find(class_="content__list--item--des")
            data['distract'] = desc.find_all(name="a")[0].text
            data['block'] = desc.find_all(name="a")[1].text
            data['community'] = desc.find_all(name="a")[2].text

            data['area'] = int(desc.text.split('\n')[3].strip()[:-1])
            data['direction'] = desc.text.split('\n')[4].strip()[1:-1].strip()

            nums = re.findall('\d', desc.text.split('\n')[5].strip())
            data['numbedroom'] = int(nums[0])
            data['numlivingroom'] = int(nums[1]) if len(nums) >= 2 else 0
            data['numbathroom'] = int(nums[2]) if len(nums) >= 3 else 0

            data['height'] = desc.text.split('\n')[7].split('（')[0].strip()
            data['numfloors'] = int(desc.text.split('\n')[7].split('（')[1].strip()[:-2])
            datasets.append(data)
        except Exception as e:
            print(e)
    return datasets


def save_data(data):
    db = pymysql.connect(host='localhost', user='nanhang', password='hang2212', port=3306, db='spiders')
    cursor = db.cursor()

    table = 'hefeihouse'
    keys = ', '.join(data.keys())
    values = ', '.join(['%s'] * len(data))
    sql = 'INSERT INTO {table}({keys}) VALUES ({values})'.format(table=table, keys=keys, values=values)

    try:
        if cursor.execute(sql, tuple(data.values())):
            print("Successful")
            db.commit()
    except Exception as e:
        print('Failed')
        db.rollback()
    db.close()



def engine(base_url):
    base_page = get_page(base_url)
    soup = BeautifulSoup(base_page.text, 'lxml')
    amounts = int(soup.find(class_='content__title--hl').text)
    max_page_num = min(amounts // 30, 100)
    root_url = base_url[:-8]
    for i in range(22, max_page_num):
        cur_url = base_url + "pg" + str(i)
        page = get_page(cur_url)
        datasets = get_info(page, root_url)
        for data in datasets:
            save_data(data)
        print('第%d页已完成' % i)
        time.sleep(1)

    print("所有任务已完成")


if __name__ == '__main__':
    base_url = "https://hf.lianjia.com/zufang/"
    engine(base_url)
