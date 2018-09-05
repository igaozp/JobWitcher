# -*- coding: utf-8 -*-
__author__ = 'igaozp'

from scrapy import Spider, Request
from bs4 import BeautifulSoup
from ..items import BosszhipinItem
import redis
import datetime
import logging
import requests

logger = logging.getLogger(__name__)


class BosszhipinSpider(Spider):
    name = "bosszhipin_spider"

    allowed_domains = ['www.zhipin.com']
    start_urls = ['https://www.zhipin.com/']

    custom_settings = {
        'DEFAULT_REQUEST_HEADERS': {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7,ja;q=0.6',
            'Cookie': 'lastCity=101010100; t=fha1PDLcWhx0Xols; wt=fha1PDLcWhx0Xols; JSESSIONID=""; __c=1534643708; '
                      '__g=-; __l=l=%2Fwww.zhipin.com%2Fc101280100-p100102%2F&r=; __a=86675520.1530673898.1534557985.'
                      '1534643708.52.8.3.52',
            'Referer': 'https://www.zhipin.com/c101030100-p100102/',
            'dnt': '1',
            'token': 'mP5W8tA0aoDmgPs',
            'x-requested-with': 'XMLHttpRequest',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/66.0.3359.181 Safari/537.36'
        }
    }

    def __init__(self, **kwargs):
        """
        初始化 Redis 连接
        :param kwargs: 传递参数
        """
        super().__init__(**kwargs)
        self.logger.info('初始化 Redis 连接')
        # self.redis_pool = redis.ConnectionPool(host='127.0.0.1', port=6379, password='', db=11)
        self.redis_pool = redis.ConnectionPool(host='123.206.53.233', port=6378, password='', db=11)
        self.redis_db = redis.Redis(connection_pool=self.redis_pool)

    def start_requests(self):
        """
        从全国主要城市获取相关专业的职位
        :return: 请求
        """
        # 获取计算机相关专业的职位类别
        response = requests.get('https://www.zhipin.com/')
        soup = BeautifulSoup(response.text, 'lxml')
        # 工程师相关职位
        engineer = soup.select('#main > div > div.home-sider > div > dl:nth-of-type(1) > '
                               'div.menu-sub > ul > li > div > a')
        # 产品经理相关职位
        project_manager = soup.select('#main > div > div.home-sider > div > dl:nth-of-type(2) > '
                                      'div.menu-sub > ul > li > div > a')
        job_list = engineer + project_manager

        # 获取相关职位的 URL
        job_url_list = []
        for job in job_list:
            job_url_list.append(job.get('href'))

        # 从 URL 中提取职位的代号
        job_type_list = []
        for url in job_url_list:
            url = url.replace('/', '')
            index = url.find('-') + 1
            job_type = url[index:]
            job_type_list.append(job_type)

        # 全国主要城市列表
        city_list = [
            'c101010100',  # 北京
            'c101020100',  # 上海
            'c101280100',  # 广州
            'c101280600',  # 深圳
            'c101210100',  # 杭州
            'c101030100',  # 天津
            'c101110100',  # 西安
            'c101190400',  # 苏州
            'c101222100',  # 武汉
            'c101230200',  # 厦门
            'c101250100',  # 长沙
            'c101270100',  # 成都
            'c101180100',  # 郑州
            'c101120100',  # 济南
            'c101120200',  # 青岛
            'c101070200',  # 大连
            'c101190100',  # 南京
            'c101220100',  # 合肥
            'c101230100',  # 福州
            'c101040100',  # 重庆
        ]

        # 通过城市代号和职位代号构造 URL
        urls = []
        for job_type in job_type_list:
            for city in city_list:
                url = 'https://www.zhipin.com/' + city + '-' + job_type + '/'
                urls.append(url)

        # 对所有的职位 URL 发送请求
        for url in urls:
            yield Request(url=url, callback=self.parse)

    def parse(self, response):
        """
        获取职位的基本信息
        :param response: 页面响应
        :return: 请求
        """
        soup = BeautifulSoup(response.text, 'lxml')

        job_name_list = soup.select('div.job-title')
        salary_list = soup.select('#main > div > div.job-list > ul > li > div > div.info-primary > h3 > a > span')
        company_name_list = soup.select('#main > div > div.job-list > ul > li > div > div.info-company > div > h3 > a')
        public_date_list = soup.select('#main > div > div.job-list > ul > li > div > div.info-publis > p')
        url_list = soup.select('#main > div > div.job-list > ul > li > div > div.info-primary > h3 > a')

        for job_name, salary, company_name, public_date, url in zip(job_name_list,
                                                                    salary_list,
                                                                    company_name_list,
                                                                    public_date_list, url_list):
            # 获取 URL 并补全
            url = url.get('href')
            if url.find('http') is -1:
                url = 'https://www.zhipin.com' + url
            # 检查页面是否重复
            if self.redis_db.hexists('bosszhipin_url', url):
                self.logger.info('重复的数据: ' + url)
                continue

            item = BosszhipinItem()
            item['job_name'] = job_name.get_text().strip()
            item['salary'] = salary.get_text().strip()
            item['company_name'] = company_name.get_text().strip()
            item['public_date'] = public_date.get_text().strip()[3:]
            item['url'] = url
            item['crawled_date'] = datetime.date.today()

            # 获取职位的详细信息
            yield Request(url=url, meta={"item": item}, callback=self.parse_content, dont_filter=True)

        # 获取下一页信息
        next_page = soup.select('#main > div > div.job-list > div.page > a.next')
        if next_page is not None and len(next_page) is not 0:
            url = next_page[0].get('href')
            if url.find('http') is -1:
                url = 'https://www.zhipin.com' + url
            logger.info('Next page: ' + url)
            yield Request(url=url, callback=self.parse)

    @staticmethod
    def parse_content(response):
        """
        获取职位的详细信息
        :param response: 页面响应
        :return: item 对象
        """
        soup = BeautifulSoup(response.body, 'lxml')
        item = response.meta['item']

        # 职位的描述信息
        description = soup.select('#main > div.job-box > div > div.job-detail > div.detail-content > '
                                  'div:nth-of-type(1) > div')[0].get_text().strip()
        # 职位所在城市、学历、经验要求
        mix_info = soup.select('#main > div.job-banner > div > div > div.info-primary > p')[0].get_text().strip()
        # 地址
        address = soup.select('#main > div.job-box > div > div.job-detail > div.detail-content > div > '
                              'div > div.location-address')[0].get_text().strip()

        # 从字符串中提取城市、学历、经验信息
        first = mix_info.find('：')
        mid = mix_info.find('：', first + 1, len(mix_info))
        last = mix_info.find('：', mid + 1, len(mix_info))
        experience_index = mix_info.find('经验')
        education_index = mix_info.find('学历')
        city = mix_info[first + 1:experience_index]
        experience = mix_info[mid + 1:education_index]
        education = mix_info[last + 1:]

        item['description'] = description
        item['address'] = address
        item['city'] = city
        item['experience'] = experience
        item['education'] = education

        yield item
