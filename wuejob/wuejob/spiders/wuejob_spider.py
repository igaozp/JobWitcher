# -*- coding: utf-8 -*-
__author__ = 'igaozp'

from scrapy import Spider, Request
from bs4 import BeautifulSoup
from ..items import WuejobItem
import datetime
import logging
import redis

logger = logging.getLogger(__name__)


class WueJobSpider(Spider):
    """
    51Job 爬虫
    """
    name = 'wuejob_spider'

    allowed_domains = ['www.51job.com']
    start_urls = ['https://www.51job.com/']
    agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 ' \
            '(KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36'

    custom_settings = {
        'COOKIES_ENABLE': False,
        'DEFAULT_REQUEST_HEADERS': {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7,ja;q=0.6',
            'Connection': 'keep-alive',
            'Cookie': 'nsearch=jobarea%3D%26%7C%26ord_field%3D%26%7C%26recentSearch0%3D%26%7C%26recentSearch'
                      '1%3D%26%7C%26recentSearch2%3D%26%7C%26recentSearch3%3D%26%7C%26recentSearch4%3D%26%7C%'
                      '26collapse_expansion%3D; guid=15271618515649520040; slife=lowbrowser%3Dnot%26%7C%26; '
                      '51job=cenglish%3D0%26%7C%26; search=jobarea%7E%60060000%7C%21ord_field%7E%600%7C%21recent'
                      'Search0%7E%601%A1%FB%A1%FA060000%2C00%A1%FB%A1%FA000000%A1%FB%A1%FA0000%A1%FB%A1%FA00%A1'
                      '%FB%A1%FA9%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1'
                      '%FA99%A1%FB%A1%FA%A1%FB%A1%FA2%A1%FB%A1%FA%A1%FB%A1%FA-1%A1%FB%A1%FA1531879817%A1%FB%A1%FA'
                      '0%A1%FB%A1%FA%A1%FB%A1%FA%7C%21recentSearch1%7E%601%A1%FB%A1%FA060000%2C00%A1%FB%A1%FA000'
                      '000%A1%FB%A1%FA0000%A1%FB%A1%FA39%A1%FB%A1%FA9%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA99%A'
                      '1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA%A1%FB%A1%FA2%A1%FB%A1%FA%A1%FB%A1%FA-1'
                      '%A1%FB%A1%FA1531827324%A1%FB%A1%FA0%A1%FB%A1%FA%A1%FB%A1%FA%7C%21recentSearch2%7E%601%A1%FB%'
                      'A1%FA020000%2C00%A1%FB%A1%FA000000%A1%FB%A1%FA0000%A1%FB%A1%FA35%A1%FB%A1%FA9%A1%FB%A1%FA99%'
                      'A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA%A1%FB%A1%FA'
                      '2%A1%FB%A1%FA%A1%FB%A1%FA-1%A1%FB%A1%FA1531823829%A1%FB%A1%FA0%A1%FB%A1%FA%A1%FB%A1%FA%7C%21'
                      'recentSearch3%7E%601%A1%FB%A1%FA110200%2C00%A1%FB%A1%FA000000%A1%FB%A1%FA0000%A1%FB%A1%FA02%A1'
                      '%FB%A1%FA9%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA'
                      '99%A1%FB%A1%FA%A1%FB%A1%FA2%A1%FB%A1%FA%A1%FB%A1%FA-1%A1%FB%A1%FA1531809088%A1%FB%A1%FA0%A1%FB'
                      '%A1%FA%A1%FB%A1%FA%7C%21recentSearch4%7E%601%A1%FB%A1%FA020000%2C00%A1%FB%A1%FA000000%A1%FB%'
                      'A1%FA0000%A1%FB%A1%FA37%A1%FB%A1%FA9%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA99%'
                      'A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA%A1%FB%A1%FA2%A1%FB%A1%FA%A1%FB%A1%FA-1%A1%FB%A1%FA153'
                      '1801281%A1%FB%A1%FA0%A1%FB%A1%FA%A1%FB%A1%FA%7C%21collapse_expansion%7E%601%7C%21',
            'Host': 'search.51job.com',
            'Referer': 'https://search.51job.com/',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                          '(KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36'
        }
    }

    def __init__(self, **kwargs):
        """
        初始化 Redis 连接
        :param kwargs: 传递参数
        """
        super().__init__(**kwargs)
        self.logger.info('初始化 Redis 连接')
        self.redis_pool = redis.ConnectionPool(host='127.0.0.1', port=6379, password='', db=11)
        self.redis_db = redis.Redis(connection_pool=self.redis_pool)

    def start_requests(self):
        """
        开始发送请求
        """
        # 全国主要城市代号
        city_list = [
            '010000',  # 北京
            '020000',  # 上海
            '030200',  # 广州
            '040000',  # 深圳
            '180200',  # 武汉
            '200200',  # 西安
            '080200',  # 杭州
            '070200',  # 南京
            '090200',  # 成都
            '060000',  # 重庆
            '030800',  # 东莞
            '230300',  # 大连
            '230200',  # 沈阳
            '070300',  # 苏州
            '250200',  # 昆明
            '190200',  # 长沙
            '150200',  # 合肥
            '080300',  # 宁波
            '170200',  # 苏州
            '050000',  # 天津
            '120300',  # 青岛
            '120200',  # 济南
            '220200',  # 哈尔滨
            '240200',  # 长春
            '110200',  # 福州
            '01',  # 珠三角
        ]

        # 行业代号
        industry_list = [
            '01',  # 计算机软件
            '37',  # 计算机硬件
            '38',  # 计算机服务
            '31',  # 通信网络
            '39',  # 电信运营增值
            '33',  # 互联网电子商务
            '40',  # 网络游戏
            '02',  # 电子半导体
            '35',  # 工业自动化
        ]
        url_prefix = 'https://search.51job.com/list/'
        url_suffix = ',9,99,%2520,2,1.html?lang=c&stype=&postchannel=0000&workyear=99&cotype=99&degreefrom=99' \
                     '&jobterm=99&companysize=99&providesalary=99&lonlat=0%2C0&radius=-1&ord_field=0&confirmdate' \
                     '=9&fromType=&dibiaoid=0&address=&line=&specialarea=00&from=&welfare='

        # 构造 URL
        urls = []
        for city in city_list:
            for industry in industry_list:
                url = url_prefix + city + ',000000,0000,' + industry + url_suffix
                urls.append(url)
                logger.info('生成 URL: ' + url)

        # 请求 URL
        for url in urls:
            yield Request(url=url, callback=self.parse)

    def parse(self, response):
        """
        爬取搜索列表中职位的基本信息
        :param response: 响应的页面
        :return: item 对象
        """
        soup = BeautifulSoup(response.text, 'lxml')

        # 从搜索结果页面抓取招聘基本信息
        job_name_list = soup.select('#resultList > div > p > span > a')
        salary_list = soup.select('#resultList > div > span.t4')
        city_list = soup.select('#resultList > div > span.t3')
        company_name_list = soup.select('#resultList > div > span.t2 > a')
        public_date_list = soup.select('#resultList > div > span.t5')

        # 去除列表首部无用的信息
        salary_list = salary_list[1:]
        city_list = city_list[1:]
        public_date_list = public_date_list[1:]

        for job_name, salary, city, company_name, public_date in zip(job_name_list, salary_list, city_list,
                                                                     company_name_list, public_date_list):
            item = WuejobItem()
            url = job_name.get('href')
            if self.redis_db.hexists('wuejob_url', url):
                logger.info('重复的数据: ' + url)
                continue
            logger.info('URL: ' + url)
            item['url'] = url
            item['job_name'] = job_name.get_text().replace('\r\n', '').strip()
            item['salary'] = salary.get_text()
            item['public_date'] = public_date.get_text()
            item['city'] = city.get_text()
            item['company_name'] = company_name.get_text()

            # 请求招聘信息的详细数据
            yield Request(url=url, meta={'item': item}, callback=self.parse_content, dont_filter=True)

        # 获取下一页
        pages = soup.select('#resultList > div.dw_page > div > div > div > ul > li > a')
        if len(pages) is not 0:
            next_page = pages[-1].get('href')
            if next_page is not None:
                logger.info('Next page: ' + next_page)
                yield Request(url=next_page, callback=self.parse, dont_filter=True)

    @staticmethod
    def parse_content(response):
        """
        爬取招聘信息详细页面的信息
        :param response: 页面响应
        :return: item 对象
        """
        soup = BeautifulSoup(response.body, 'lxml')
        item = response.meta['item']

        # 获取职位的详细信息
        address = soup.select('body > div.tCompanyPage > div.tCompany_center.clearfix > div.tCompany_main > '
                              'div:nth-of-type(2) > div > p')[0].get_text().strip()

        base_info = soup.select('div.tCompany_center.clearfix > div.tHeader.tHjob > div '
                                '> div.cn > p.msg.ltype')[0].get_text().strip()
        experience = base_info.split('|')[1].strip()
        education = base_info.split('|')[2].strip()
        head_count = base_info.split('|')[3].strip()

        descriptions = soup.select('div.tCompany_center.clearfix > div.tCompany_main > div:nth-of-type(1) > div > p')
        description = ''
        for line in descriptions:
            description = description + line.get_text().strip()
        description = description.replace('\n', '')
        address = address.replace('上班地址：', '')

        item['address'] = address
        item['experience'] = experience
        item['education'] = education
        item['head_count'] = head_count
        item['description'] = description
        item['crawled_date'] = datetime.date.today()

        yield item
