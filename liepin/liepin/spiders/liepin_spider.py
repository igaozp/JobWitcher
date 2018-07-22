# -*- coding: utf-8 -*-
__author__ = 'igaozp'

from scrapy import Spider, Request
from bs4 import BeautifulSoup
from ..items import LiepinItem
import datetime
import logging
import redis

logger = logging.getLogger(__name__)


class LiepinSpider(Spider):
    """
    猎聘网爬虫
    """
    name = 'liepin_spider'

    allowed_domains = ['www.liepin.com']
    start_urls = ['https://www.liepin.com/']

    custom_settings = {
        'COOKIES_ENABLE': False,
        'DEFAULT_REQUEST_HEADERS': {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7,ja;q=0.6',
            'Connection': 'keep-alive',
            'Cookie': 'slide_guide_home_new=1; __uuid=1527384896644.84; _uuid=9F3BDD66FFA848187736D9B98A8FC0F6; '
                      'ADHOC_MEMBERSHIP_CLIENT_ID1.0=dacdd93e-49da-62e4-26d0-718966f79fd7; abtest=0; _fecdn_=1; '
                      '__tlog=1532260799011.06%7C00000000%7C00000000%7C00000000%7C00000000; _mscid=00000000; firsIn=1; '
                      'JSESSIONID=ACD03908741477165BF73973974AB19C; __session_seq=7; __uv_seq=7',
            'Host': 'www.liepin.com',
            'Origin': 'https://www.liepin.com',
            'Referer': 'https://www.liepin.com/',
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
        self.redis_pool = redis.ConnectionPool(host='127.0.0.1', port=6379, password='', db=11)
        self.redis_db = redis.Redis(connection_pool=self.redis_pool)

    def start_requests(self):
        # 城市列表
        city_list = [
            '010',  # 北京
            '020',  # 上海
            '040',  # 重庆
            '030',  # 杭州
            '050020',  # 广州
            '050090',  # 深圳
            '280020',  # 成都
            '060020',  # 南京
            '070020',  # 杭州
            '210040',  # 大连
            '140020',  # 石家庄
            '150020',  # 郑州
            '170020',  # 武汉
            '180020',  # 长沙
            '200020',  # 南昌
            '210020',  # 沈阳
            '190020',  # 长春
            '160020',  # 哈尔滨
            '270020',  # 西安
            '250020',  # 济南
            '080020',  # 合肥
            '120020',  # 贵阳
            '090020',  # 福州
            '250070',  # 青岛
        ]

        # 行业列表
        industries = [
            '010',  # 计算机软件
            '040',  # 互联网 电商
            '420',  # 网络游戏
            '030',  # IT 服务
            '020',  # 计算机硬件
            '060',  # 通信工程
        ]

        # 构建 URL
        urls = []
        for city in city_list:
            for industry in industries:
                url = 'https://www.liepin.com/company/' + city + '-' + industry + '/'
                urls.append(url)
                logger.info('生成 URL: ' + url)

        # 请求 URL
        for url in urls:
            yield Request(url=url, callback=self.parse)

    def parse(self, response):
        """
        处理公司页面
        :param response: 页面响应
        :return: 请求
        """
        soup = BeautifulSoup(response.text, 'lxml')

        # 获取当前页面的公司列表
        company_list = soup.select('#region > div.wrap > div.company-list.clearfix > div > div.item-top.clearfix > '
                                   'div > p.job-num > a')
        # 查找正在招聘的职位 URL
        urls = []
        for company in company_list:
            url = company.get('href')
            urls.append(url)

        # 请求招聘的职位列表
        for url in urls:
            yield Request(url=url, callback=self.parse_job)

        # 获取下一页的公司列表
        pages = soup.select('#region > div.wrap > div.pager-box > div > a')
        if len(pages) is not 0:
            next_page = pages[-2].get('href')
            if next_page is not None:
                logger.info('Next page: ' + next_page)
                yield Request(url=next_page, callback=self.parse, dont_filter=True)

    def parse_job(self, response):
        """
        从职位列表中获取基本信息
        :param response: 页面响应
        :return: 请求
        """
        soup = BeautifulSoup(response.text, 'lxml')

        # 获取基本信息列表
        job_name_list = soup.select('#company > div.main.wrap > div > div > div > div.job.clearfix > div '
                                    '> ul > li > div.job-info > a')
        salary_list = soup.select('#company > div.main.wrap > div > div > div > div.job.clearfix > div > ul > li > '
                                  'div.job-info > p.condition.clearfix > span.text-warning')
        city_list = soup.select('#company > div.main.wrap > div > div > div > div.job.clearfix > div > ul > li > '
                                'div.job-info > p.condition.clearfix > span:nth-of-type(2)')
        education_list = soup.select('#company > div.main.wrap > div > div > div > div.job.clearfix > div > ul '
                                     '> li > div.job-info > p.condition.clearfix > span:nth-of-type(3)')
        experience_list = soup.select('#company > div.main.wrap > div > div > div > div.job.clearfix > div > ul > '
                                      'li > div.job-info > p.condition.clearfix > span:nth-of-type(4)')

        for job_name, salary, city, education, experience in zip(job_name_list, salary_list, city_list, education_list,
                                                                 experience_list):
            item = LiepinItem()
            url = job_name.get('href')
            if self.redis_db.hexists('liepin_url', url):
                logger.info('重复的数据: ' + url)
                continue
            logger.info('URL: ' + url)
            item['url'] = url
            item['job_name'] = job_name.get_text().strip()
            item['salary'] = salary.get_text().strip()
            item['city'] = city.get_text().strip()
            item['education'] = education.get_text().strip()
            item['experience'] = experience.get_text().strip()

            # 获取职位的详细信息
            yield Request(url=url, meta={'item': item}, callback=self.parse_content, dont_filter=True)

        # 获取职位列表的下一页
        pages = soup.select('#page-bar-holder > div > a')
        if len(pages) is not 0:
            next_page = pages[-2].get('href')
            if next_page is not None:
                logger.info('Next page: ' + next_page)
                yield Request(url=next_page, callback=self.parse, dont_filter=True)

    @staticmethod
    def parse_content(response):
        """
        获取职位的详细信息
        :param response: 页面响应
        :return: item 对象
        """
        soup = BeautifulSoup(response.body, 'lxml')
        item = response.meta['item']

        # 获取职位的详细信息
        description = soup.select('#job-view-enterprise > div.wrap.clearfix > div.clearfix > div.main > '
                                  'div.about-position > div.job-item.main-message.job-description '
                                  '> div')[0].get_text().strip()
        address = soup.select('#job-view-enterprise > div.wrap.clearfix > div.clearfix > div.side > div:nth-of-type(2) '
                              '> div.right-post-top > div.company-infor > div > ul.new-compintro > '
                              'li:nth-of-type(3)')[0].get_text().strip()
        public_date = soup.select('#job-view-enterprise > div.wrap.clearfix > div.clearfix > div.main > '
                                  'div.about-position > div:nth-of-type(2) > div.clearfix > div.job-title-left '
                                  '> p.basic-infor > time')[0].get_text().strip()
        company_name = soup.select('#job-view-enterprise > div.wrap.clearfix > div.clearfix > div.side > '
                                   'div:nth-of-type(2) > div > div.company-infor > div > '
                                   'div.company-logo > p > a')[0].get_text().strip()
        item['description'] = description
        item['address'] = address
        item['public_date'] = public_date
        item['company_name'] = company_name
        item['crawled_date'] = datetime.date.today()

        yield item
