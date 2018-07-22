# -*- coding: utf-8 -*-
__author__ = 'igaozp'

from scrapy.exceptions import DropItem
import pymysql
import redis
import logging

logger = logging.getLogger(__name__)


class LiepinPipeline(object):
    def __init__(self, host, database, user, password, port):
        """
        初始化数据库所需要的参数
        :param host: 数据库主机地址
        :param database: 数据库名称
        :param user: 数据库用户名
        :param password: 数据库密码
        :param port: 数据库服务端口
        """
        self.host = host
        self.db = database
        self.user = user
        self.password = password
        self.port = port
        self.connect = None
        self.cursor = None
        self.redis_pool = None
        self.redis_db = None

    @classmethod
    def from_crawler(cls, crawler):
        """
        使用 crawler 从 settings 中获取数据库相关的配置
        :param crawler: 使用该 pipeline 的 crawler
        :return: pipeline 实例
        """
        host = crawler.settings.get('MYSQL_HOST')
        db = crawler.settings.get('MYSQL_DB')
        user = crawler.settings.get('MYSQL_USER')
        password = crawler.settings.get('MYSQL_PASSWORD')
        port = crawler.settings.get('MYSQL_PORT')
        return cls(host, db, user, password, port)

    def open_spider(self, spider):
        """
        spider 初始化时创建数据库连接
        :param spider: 启动的 spider
        """
        logger.info('初始化数据库连接...')
        # 创建 MySQL 数据库连接
        self.connect = pymysql.connect(self.host, self.user, self.password, self.db, port=self.port, charset='utf8',
                                       cursorclass=pymysql.cursors.DictCursor)
        self.cursor = self.connect.cursor()
        # 创建 Redis 数据库连接
        self.redis_pool = redis.ConnectionPool(host='127.0.0.1', port=6379, password='', db=11)
        self.redis_db = redis.Redis(connection_pool=self.redis_pool)

    def close_spider(self, spider):
        """
        spider 关闭时持久化 Redis 缓存，同时关闭 MySQL 数据库连接
        :param spider: 关闭的 spider
        """
        # Redis 持久化
        self.redis_db.bgsave()
        # MySQL 连接关闭
        self.cursor.close()
        self.connect.close()

    def process_item(self, item, spider):
        """
        pipeline 数据处理
        :param item: 爬取的 item
        :param spider: 爬取 item 的 spider
        :return: item 对象
        """
        if spider.name == 'liepin_spider':
            url = item['url']
            if self.redis_db.hexists('liepin_url', url):
                # 若缓存有该 URL 则跳过该 URL 的处理
                raise DropItem("Duplicate item found: %s" % url)
            else:
                # 若没有则将 URL 添加到缓存中，并写入数据库
                logger.info('插入新数据: ' + url)
                self.redis_db.hset('liepin_url', url, 0)
                self.insert_data(item)
        return item

    def insert_data(self, item):
        """
        向数据库插入数据
        :param item: 插入数据的对象实例
        """

        sql = 'insert into liepin (job_name, salary, company_name, description, city, address, ' \
              'experience, education, url, public_date, crawled_date)' \
              'values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'
        params = [
            item['job_name'],
            item['salary'],
            item['company_name'],
            item['description'],
            item['city'],
            item['address'],
            item['experience'],
            item['education'],
            item['url'],
            item['public_date'],
            item['crawled_date']
        ]

        # 执行 SQL 语句并提交
        self.cursor.execute(sql, params)
        self.connect.commit()
