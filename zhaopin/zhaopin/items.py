# -*- coding: utf-8 -*-
__author__ = 'igaozp'

import scrapy


class ZhaopinItem(scrapy.Item):
    # 工作名称
    job_name = scrapy.Field()
    # 薪水
    salary = scrapy.Field()
    # 工作经验
    experience = scrapy.Field()
    # 所在城市
    city = scrapy.Field()
    # 公司地址
    address = scrapy.Field()
    # 公司名称
    company_name = scrapy.Field()
    # 招聘人数
    head_count = scrapy.Field()
    # 学历要求
    education_require = scrapy.Field()
    # 公司规模
    company_size = scrapy.Field()
    # 工作要求
    job_require = scrapy.Field()
    # 发布日期
    public_date = scrapy.Field()
    # URL
    url = scrapy.Field()
    # 数据生成的时间
    time = scrapy.Field()
