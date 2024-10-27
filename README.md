# A-share-review-tools
This is a tool that can obtain the top 100 hot stocks, stocks with a rise or fall of more than 5%, stocks with daily limit up and daily limit down, and a collection tool for Cailianshe Telegram


python程序：

Untitled-1.py
筛选每日涨跌幅大于5%的股票对应的信息

update_stock_info.py
所有上市的A股的相关信息，保存到上市公司股票信息.xlsx中
三个输出：代码行业概念、代码主营业务、代码名称

hot_index_stocks.py
东财当天热度前100的股票以及对应的题材和主营业务
输出：每日热股排名


cls_telegram_crawling.py
爬虫爬取财联社电报，后续更新更多新闻网站爬虫
用途：喂给AI大模型用于总结



数据库表：
代码行业概念
代码主营业务
代码名称
