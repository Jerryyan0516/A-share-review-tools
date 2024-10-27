import pandas as pd
import akshare as ak
import requests
import os
import concurrent.futures
import time
from lxml import html
from datetime import datetime


def get_stocks_with_large_fluctuations():
    # 获取沪深A股市场数据
    columns_of_interest = ['代码', '名称']

    df = ak.stock_zh_a_spot_em()
    df = df[columns_of_interest]
    df.to_excel(r'C:\Users\65179\Desktop\quant\代码名称.xlsx',index=False)
    # 检查数据是否正确加载
    if df is None or df.empty:
        print("没有获取到有效的股票数据！")
        return None
    else:
        return df
    
# df的列名为 "代码" 和 "名称"

df = get_stocks_with_large_fluctuations()

if df is not None:
    stock_codes = df['代码']

    def get_stock_main_business(stock_code):
        try:
            stock_zyjs_ths_df = ak.stock_zyjs_ths(symbol=stock_code)
            if stock_zyjs_ths_df is not None and not stock_zyjs_ths_df.empty:
                stock_zyjs_ths_df['股票代码'] = stock_code  # 添加股票代码列
                return stock_zyjs_ths_df
            else:
                print(f"股票 {stock_code} 的主要业务信息获取失败")
                return None
        except Exception as e:
            print(f"股票 {stock_code} 的主要业务信息获取异常：{e}")
            return None
        
    # 计算最大线程数
    num_cores = os.cpu_count()  # 获取逻辑核心数
    
    # 使用多线程获取股票信息
    max_workers = min(2 * num_cores, len(stock_codes))  # 根据CPU核心数和股票数量调整最大线程数
    print(f"使用最大线程数：{max_workers}")
    stock_info_list = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(get_stock_main_business, stock_code) for stock_code in stock_codes]
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            if result is not None:
                stock_info_list.append(result)
                print(result['股票代码'][0], "完成")

    # 将列表中的所有 DataFrame 拼接成一个大的 DataFrame
    combined_stock_info = pd.concat(stock_info_list, ignore_index=True)

    # 保存最终的大 DataFrame 到 Excel 文件中
    output_file = r'C:\Users\65179\Desktop\quant\代码主营业务.xlsx'
    combined_stock_info.to_excel(output_file, index=False)

    print(f"汇总数据已成功保存到 {output_file}")
else:
    print("没有获取到有效的股票数据。")






def get_stock_industry_and_concept(stock_code):
    try:
        # 获取个股信息
        stock_individual_info_em_df = ak.stock_individual_info_em(symbol=stock_code)

        # 获取行业信息
        industry_info = {
            '行业': stock_individual_info_em_df.loc[
                stock_individual_info_em_df['item'] == '行业', 'value'].iloc[0]
        }

        # 定义 URL 和 headers
        url = f"https://stockpage.10jqka.com.cn/{stock_code}/"
        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
            'Cache-Control': 'max-age=0',
            'Connection': 'keep-alive',
            'Cookie': '...'  # 替换为实际的 Cookie 值
            ,
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-User': '?1',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36 Edg/129.0.0.0',
            'sec-ch-ua': '"Microsoft Edge";v="129", "Not=A?Brand";v="8", "Chromium";v="129"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"'
        }

        # 发送请求获取网页
        response = requests.get(url, headers=headers)

        # 检查请求是否成功
        if response.status_code == 200:
            print(f"股票 {stock_code} 页面获取成功")
        else:
            print(f"股票 {stock_code} 页面获取失败。状态码: {response.status_code}")
            return pd.DataFrame()

        # 解析网页内容
        tree = html.fromstring(response.content)

        # 使用 XPath 提取 dd[2] 中的 title 属性
        concepts_title = tree.xpath('/html/body/div[9]/div[2]/div[3]/dl/dd[2]/@title')

        # 初始化概念信息
        concept_info = {'所属概念': ''}

        # 如果找到了概念信息，则更新概念信息
        if concepts_title:
            print(f"涉及概念 (title) for {stock_code}:")
            print(concepts_title[0].strip())
            concept_info = {'所属概念': concepts_title[0].strip()}
        else:
            print(f"未找到涉及概念的部分 for {stock_code}")

        # 创建包含股票代码、行业和概念的数据字典
        stock_info = {
            '代码': stock_code,
            '行业': industry_info['行业'],
            '所属概念': concept_info['所属概念']
        }

        # 将字典转换为 DataFrame
        stock_info_df = pd.DataFrame([stock_info])

        return stock_info_df

    except Exception as e:
        print(f"获取股票 {stock_code} 信息失败：{e}")
        return pd.DataFrame()

if df is not None:
    stock_codes = df['代码']
    stock_info_list = []


    for stock_code in stock_codes:
        stock_info_df = get_stock_industry_and_concept(stock_code)
        if not stock_info_df.empty:
            stock_info_list.append(stock_info_df)
            print(stock_code, "完成")
        
        # 在每次请求之后加入延时
         
        time.sleep(1)  # 延长时间间隔



# 多线程爬虫，已经被封禁

    # 计算最大线程数
    # num_cores = os.cpu_count()  # 获取逻辑核心数
    # max_workers = min(2 * num_cores, len(stock_codes))

    # with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
    #     futures = [executor.submit(get_stock_industry_and_concept, stock_code) for stock_code in stock_codes]
    #     for future in concurrent.futures.as_completed(futures):
    #         result = future.result()
    #         if not result.empty:
    #             stock_info_list.append(result)
    #             print(result['代码'][0], "完成")

    #         # 在每次请求之后加入延时
    #         time.sleep(5)




    # 将列表中的所有 DataFrame 拼接成一个大的 DataFrame
    combined_stock_info = pd.concat(stock_info_list, ignore_index=True)

    # 保存最终的大 DataFrame 到 Excel 文件中
    output_file = r'C:\Users\65179\Desktop\quant\代码行业概念.xlsx'
    combined_stock_info.to_excel(output_file, index=False)

    print(f"汇总数据已成功保存到 {output_file}")
else:
    print("没有获取到有效的股票数据。")
