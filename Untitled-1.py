import pandas as pd
import akshare as ak
import requests
import time
from lxml import html
from datetime import datetime


def get_stocks_with_large_fluctuations():
    # 获取沪深A股市场数据
    
    columns_of_interest = ['代码', '名称', '最新价', '涨跌幅', '成交量', '成交额', '最高', '最低', '今开', '昨收', '换手率', '总市值', '流通市值']

    df = ak.stock_zh_a_spot_em()
    df = df[columns_of_interest]
    # 检查数据是否正确加载
    if df is None or df.empty:
        print("没有获取到有效的股票数据！")
        return
    
    # 尝试将涨跌幅列转换为浮点数
    try:
        df['涨跌幅'] = df['涨跌幅'].astype(str).str.replace('%', '').astype(float)

        # 定义要排除的前缀

        # 只保留沪深
        prefixes_to_avoid = ["30", "68", "83", "92","87","43"]

        # 排除北交所
        # prefixes_to_avoid = ["83", "92","87","43"]
        
        # 过滤掉指定前缀的股票代码
        df = df[~df['代码'].astype(str).str.startswith(tuple(prefixes_to_avoid))]

        # 过滤出涨跌幅大于5%的股票
        filtered_df = df[(df['涨跌幅'].abs() >= 5)]

        # 过滤ST的股票
        filtered_df = filtered_df[~filtered_df['名称'].str.contains('ST')]

        # 成交量单位是 '万手'
        filtered_df['成交量'] = (filtered_df['成交量']/10000).round(2).astype(str) + '万手'

        # 将总市值和流通市值转换为以亿为单位，并保留两位小数，并加上'亿'
        filtered_df['总市值'] = (filtered_df['总市值'] / 100000000).round(2).astype(str) + '亿'
        filtered_df['流通市值'] = (filtered_df['流通市值'] / 100000000).round(2).astype(str) + '亿'
        filtered_df['成交额'] = (filtered_df['成交额'] / 100000000).round(2).astype(str) + '亿'

        # 将换手率转换为字符串，并加上'%'
        filtered_df['换手率'] = filtered_df['换手率'].astype(str) + '%'  # 去除原始的百分号再添加

        # 计算涨幅大于5%的股票数量
        rising_stocks_count = len(filtered_df[filtered_df['涨跌幅'] > 0])

        # 计算跌幅大于5%的股票数量
        falling_stocks_count = len(filtered_df[filtered_df['涨跌幅'] < 0])

        # 输出结果
        print(f"涨幅大于5%的股票有 {rising_stocks_count} 个，跌幅大于5%的股票有 {falling_stocks_count} 个。")

        return filtered_df, rising_stocks_count, falling_stocks_count

    except ValueError:
        print("数据处理失败，请检查数据格式。")
        return None, None, None

def get_stock_infomations(stock_code):
    # 获取股票的基本信息，包括所属板块和概念
    try:
    # 获取板块内容
        # 获取同花顺上面的主营业务，产品类型，产品名称，经营范围
        stock_zyjs_ths_df = ak.stock_zyjs_ths(symbol=stock_code)
        stock_info = stock_zyjs_ths_df[['主营业务', '产品类型', '产品名称', '经营范围']]
        stock_individual_info_em_df = ak.stock_individual_info_em(symbol=stock_code)

        industry_info = {
            '行业': stock_individual_info_em_df.loc[stock_individual_info_em_df['item'] == '行业', 'value'].iloc[0]
        }

    # 通过爬虫获取个股相关概念
        # 定义 URL 和 headers
        url = f"https://stockpage.10jqka.com.cn/{stock_code}/"

        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
            'Cache-Control': 'max-age=0',
            'Connection': 'keep-alive',
            'Cookie': 'searchGuide=sg; log=; spversion=20130314; cmsad_170_0=0; _ga=GA1.1.1415001867.1727210208; _ga_KQBDS1VPQF=GS1.1.1727210207.1.1.1727210279.0.0.0; Hm_lvt_78c58f01938e4d85eaf619eae71b4ed1=1727210304; HMACCOUNT=E7F9CF2FBDF40D14; Hm_lvt_22a3c65fd214b0d5fd3a923be29458c7=1727210304; Hm_lpvt_78c58f01938e4d85eaf619eae71b4ed1=1727210316; Hm_lpvt_22a3c65fd214b0d5fd3a923be29458c7=1727210316; historystock=600000%7C*%7C600550; v=A8nHmBtly4uKV7f_-IAU1TZG2P4mFr-fJwvhwGs-R1TuNOdgs2bNGLda8bb4',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-User': '?1',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36 Edg/129.0.0.0',
            'sec-ch-ua': '"Microsoft Edge";v="129", "Not=A?Brand";v="8", "Chromium";v="129"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
        }

        # 发送请求获取网页
        response = requests.get(url, headers=headers)

        # 检查请求是否成功
        if response.status_code == 200:
            print("Successfully fetched the page")
        else:
            print(f"Failed to fetch the page. Status code: {response.status_code}")

        # 解析网页内容
        tree = html.fromstring(response.content)

        # 使用 XPath 提取 dd[2] 中的 title 属性
        concepts_title = tree.xpath('/html/body/div[9]/div[2]/div[3]/dl/dd[2]/@title')

        # 打印 title 属性中的完整内容
        if concepts_title:
            print("涉及概念 (title):")
            print(concepts_title[0].strip())  # 只取第一个匹配的结果，并去除空格
            concept_info = {'所属概念': concepts_title[0].strip()}
        else:
            print("未找到涉及概念的部分")



        # 拼接行业的数据以及概念的数据
        stock_info['行业'] = industry_info['行业'] 
        stock_info['所属概念'] = concept_info['所属概念']       
        return stock_info

    except Exception as e:
        print(f"获取股票 {stock_code} 信息失败：{e}")
        return {}
    
    
# 获取当前日期
current_date = datetime.now().strftime("%Y-%m-%d")

# 调用函数并获取结果
large_fluctuations, rising_stocks_count, falling_stocks_count = get_stocks_with_large_fluctuations()

if large_fluctuations is not None:
    print(large_fluctuations.columns)

    # 添加股票的基本信息
    stock_info_list = []

    for _, row in large_fluctuations.iterrows():
        stock_code = row['代码']
        stock_info = get_stock_infomations(stock_code)
        
        # 检查是否成功获取到信息
        if stock_info is None or stock_info.empty:
            print(f"股票 {stock_code} 的基本信息获取失败")
        else:
            stock_info_list.append(stock_info)
            # 打印中间结果用于调试
            print(f"股票 {stock_code} 的基本信息已添加到列表中：\n{stock_info}")
        


    # 检查 stock_info_list 是否为空
    if not stock_info_list:
        print("没有获取到有效的股票信息！")
    else:
        # 将 stock_info_list 中的 DataFrame 拼接成一个大的 DataFrame
        combined_stock_info = pd.concat(stock_info_list, ignore_index=True)

        # 输出最终结果用于调试
        print("合并后的股票信息：\n", combined_stock_info)

        # 合并两部分数据
        final_df = pd.concat([large_fluctuations.reset_index(drop=True), combined_stock_info], axis=1)

        # 创建新的列顺序列表
        new_column_order = ['代码', '名称', '行业', '所属概念', '主营业务', '产品类型', '产品名称', '经营范围', '最新价', '涨跌幅', '成交量', '成交额', '最高', '最低', '今开', '昨收', '换手率', '总市值', '流通市值']

        # 重新排列列顺序
        final_df = final_df.reindex(columns=new_column_order)
    # 应用样式
    def color_negative_green_positive_red(value):
        if value >= 5:
            return 'color: red'
        elif value <= -5:
            return 'color: green'
        else:
            return ''

    styled_df = final_df.style
    # styled_df = styled_df.applymap(color_negative_green_positive_red, subset=['涨跌幅'])
    styled_df = styled_df.map(color_negative_green_positive_red, subset=['涨跌幅'])

    # 将结果保存到带有日期的Excel文件中
    output_file = rf'C:\Users\65179\Desktop\quant\{current_date}复盘.xlsx'
    styled_df.to_excel(output_file, engine='openpyxl', index=False, sheet_name='公司info')

    print(f"数据已成功保存到 {output_file}")
else:
    print("没有符合条件的股票数据。")

print(f"涨幅大于5%的股票有 {rising_stocks_count} 个，跌幅大于5%的股票有 {falling_stocks_count} 个。")

