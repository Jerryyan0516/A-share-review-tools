import requests
import datetime
import pandas as pd
import schedule
import time
from os.path import exists
def fetch_and_save_data():
    # 等待30秒，尽量接近23:59:30

    # time.sleep(30)

    # 获取当前时间
    current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("current_time:", current_time)
    current_time = datetime.datetime.strptime(current_time, "%Y-%m-%d %H:%M:%S")
    current_time_timestamp = str(int(current_time.timestamp()))
    print("current_time_timestamp(seconds):", current_time_timestamp)

    # 获取当天开始的时间戳
    start_time = datetime.datetime.now().strftime("%Y-%m-%d") + " 00:00:00"
    print("start_time:", start_time)
    start_time_timestamp = str(int(datetime.datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S").timestamp()))
    print("start_time_timestamp(seconds):", start_time_timestamp)

    url = "https://www.cls.cn/nodeapi/telegraphList"
    # url = "https://www.cls.cn/nodeapi/get_roll_list"
    params = {
        "app": "CailianpressWeb",
        "category": "",
        "lastTime": current_time_timestamp,
        "last_time": current_time_timestamp,
        "os": "web",
        "refresh_type": 1,
        "rn": 1000
    }
    headers = {
        "Referer": "https://www.cls.cn/telegraph",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36 Edg/129.0.0.0",
        "Cookies": "Hm_lvt_fa5455bb5e9f0f260c32a1d45603ba3e=1727661570; HWWAFSESID=8d329a05bfb416c4ce; HWWAFSESTIME=1728478901642; hasTelegraphNotification=on; hasTelegraphRemind=on; hasTelegraphSound=on; vipNotificationState=on; tfstk=gz9iw-mETC514qVClDWsinAAkVGpzN613xYAkZKVuGbC7PeTBBbDRn12BnBN0-YBrFIx1tE0fpYuHR3s1nycHtu-yYHJfGBf34QiZoF05gI7gxQN0W-sZjeqpYHJfhJgjdxKedF_hzbNut5N_kyFRgsNb-8VTJSf8RS4QizExi_F3PS4QM5FfiIagEWqxH7C0AY-_aA2tpu67KBwpGkcOGfGzh7wXhJUaGP6Xw243pXu7axaj-y2KGfMHxQ_H819g31WBnH0dOKkT9SDh0Pco_A2B9JZ8Y5CgIRRiBUZXU8DYE69LmPNzCLOLdRu0--hIwfAj1ai-aJ6YL6F9YokYpT9fpxY0xSpyw-6QtDrVOXN79jBH2wNoBR2B16b7qbXKHJwigrbT7SHA-sEDpPbG1SCxahESdE7-cty-Dm3Nm1NAGi-xDVbG1SCxannx7Mf_Mss2"
    }

    response = requests.get(url, headers=headers, params=params)
    response.encoding = "UTF-8"
    response.close()
    data = response.json()
    print('-----------------------')
    data_first = data["data"]["roll_data"]

    df_new = pd.DataFrame(columns=["标题", "内容", "消息时间", "分类"])

    for data_second in data_first:
        if int(data_second["ctime"]) >= int(start_time_timestamp):
            subjects = data_second.get("subjects", None)
            subject_names = None
            if subjects is not None:
                subject_names = [subject["subject_name"] for subject in subjects]
                subject_names_str = "+".join(subject_names)
            else:
                subject_names_str = ""

            message_time = datetime.datetime.fromtimestamp(int(data_second["ctime"])).strftime("%Y-%m-%d %H:%M:%S")

            temp_df = pd.DataFrame({
                "标题": data_second["title"],
                "内容": data_second["content"],
                "消息时间": message_time,
                "分类": subject_names_str
            }, index=[0])
            df_new = pd.concat([df_new, temp_df], ignore_index=True)


    # 检查并读取现有 Excel 文件中的数据（如果文件存在）
    output_file = rf'C:\Users\65179\Desktop\quant\{datetime.datetime.now().strftime("%Y-%m-%d")}财联社电报.xlsx'
    
    if exists(output_file):
        local_df = pd.read_excel(output_file)
        df = pd.concat([df_new, local_df], ignore_index=True)
    else:
        df = df_new

    # 处理 NaN 值
    df.fillna('', inplace=True)

    # 在保存之前对DataFrame按消息时间降序排序
    df['消息时间'] = pd.to_datetime(df['消息时间'])
    df.sort_values(by="消息时间", ascending=False, inplace=True)

    # 对整个 DataFrame 进行去重
    df.drop_duplicates(inplace=True)

    # 使用 openpyxl 引擎保存 Excel 文件
    with pd.ExcelWriter(output_file, engine='openpyxl', mode='w') as writer:
        df.to_excel(writer, sheet_name='财联社', index=False)
    print(f"数据已保存到Excel文件：{output_file}")
    




if __name__ == "__main__":
    fetch_and_save_data()

# 定义定时任务
def schedule_tasks():
    # 每半小时执行一次
    schedule.every(30).minutes.do(fetch_and_save_data)
    
    # 每天23:59执行一次
    schedule.every().day.at("23:59").do(fetch_and_save_data)

    while True:
        schedule.run_pending()
        time.sleep(1)
if __name__ == "__main__":
    schedule_tasks()
