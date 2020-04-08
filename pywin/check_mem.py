#!/usr/bin/python
# -*- coding: UTF-8 -*-

"""
        Created on 2019/12/2 17:33
        ————————————————————————————————————————————————————————————————————
        @File Name            :    ss.py
        @Author                 :    Frank
        @Description        :
            小组成员信息获取，数据筛选模块
            本程序主要调用扇贝提供的 API 来帮助进行群组的管理
            部分功能采用爬虫技术
        ————————————————————————————————————————————————————————————————————
        @Change Activity:
        mod on 2020/4/7 17:30
            文件操作的代码全部弃用，改用 sqlite 数据库来存储所有用户数据。


"""

import requests
import json
import time
import os
import copy
import sqlite3


class MemDataBasic(object):
    """
        获取全体组员的基本打卡信息，筛选并存储这些内容
    """

    def __init__(self, team_name ='35K', team_id = 10879):
        """
          初始化，创建目录，跳转路径等操作
        :param team_name:
        :param team_id:
        """
        self.db_name = team_name + '.db'
        self.table = 'table_' + time.strftime('%Y%m%d', time.localtime())
        
        #TODO 测试代码
        # self.table='table_20200407'

        self.START_TIME = 'start_time'    # 开始时间的常量，只用来判断
        self.END_TIME = 'end_time'    # 结束时间的常量，只用来判断
        self.start_time = ''    # 计时器，程序开始时间
        self.end_time = ''    # 计时器，程序结束时间
        self.cnt = 0    # test code
        self.data = {}    # 存储从扇贝获取数据后的初步处理后的有效数据，包括小组信息和用户信息
        self.members = []    # 为 data 字典中一部分，主要暂存用户信息
        self.valid_data = {}    # data 的数据存放到文件中，再从文件中读取放入到 validData 变量中。
        self.filter_items = {'rank': 'rank',
                                                'points': 'points',
                                                'age': 'age',
                                                'chkDay': 'checkin_days',
                                                'chkRate': 'checkin_rate'}    # 可用来进行筛选的几个数据项

        # 存储文件名称的相关变量
        self.file_name_date = time.strftime('%Y%m%d', time.localtime())    # 获取本地时间的元组转换为字符串
        self.dir_name_year = self.file_name_date[-8:-4]    # 文件名中的年份
        self.dir_name_month = self.file_name_date[-4:-2]    # 文件名中的月份

        self.team_name = team_name
        self.team_id = team_id
        self.start_time = time.time()
        #TODO 判断数据库是否存在
        self.conn = sqlite3.connect(self.db_name)
        self.cs = self.conn.cursor()
        try:
            cmd = """CREATE TABLE %s (id INT PRIMARY KEY,
                                        username TEXT,
                                        nickname TEXT,
                                        timezone TEXT,
                                        rank INT,
                                        points INT,
                                        age INT,
                                        checkined_today BOOLEAN,
                                        checkined_ytd BOOLEAN,
                                        checkin_days INT,
                                        checkin_rate FLOAT)
                                        """ % (self.table)
            self.cs.execute(cmd)
        except sqlite3.OperationalError:
            self.cs.execute("DROP TABLE %s" % (self.table))
            self.conn.commit()
            cmd = """CREATE TABLE %s (id INT PRIMARY KEY,
                                        username TEXT,
                                        nickname TEXT,
                                        timezone TEXT,
                                        rank INT,
                                        points INT,
                                        age INT,
                                        checkined_today BOOLEAN,
                                        checkined_ytd BOOLEAN,
                                        checkin_days INT,
                                        checkin_rate FLOAT)
                                        """ % (self.table)
            self.cs.execute(cmd)
            print("删除表格，重新创建！")

        
    def __del__(self):
        self.cs.close()
        self.conn.close()
        
        
    def save_data_to_file(self, data, file_name):
        with open(file_name, 'w+') as f:
            f.write(json.dumps(data))

    def read_data_from_file(self, file_name):
        with open(file_name, 'r') as f:
            return f.read()

    def exe_sql(self, cmd):
        self.cs.execute(cmd)
        res = self.cs.fetchall()
        for i in res:
            print(i)
        return res


    def save_data_to_db(self, data):
        """
        保存数据到数据库中
        :param data: 实际要保存的数据
        :param date: 文件名中的日期，具体日期格式如 19700101
        :param file_type: 前缀表示文件类型
        :return:
        """
        
        if type(data) is str:
            data = json.loads(data)
        
        for i in data:
            try:
                cmd = """INSERT INTO %s VALUES (%d,
                                                "%s",
                                                "%s",
                                                "%s",
                                                %d,
                                                %d,
                                                %d,
                                                %d,
                                                %d,
                                                %d,
                                                "%f")
                                                """ % (self.table,
                                                        i["id"],
                                                        i["username"],
                                                        i['nickname'],
                                                        i['timezone'],
                                                        i['rank'],
                                                        i['points'],
                                                        i['age'],
                                                        i['checkined_today'],
                                                        i['checkined_ytd'],
                                                        i['checkin_days'],
                                                        i['checkin_rate'])
                self.cs.execute(cmd)
            except sqlite3.IntegrityError:
                cmd = """INSERT INTO %s VALUES (%d,
                                                "%s",
                                                "%s",
                                                "%s",
                                                %d,
                                                %d,
                                                %d,
                                                %d,
                                                %d,
                                                %d,
                                                "%f")
                                                """ % (self.table,
                                                        i["id"],
                                                        i["username"],
                                                        i['nickname'],
                                                        i['timezone'],
                                                        i['rank'],
                                                        i['points'],
                                                        i['age'],
                                                        i['checkined_today'],
                                                        i['checkined_ytd'],
                                                        i['checkin_days'],
                                                        i['checkin_rate'])
                self.cs.execute(cmd)

                print("数据已经存在，无需重复添加！")
        self.conn.commit()


    def get_team_all_mem(self):
        """
        通过扇贝提供的 api 获得小组成员的全部打卡信息
        对这个信息进行筛选，只保留个人数据相关的有效信息
        :return: None
        """
        
        # 获取末尾数据，获得实际人数是否读 api 可用等信息
        url = "https://www.shanbay.com/api/v1/team/" + str(self.team_id) + "/member/?page="
        kw = '70'
        headers = {
            'user-agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.100 Safari/537.36'
        }
        response = requests.get(url+kw, headers=headers)
        jdata = json.loads(response.content.decode('utf-8'))
        baseData = {
            'msg': jdata['msg'],
            'total': jdata['data']['total'],
        }
        self.data['baseData'] = baseData
        
        # 循环查询所有用户的数据信息
        for i in range(1, baseData['total']//10+2):
        # for i in range(1, 3):         # test code
            url = "https://www.shanbay.com/api/v1/team/10879/member/?page="
            kw = str(i)
            response = requests.get(url+kw, headers=headers)
            jdata = json.loads(response.content.decode('utf-8'))            # api 获得的原始用户数据
            
            # 筛选出整数页数据，如 61 页，则筛选出前 60 页数据，每页刚好 10 个，共 600
            if i <= baseData['total']//10:
                for i in range(10):
                    SigMemMsg = {
                        'id':jdata['data']['members'][i]['user']['id'], 
                        "username":jdata['data']['members'][i]['user']['username'],
                        'nickname':jdata['data']['members'][i]['user']['nickname'],
                        "timezone":jdata['data']['members'][i]['user']['timezone'],
                        'rank':jdata['data']['members'][i]['rank'],
                        "points":jdata['data']['members'][i]['points'],
                        "age":jdata['data']['members'][i]['age'],
                        "checkined_today":jdata['data']['members'][i]['checkined_today'],
                        "checkined_ytd":jdata['data']['members'][i]['checkined_ytd'],
                        "checkin_days":jdata['data']['members'][i]['checkin_days'],
                        "checkin_rate":float(jdata['data']['members'][i]['checkin_rate'].split("%")[0])
                        }
                    self.members.append(SigMemMsg)
                    
            # 筛选出最后一页数据，如 61 页，则筛选出第 61 页的数据，如 603 个数据，最后一页则是 3 个数据
            elif i == baseData['total']//10+1:
                for i in range(baseData['total']%10):
                    SigMemMsg = {
                        'id':jdata['data']['members'][i]['user']['id'], 
                        "username":jdata['data']['members'][i]['user']['username'],
                        'nickname':jdata['data']['members'][i]['user']['nickname'],
                        "timezone":jdata['data']['members'][i]['user']['timezone'],
                        'rank':jdata['data']['members'][i]['rank'],
                        "points":jdata['data']['members'][i]['points'],
                        "age":jdata['data']['members'][i]['age'],
                        "checkined_today":jdata['data']['members'][i]['checkined_today'],
                        "checkined_ytd":jdata['data']['members'][i]['checkined_ytd'],
                        "checkin_days":jdata['data']['members'][i]['checkin_days'],
                        "checkin_rate":float(jdata['data']['members'][i]['checkin_rate'].split("%")[0])
                        }
                    self.members.append(SigMemMsg)        
            time.sleep(0.1)             # 疑似远程主机会关闭链接获取数据，防止被反爬
            self.cnt += 1        # test code
            print(self.cnt)    # test code
            
        self.data['MemInfo'] = self.members                                                 # members 是 data 中的主要数据内容
        return self.data['MemInfo']



    def read_all_data_from_db(self, table):
        table = 'table_'+table
        cmd = """SELECT * FROM %s

        """ % (table)
        return self.exe_sql(cmd)


    def unchecked_today(self):
        table = 'table_'+time.strftime('%Y%m%d',  time.localtime())
        cmd = """SELECT id,username,nickname FROM %s WHERE checkined_today=0
                """ % (table)
        print("今天未打卡：")
        self.exe_sql(cmd)
        
    def unchecked_ytd(self):
        table = 'table_'+time.strftime('%Y%m%d',  time.localtime(round(time.time())-86400))
        cmd = """SELECT id,username,nickname FROM %s WHERE checkined_ytd=0
                 """ % (table)
        print("昨天未打卡：")
        self.exe_sql(cmd)
        
    def sum_points(self, table):
        table = 'table_'+table
        cmd = """SELECT sum(points) FROM %s
                """ % (table)
        print("总积分数：")
        return self.exe_sql(cmd)[0][0]

    def filterTopNum(self, tdata, num, item):
        """
            筛选出前 n 的数据，
        :param tdata: 原始数据
        :param num: 需要筛选出的前 n 个
        :param item: 只能是 filterItems 中的一个值，且不能为 "checkin_rate"
        :return:
        """
        res = []
        tmpData = copy.deepcopy(tdata)
        for i in range(num):
            tmpCmp = tmpData[0][item]
            tmpPos = int()
            for j in range(len(tmpData)):
                if tmpCmp < tmpData[j][item]:
                    tmpCmp = tmpData[j][item]
                    tmpPos = j
            else:
                res.append(tmpData.pop(tmpPos))
        return res 
        
    
    def team_points_last_days(self, days):
        return self.sum_points(time.strftime('%Y%m%d',  time.localtime())) - self.sum_points(time.strftime('%Y%m%d',  time.localtime(round(time.time())-86400*days)))



class MemDataCommon(MemDataBasic):
    """
        以下代码都是常用操作的示例代码，是对上面一些功能函数的组合使用，方便用户调用
        实际使用时，更建议根据自己的需求对上面的代码进行组合调用，例如：想要获取年贡献榜前 10
        的用户，则可以在 calThisPeriodData 输入 20190101 ，20191231 和 points
        获取打卡天数前十的用户
    """

    def team_points_last_one_day(self):
        return self.team_points_last_days(1)


    def team_points_last_week(self):
        return self.team_points_last_days(7)


    def team_points_last_week(self):
        return self.team_points_last_days(30)


    def chk_days_top_10(self, table):
        """
            获取打卡排名前十的用户
        :return:
        """
        if table == 'Today':
            table = 'table_'+time.strftime('%Y%m%d',  time.localtime())
        else:
            table = 'table_'+table
        cmd = """ SELECT id, username, nickname, checkin_days  FROM %s ORDER BY points DESC LIMIT 10;
                    """ % (table)
        print("排名前十的用户:")
        return self.exe_sql(cmd)
        
        
    def age_below_21(self, table):
        """
            从指定日期的文件中获取组龄低于 21 天的用户
        :return:
        """
        if table == 'Today':
            table = 'table_'+time.strftime('%Y%m%d',  time.localtime())
        else:
            table = 'table_'+table
        cmd = """ SELECT id, username, nickname, checkin_days  FROM %s WHERE age<=21;
                    """ % (table)
        print("组龄低于 21 天的用户:")
        return self.exe_sql(cmd)

    def age_over_100(self, table):
        """
            获得组龄为 100 天的用户，仅作为案例可以设置为其他日子的用户
        :return:
        """
        if table == 'Today':
            table = 'table_'+time.strftime('%Y%m%d',  time.localtime())
        else:
            table = 'table_'+table
        cmd = """ SELECT id, username, nickname, checkin_days  FROM %s WHERE age>=100;
                    """ % (table)
        print("获得组龄为 100 天的用户:")
        return self.exe_sql(cmd)
    
    
    def age_equal_100s(self, table):
        """
            筛选出满整百天数的人
        :return:
        """
        if table == 'Today':
            table = 'table_'+time.strftime('%Y%m%d',  time.localtime())
        else:
            table = 'table_'+table
        res = []
        cmd = """ SELECT max(age) FROM %s;
                                """ % (table)
        for i in range(self.exe_sql(cmd)[0][0] // 100):
            cmd = """ SELECT id, username, nickname, checkin_days  FROM %s WHERE age==%d;
                        """ % (table, 100*i)
            res += self.exe_sql(cmd)
        return res

    def points_top_10(self, table):
        """
            从指定日期的文件中获取贡献值前十用户
        :return:
        """
        if table == 'Today':
            table = 'table_'+time.strftime('%Y%m%d',  time.localtime())
        else:
            table = 'table_'+table
        cmd = """ SELECT id, username, nickname, checkin_days  FROM %s ORDER BY points DESC LIMIT 10;
                    """ % (table)
        print("总贡献值前十用户：")
        return self.exe_sql(cmd)
    
    def age_top_10(self, table):
        """
            从指定日期的文件中获取组龄前十用户
        :return:
        """
        if table == 'Today':
            table = 'table_'+time.strftime('%Y%m%d',  time.localtime())
        else:
            table = 'table_'+table
        cmd = """ SELECT id, username, nickname, checkin_days  FROM %s ORDER BY age DESC LIMIT 10;
                    """ % (table)
        print("组龄前十用户：")
        return self.exe_sql(cmd)
    
        
    def chk_days_top_10(self, table):
        """
            从指定日期的文件中获取组打卡天数前十用户
        :return:
        """
        if table == 'Today':
            table = 'table_'+time.strftime('%Y%m%d',  time.localtime())
        else:
            table = 'table_'+table
        cmd = """ SELECT id, username, nickname, checkin_days  FROM %s ORDER BY checkin_days DESC LIMIT 10;
                    """ % (table)
        print("打卡天数前十用户：")
        return self.exe_sql(cmd)
    
    
    def chk_rate_last_10(self, table):
        """
            从指定日期的文件中获取打卡率后十用户
        :return:
        """
        if table == 'Today':
            table = 'table_'+time.strftime('%Y%m%d',  time.localtime())
        else:
            table = 'table_'+table
        cmd = """ SELECT id, username, nickname, checkin_days, checkin_rate  FROM %s ORDER BY checkin_rate ASC LIMIT 10;
                    """ % (table)
        print("打卡率后十用户：")
        return self.exe_sql(cmd)
    
    def untask_user_list(self, days, threshold):
        """
        筛选出未加入计划的用户
        days：n 天内的积分变化情况
        threshold： 阈值，筛选出低于这个值得用户
        :return:
        """
        data1 = self.read_all_data_from_db(time.strftime('%Y%m%d',  time.localtime()))
        data2 = self.read_all_data_from_db(time.strftime('%Y%m%d',  time.localtime(round(time.time())-86400*days)))
        val = []
        # 读取两个表格所有的数据，手动减出差值，也可以用数据库检索的方式
        for i in data1:
            for j in data2:
                if i[0] == j[0]:
                    val.append({'id': i[0],
                                'username': i[1],
                                'nickname': i[2],
                                'timezone': i[3],
                                'rank': i[4],
                                'points_diff': i[5]-j[5],
                                'age': i[6],
                                'checkined_today': i[7],
                                'checkined_ytd': i[8],
                                'checkin_days': i[9],
                                'checkin_rate': i[10]
                              })
        
        for i in val:
            if i['points_diff'] < threshold:
                print("满足的成员: %d, %s, %d" % (i['id'], i['username'], i['points_diff']))
    
    
    def month_points_top_10(self):
        """
            本月用户贡献排名前十用户
        :return:
        """
        pass
        
    
    def time_cnt(self, state):
        """
            计时函数，统计整个程序执行的时间
            时间尤其是 api 爬取用户数据，整个需要控制在有效范围内，否则发送警报
        :param state:
        :return:
        """
        if state == self.START_TIME:
            self.StartTime = time.time()
        elif state == self.END_TIME:
            self.EndTime = time.time()
            
        optTime = self.EndTime-self.StartTime
        print('Getting Data from Shanbay takes '+str(optTime)+' s')
        return optTime


    def user_filter_data(self):
        """
            多层筛选，从某预处理过的文件中再次筛选出数据
            预留用户外部可自己调用几个函数组合。
            实际这个函数不再补充，用户自行组合调用以上功能即可
        :return:
        """
        pass


if __name__ == '__main__':
    try:
        teamName = '35K'
        teamID = 10879
        s = MemDataCommon(teamName, teamID)
        res = s.get_team_all_mem()
        s.save_data_to_file(res, 'tmp.txt')
        s.save_data_to_db(s.read_data_from_file('tmp.txt'))
        s.unchecked_today()
        s.unchecked_ytd()
        s.chk_days_top_10('Today')
        s.chk_rate_last_10("Today")
        s.team_points_last_days(1)
        s.untask_user_list(1, 8)
    
        teamName = '兰芷馥郁'
        teamID = 34543
        s = MemDataCommon(teamName, teamID)
        res = s.get_team_all_mem()
        s.save_data_to_file(res, 'tmp.txt')
        s.save_data_to_db(s.read_data_from_file('tmp.txt'))
        s.chk_days_top_10('Today')
        s.chk_rate_last_10("Today")
        pass
        
    except Exception as result:
        print("错误位置： %s" % result)
        
    
