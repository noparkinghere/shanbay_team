#!/usr/bin/python
# -*- coding: UTF-8 -*-
# 小组成员信息获取，数据筛选模块
# 本程序主要调用扇贝提供的 API 来帮助进行群组的管理
# 部分功能采用爬虫技术


import requests
import json
import time
import os
import copy

# 获取全体组员打卡信息
class ShanbayMemData:
  START_TIME = 'start_time'       # 开始时间的常量，只用来判断
  END_TIME = 'end_time'           # 结束时间的常量，只用来判断
  StartTime = ''                  # 计时器，程序开始时间
  EndTime = ''                    # 计时器，程序结束时间
  cnt = 0   # test code
  data = {}                       # 存储从扇贝获取数据后的初步处理后的有效数据，包括小组信息和用户信息
  members = []                    # 为 data 字典中一部分，主要暂存用户信息
  validData = {}                  # data 的数据存放到文件中，再从文件中读取放入到 validData 变量中。
  filterItems = {'rank':'rank', 'points':'points', 'age':'age', 'chkDay':'checkin_days', 'chkRate':'checkin_rate'}   # 可用来进行筛选的几个数据项

  # 存储文件名称的相关变量
  fileNameDate = time.strftime('%Y%m%d', time.localtime())        # 获取本地时间的元组转换为字符串
  dirNameYear = fileNameDate[-8:-4]                               # 文件名中的年份
  dirNameMonth = fileNameDate[-4:-2]                              # 文件名中的月份
  ORG_DATA = 'org_data_'                                          # 存储扇贝 api 获取的原始数据文件前缀，暂未使用
  FILTER_DATA = 'data_'                                           # 从扇贝 api 处理后的有效数据文件前缀
  
  # 初始化数据
  def __init__(self, teamName = '35K', teamID = 10879):
    self.teamName = teamName
    self.teamID = teamID
    self.StartTime = time.time()

    # 在上级目录下新建 data 目录，存在则进入该目录，否则创建该目录
    os.chdir('..')
    os.chdir('..')

    for i in os.listdir(os.getcwd()):
      if i == 'data':
        os.chdir('data')  # 迁移到指定目录
        break
    else:
        os.mkdir('data')
        os.chdir('data')  # 迁移到指定目录

    self.CurFileDir = os.getcwd()
    print(self.CurFileDir)

    # 如果已经存在小组名，则进入该目录，否则创建该目录
    for i in os.listdir(os.getcwd()):
      if i == self.teamName:
        os.chdir(self.teamName)   # 迁移到指定目录
        break
    else:
      os.mkdir(self.teamName) 
      os.chdir(self.teamName)   # 迁移到指定目录

    # 如果已经存在该年份目录，则进入该目录，否则创建该目录
    for i in os.listdir(os.getcwd()):
      if i == self.dirNameYear:
        os.chdir(self.dirNameYear)   # 迁移到指定目录
        break    
    else:
      os.mkdir(self.dirNameYear) 
      os.chdir(self.dirNameYear)   # 迁移到指定目录    

    # 如果已经存在该月份目录，则进入该目录，否则创建该目录
    for i in os.listdir(os.getcwd()):
      if i == self.dirNameMonth:
        os.chdir(self.dirNameMonth)   # 迁移到指定目录
        break    
    else:
      os.mkdir(self.dirNameMonth) 
      os.chdir(self.dirNameMonth)   # 迁移到指定目录      


  # 保存数据函数,data ： 实际数据；
  # date：文件名中的日期，具体日期格式如 19700101
  # fileType：前缀表示文件类型
  def SaveDataToFile(self, data, date=fileNameDate, fileType=FILTER_DATA):
    # 保存原始数据
    fileName = fileType + date
    with open(fileName, 'w', encoding='utf-8') as f:
      f.write(json.dumps(data))
    
    
  # 从文件中读取数据，返回结果给 validData，后面数据筛选大都从这边读原始数据
  # date：文件名中的日期，具体日期格式如 19700101
  # fileType：前缀表示文件类型
  def ReadDataFromFile(self, date=fileNameDate, fileType=FILTER_DATA):
    fileName = fileType + date
    with open(fileName, 'r', encoding='utf-8') as f:
      res = json.loads(f.read())
    self.validData = res
    # print(self.validData)


  # 通过扇贝提供的 api 获得小组成员的全部打卡信息
  # 对这个信息进行筛选，只保留个人数据相关的有效信息，存储到本地文件中
  def GetTeamAllMemMsg(self): 
    # 获取末尾数据，获得实际人数是否读 api 可用等信息
    url = "https://www.shanbay.com/api/v1/team/"+ str(self.teamID) + "/member/?page="
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
    # for i in range(1, 3):     # test code
      url = "https://www.shanbay.com/api/v1/team/10879/member/?page="
      kw = str(i)
      response = requests.get(url+kw, headers=headers)
      jdata = json.loads(response.content.decode('utf-8'))      # api 获得的原始用户数据
      
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
            "checkin_rate":jdata['data']['members'][i]['checkin_rate']
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
            "checkin_rate":jdata['data']['members'][i]['checkin_rate']
            }
          self.members.append(SigMemMsg)    
      time.sleep(0.2)       # 疑似远程主机会关闭链接获取数据，防止被反爬
      self.cnt += 1    # test code
      print(self.cnt)  # test code
      
    self.data['MemInfo'] = self.members                         # members 是 data 中的主要数据内容
    self.SaveDataToFile(self.data, date=self.fileNameDate, fileType=self.FILTER_DATA)


  # 筛选出前 n 的数据，item 只能是 filterItems 中的一个值，且不能为 "checkin_rate"
  def filterTopNum(self, tdata, num, item):
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
    
  
  # 筛选出后 n 的数据，item 只能是 filterItems 中的一个值，且不能为 "checkin_rate"
  def filterBtmNum(self,tdata, num, item):
    res = []
    tmpData = copy.deepcopy(tdata)
    for i in range(num):
      tmpCmp = tmpData[0][item]
      tmpPos = int()
      for j in range(len(tmpData)):
        if tmpCmp > tmpData[j][item]:
          tmpCmp = tmpData[j][item]
          tmpPos = j
      else:
        res.append(tmpData.pop(tmpPos))
    return res 


  # 打卡率筛选最低的 n 组数据
  def filterChkRate(self, tdata, num):
    res = []
    tmpData = copy.deepcopy(tdata)
    # print(tmpData)
    for i in range(num):
      tmpCmp = float(tmpData[0]["checkin_rate"][:-1])
      tmpPos = int()
      tmpCnt = -1
      for j in tmpData:
        tmpCnt += 1
        if tmpCmp > float(j["checkin_rate"][:-1]):
          # print('*')
          tmpCmp = float(j["checkin_rate"][:-1])
          tmpPos = tmpCnt 
          # print(tmpCmp)
      else:
        # print(tmpData.pop(tmpPos))
        # print('*'*20)
        res.append(tmpData.pop(tmpPos))
    return res   
    
    
  # 获取组龄低于 n 天的用户
  def GetAgeBelowNum(self,tdata, age):
    res = []
    tmpData = copy.deepcopy(tdata)
    # 默认是贡献值排名，因此测试后 120 名用户即可
    for i in range(len(tmpData)-120, len(tmpData)):
      if tmpData[i]['age'] <= age:
        res.append(tmpData[i])
    # s.SaveDataToFile(res, fileType='tmp2') # test code
    return res


  # 获取组龄为 n 天的用户
  def GetAgeNum(self, tdata, age):
    res = []
    tmpData = copy.deepcopy(tdata)
    # 默认是贡献值排名，因此测试后 120 名用户即可
    for i in range(0, len(tmpData)):
      if tmpData[i]['age'] == age:
        res.append(tmpData[i])
    # s.SaveDataToFile(res, fileType='tmp2') # test code
    return res


  # 基于本地数据的指定起始和结尾日期，这段时间之间，各个用户数据的差值
  # item item 只能是 filterItems 中的一个值，key 差值名称
  # startDate, endDate 的格式为 19970101，个位数日期用 0 补全
  def calThisPeriodData(self, startDate, endDate, item, key):
    os.chdir(self.CurFileDir)  # 回归当前文件目录路径
    os.chdir(self.teamName)     # 跳转到指定小组名称路径
    os.chdir(startDate[-8:-4])  # 迁移到指定目录
    os.chdir(startDate[-4:-2])  # 迁移到指定目录
    posMin = self.FILTER_DATA + startDate
    with open(posMin, 'r', encoding='utf-8') as f:
      dataMin = json.loads(f.read())['MemInfo']

    os.chdir(self.CurFileDir)  # 回归当前文件目录路径
    os.chdir(self.teamName)     # 跳转到指定小组名称路径
    os.chdir(endDate[-8:-4])  # 迁移到指定目录
    os.chdir(endDate[-4:-2])  # 迁移到指定目录
    posMax = self.FILTER_DATA + endDate
    with open(posMax, 'r', encoding='utf-8') as f:
      dataMax = json.loads(f.read())['MemInfo']

    res = []
    pos = 0
    for i in dataMax:
      for j in dataMin:
        if i['id'] == j['id']:
          res.append(i)
          res[pos][key] = i[item] - j[item]
          dataMax.remove(i)
          dataMin.remove(j)
          pos += 1

    return res


  # 基于本地数据的本月数据筛选，item 进行筛选的项目，key 差值名称
  # 计算当月截止目前为止，首尾天数中各个用户数据的差值
  # 首日未必是 1，尾日也未必是 30，而是选择文件夹中最大和最小的进行计算
  def calThisMonthData(self, item, key):
    os.chdir(self.CurFileDir)
    os.chdir(self.teamName)
    os.chdir(self.dirNameYear)  # 迁移到指定目录
    os.chdir(self.dirNameMonth)  # 迁移到指定目录
    # 获取最小日期的文件和最大日期的文件名称
    max = 0
    min = 32
    posMax = int()
    posMin = int()
    for i in os.listdir(os.getcwd()):
      if i[0:5] == self.FILTER_DATA:
        if max < int(i[-2:]):
          max = int(i[-2:])
          posMax = i
        if min > int(i[-2:]):
          min = int(i[-2:])
          posMin = i

    with open(posMax, 'r', encoding='utf-8') as f:
      dataMax = json.loads(f.read())['MemInfo']
    with open(posMin, 'r', encoding='utf-8') as f:
      dataMin = json.loads(f.read())['MemInfo']

    res = []
    pos = 0
    for i in dataMax:
      for j in dataMin:
        if i['id'] == j['id']:
          res.append(i)
          res[pos][key] = i[item] - j[item]
          dataMax.remove(i)
          dataMin.remove(j)
          pos += 1

    return res


  # 删除某指定文件或文件夹
  def DelOldDataFile(self, fileName):
    if os.path.isdir(fileName):
      os.rmdir(fileName)
    elif os.path.isfile(fileName):
      os.remove(fileName)


  # 删除从开始日期到截止日期之间的旧文件旧文件
  # 未经严格测试，慎用
  def DelOldDataFile(self, startDate='', endDate=''):
    os.chdir(self.CurFileDir)  # 回归当前文件目录路径
    os.chdir(self.teamName)     # 跳转到指定小组名称路径

    # 从 startDate 文件开始一直往后删，直到遇到 endDate 文件为止
    tmpDate = startDate
    os.chdir(self.FILTER_DATA+tmpDate[-8:-4])  # 跳转到指定年目录
    os.chdir(self.FILTER_DATA+tmpDate[-4:-2])  # 跳转到指定月目录
    while tmpDate != endDate:
      for i in os.listdir(os.getcwd()):
        tmpDate = int(i[-8:])
        if int(startDate) <= int(tmpDate) and int(tmpDate) <= int(endDate):
          self.DelOldDataFile(i)
      else:   # 循环遍历下一个目录
        os.chdir('..')        # 跳转到上一层目录
        tmpDate = tmpDate[-8:-4] + str(int(tmpDate[-4:-2])+1) + '00'  # 如 20191205 -> 20191300
        if int(tmpDate[-4:-2]) <= 12:
          os.chdir(tmpDate[-4:-2])  # 跳转到指定月目录
        else:
          tmpDate = str(int(tmpDate[-8:-4]  + 1)) + '01' + '00'
          os.chdir('..')
          os.chdir(tmpDate[-8:-4])  # 跳转到指定月目录
          os.chdir(tmpDate[-4:-2])  # 跳转到指定月目录

    # 遍历删除空文件夹
    os.chdir(self.CurFileDir)  # 回归当前文件目录路径
    os.chdir(self.teamName)     # 跳转到指定小组名称路径
    for i in os.listdir(os.getcwd()):
      if os.path.isdir(i):
        if len(os.listdir(i))==0:
          self.DelOldDataFile(i)
        else:
          os.chdir(i)
          for i in os.listdir(os.getcwd()):
            if os.path.isdir(i) and len(os.listdir(i)) == 0:
              self.DelOldDataFile(i)


  #*****************************************************************************
  # 以下代码都是常用操作的示例代码，是对上面一些功能函数的组合使用，方便用户调用
  # 实际使用时，更建议根据自己的需求对上面的代码进行组合调用，例如：想要获取年贡献榜前 10
  # 的用户，则可以在 calThisPeriodData 输入 20190101 ，20191231 和 points
  #*****************************************************************************
  # 获取打卡天数前十的用户
  def GetChkDaysTop10(self):
    return(self.filterTopNum(self.validData['MemInfo'], 10, 'checkin_days'))
    
    
  # 从指定日期的文件中获取组龄低于 21 天的用户
  def GetAgeBelow21(self):
    return(self.GetAgeBelowNum(self.validData['MemInfo'], 21))

  # 获得组龄为 100 天的用户，仅作为案例可以设置为其他日子的用户
  def GetAge100(self):
    return(self.GetAgeNum(self.validData['MemInfo'], 100))

  # 筛选出满整百天数的人
  def GetAgeEach100(self):
    res = {}
    for i in range(20):
      i += 1
      # s.SaveDataToFile(self.GetAgeNum(self.validData['MemInfo'], 100*i), fileType='Age_'+str(100*i)+'_')
      res[i*100] = self.GetAgeNum(self.validData['MemInfo'], 100*i)
    return res

  # 从指定日期的文件中获取贡献值前十用户
  def GetTopPoints10(self):
    return(self.filterBtmNum(self.validData['MemInfo'], 10, 'points'))
  
  
  # 从指定日期的文件中获取组龄前十用户
  def GetTopAge10(self):
    return(self.filterBtmNum(self.validData['MemInfo'], 10, 'age'))
  
    
  # 从指定日期的文件中获取组打卡天数前十用户
  def GetTopChkDays10(self):
    return(self.filterBtmNum(self.validData['MemInfo'], 10, 'checkin_days'))  
  
  
  # 从指定日期的文件中获取打卡率后十用户
  def GetChkRateBtm10(self):
    return(self.filterChkRate(self.validData['MemInfo'], 10))

  
  # 本月用户贡献排名前十用户
  def calPointMonthTop10(self):
    res = self.filterTopNum(self.calThisMonthData(s.filterItems['points'], 'gongxian'), 10, 'gongxian')
    s.SaveDataToFile(res, fileType='tmp1')
  
  
  # 本月排名进步最快前十用户
  def calRankMonthTop10(self):
    res = self.filterBtmNum(self.calThisMonthData(s.filterItems['rank'], 'paiming'), 10, 'paiming')
    s.SaveDataToFile(res, fileType='tmp2')
  
  
  # 本月被移除用户信息
  # 预留，功能未必在这边实现
  def DelMemlist():
    pass
    
  
  # 计时函数，统计整个程序执行的时间
  # 时间尤其是 api 爬取用户数据，整个需要控制在有效范围内，否则发送警报
  def TimeCnt(self, state):
    if state == self.START_TIME:
      self.StartTime = time.time()
    elif state == self.END_TIME:
      self.EndTime = time.time()
      
    optTime = self.EndTime-self.StartTime
    print('Getting Data from Shanbay takes '+str(optTime)+' s')
    return optTime


  # 多层筛选，从某预处理过的文件中再次筛选出数据
  # 预留用户外部可自己调用几个函数组合。
  # 实际这个函数不再补充，用户自行组合调用以上功能即可
  def DIYFilterData(self):
    pass


if __name__ == '__main__':
  # teamName = '35K'
  # teamID = 10879
  # s = ShanbayMemData(teamName, teamID)
  s = ShanbayMemData()
  s.GetTeamAllMemMsg()
  s.ReadDataFromFile()
  
  tmp = s.GetChkDaysTop10()
  # s.SaveDataToFile(tmp, fileType='tmp')
  
  tmp = s.GetChkRateBtm10()
  # s.SaveDataToFile(tmp, fileType='tmp1')
  s.GetAgeBelow21()
  s.TimeCnt(s.END_TIME)
  s.calPointMonthTop10()
  s.calRankMonthTop10()
  print(s.GetAgeEach100())
