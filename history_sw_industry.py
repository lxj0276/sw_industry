
import os
import datetime as dt
import numpy as np
import pandas as pd
import scipy.io as scio

from remotewind import w





def get_dict(dtype='change'):
    if dtype=='change':
        swChangeLevel1 = pd.read_csv(r'E:\stocks_data\sw_industry\change_dict.csv', encoding='gbk')
        swChangeDict = {}
        for dumi in range(swChangeLevel1.shape[0]):
            if swChangeDict.get(swChangeLevel1['swName1Old'][dumi]) is None:
                swChangeDict[swChangeLevel1['swName1Old'][dumi]] = [swChangeLevel1['swName1New'][dumi]]
            else:
                swChangeDict[swChangeLevel1['swName1Old'][dumi]].append(swChangeLevel1['swName1New'][dumi])
        return swChangeDict
    else:
        swNameLevel1 = pd.read_csv(r'E:\stocks_data\sw_industry\sw_dict_level1.csv', encoding='gbk')
        swNameCodeDict = {swNameLevel1['swName1'][dumi]: swNameLevel1['swCode1'][dumi] for dumi in range(swNameLevel1.shape[0])}
        swNameCodeDict[np.nan] = -1
        return swNameCodeDict

class CONSTATNS:
    LATEST = 20180709
    swNameCodeDict = get_dict(dtype='name')
    swChangeDict = get_dict(dtype='change')


def date_trans(tdt):
    if isinstance(tdt,str):
        return int(dt.datetime.strptime(tdt,'%Y/%m/%d').strftime('%Y%m%d'))
    elif np.isnan(tdt):
        return CONSTATNS.LATEST

def history_sw_data():
    lastData = pd.read_csv(r'E:\stocks_data\sw_industry\sw_data\sw_industry_20180709.csv',encoding='gbk')
    lastStks = lastData['stkcd'].values
    histData = pd.read_csv(r'.\sw_history.csv',encoding='gbk')
    histData.columns = ['stkcd','exchg','stkname','standerd','indate','outdate','swName1','swName2','swName3','isnew']
    histData['stkcd'] = histData['stkcd'].map(lambda x:int(x))
    histData['indate'] = histData['indate'].map(date_trans)
    histData['outdate'] = histData['outdate'].map(date_trans)
    trdDates = scio.loadmat(r'E:\bqfcts\bqfcts\data\trddates.mat')['trddates'][:,0]
    stkInfo = scio.loadmat(r'E:\bqfcts\bqfcts\data\stkinfo.mat')['stkinfo'][:,[0,1,2]]    # stkcd and ipo date
    stkInfo[:,2] = CONSTATNS.LATEST
    offListed = sorted(list(set(stkInfo[:, 0]) - set(lastStks)))
    stkInfo = pd.DataFrame(stkInfo,columns=['stkcd','ipo_date','delist_date']).set_index('stkcd')
    offListedWind = ['{}.SH'.format(stkcd) if stkcd>=600000 else ''.join(['0'*(5-int(np.log10(stkcd))),'{}.SZ'.format(stkcd)]) for stkcd in offListed]
    offListedDate = [int(tdt.strftime('%Y%m%d')) for tdt in w.wss(offListedWind,'delist_date').Data[0]]
    stkInfo.loc[offListed,'delist_date'] = offListedDate
    stkInfo.reset_index(inplace=True)
    for tdt in trdDates:
        if tdt>=CONSTATNS.LATEST:
            break
        listedIdx = np.logical_and(stkInfo['ipo_date'].values<=tdt,stkInfo['delist_date'].values>=tdt)
        listedStocks = stkInfo.loc[listedIdx,'stkcd']
        histCut = histData.loc[np.isin(histData['stkcd'],listedStocks),:]
        histCutIdx = np.logical_and(histCut['indate'].values<=tdt,histCut['outdate'].values>tdt)
        histCut = histCut.loc[histCutIdx,['stkcd','swName1','swName2','swName3']]
        if histCut.empty:
            print('{} is empty'.format(tdt))
        histCut.to_csv(os.path.join(r'E:\stocks_data\sw_industry\sw_data','sw_industry_{}.csv'.format(tdt)),index=False)


def sw_leve1_code(swName1,swName2,stkcd,tdate,lastNoChange,firstChange):
    if swName1 in CONSTATNS.swNameCodeDict:
        code = CONSTATNS.swNameCodeDict[swName1]
    elif swName1 in CONSTATNS.swChangeDict:
        if len(CONSTATNS.swChangeDict[swName1])==1:     # 一一对应，直接返回即可
            code = CONSTATNS.swNameCodeDict[CONSTATNS.swChangeDict[swName1][0]]
        else:           # 旧名 一对多 新名，
            # 先通过 二级行业进行匹配
            name2Pair = np.array([name in swName2 for name in CONSTATNS.swChangeDict[swName1]])
            if swName2 in CONSTATNS.swChangeDict[swName1]:  # 二级行业名 变更为 新一级行业名
                code = CONSTATNS.swNameCodeDict[swName2]
            elif swName1 == '金融服务' and swName2 != '银行':  # 非银 特殊处理
                code = CONSTATNS.swNameCodeDict['非银金融']
            elif swName1 == '信息服务' and swName2=='网络服务':
                code = CONSTATNS.swNameCodeDict['计算机']
            elif np.any(name2Pair):  # 新一级行业 包含于 旧二级行业
                code = CONSTATNS.swNameCodeDict[CONSTATNS.swChangeDict[swName1][np.argwhere(name2Pair)[0][0]]]
            else:   # 二级行业匹配失败，按照行业变更后 该股票所属行业
                if stkcd in firstChange.index:       # 若该股票行业变动时还为退市，对照改名后该股票所在的行业
                    newName1 = firstChange.loc[stkcd,'swName1']
                    if newName1 in CONSTATNS.swChangeDict[swName1]:   # 变更后的行业名 处在 变更字典中
                        code = CONSTATNS.swNameCodeDict[newName1]
                    else:       # 变更后股票未退市，但是该股票发生行业变更，且变更后行业 不属于变更字典，需要特殊处理
                        code = np.nan
                else:   # 行业变更时 股票已经退市 匹配失败
                    code = np.nan
    else:   #
        code = np.nan
    if tdate<=20131231:         # 使变更前的行业恢复的更加均衡
        if (stkcd in firstChange.index) and (stkcd in lastNoChange.index):
            newName1 = firstChange.loc[stkcd, 'swName1']
            code = CONSTATNS.swNameCodeDict[newName1] if lastNoChange.loc[stkcd,'swName1']==swName1 else code   # 与变更前对后一天相同的行业，使用变更后的代码
    return code


def update_sw_mat():

    trdDates = scio.loadmat(r'E:\bqfcts\bqfcts\data\trddates.mat')['trddates'][:, 0]
    stkCodes = scio.loadmat(r'E:\bqfcts\bqfcts\data\stkinfo.mat')['stkinfo'][:, 0]
    ##### update mat #####
    swPath = r'E:\stocks_data\sw_industry\sw_data'
    histMatName = 'data_19901219_20170630'
    histPath = os.path.join(r'E:\bqfcts\bqfcts\data\SW_Industry','{}.mat'.format(histMatName))
    if not os.path.exists(histPath):
        histStkNum = 3433
        histDayNum = 6488
        histStks = stkCodes[:histStkNum]
        histTrds = trdDates[:histDayNum]
        histMat = pd.DataFrame(np.zeros([histStkNum,histDayNum]),index=histStks,columns=histTrds)
        firstChange = pd.read_csv(os.path.join(swPath,'sw_industry_20140102.csv'), encoding='gbk').set_index('stkcd')
        lastNoChange = pd.read_csv(os.path.join(swPath, 'sw_industry_20131231.csv'), encoding='gbk').set_index('stkcd')
        for tdt in histTrds:
            swData = pd.read_csv(os.path.join(swPath,'sw_industry_{}.csv'.format(tdt)),encoding='gbk').set_index('stkcd')
            swCode1 = []
            for stkcd in swData.index.values:
                swName1 = swData.loc[stkcd, 'swName1']
                swName2 = swData.loc[stkcd, 'swName2']
                swCode1.append(sw_leve1_code(swName1=swName1,
                                             swName2=swName2,
                                             stkcd=stkcd,
                                             tdate=tdt,
                                             lastNoChange=lastNoChange,
                                             firstChange=firstChange))
            swData['swCode1'] = swCode1
            histMat.loc[swData.index,tdt] = swData['swCode1']
            print(tdt)
        scio.savemat(file_name=histPath, mdict={'swIndustry': histMat.values})
        print('hist mat created')
    currMatName = 'data_20150701_now'
    currPath = os.path.join(r'E:\bqfcts\bqfcts\data\SW_Industry', '{}.mat'.format(currMatName))
    currDayStart = 6000
    if not os.path.exists(currPath):
        currTrds = trdDates[currDayStart:]
        currMat = pd.DataFrame(np.zeros([stkCodes.shape[0], currTrds.shape[0]]), index=stkCodes, columns=currTrds)
        for tdt in currTrds:
            swData = pd.read_csv(os.path.join(swPath,'sw_industry_{}.csv'.format(tdt)),encoding='gbk').set_index('stkcd')
            swData['swCode1'] = swData['swName1'].map(CONSTATNS.swNameCodeDict)
            currMat.loc[swData.index,tdt] = swData['swCode1']
            print(tdt)
        scio.savemat(file_name=currPath, mdict={'swIndustry':currMat.values})
        print('curr mat created')
    else:
        currDates = trdDates[currDayStart:]
        currStkcds = stkCodes
        currDayNum = currDates.shape[0]
        currStkNum = currStkcds.shape[0]
        currMatSaved = scio.loadmat(currPath)['swIndustry']
        (savedStkNum, savedDayNum) = currMatSaved.shape
        if (currDayNum == savedDayNum) and (currStkNum == savedStkNum):
            print('no data to update')
            return
        currTrds = currDates[currDayNum-2:]     # 前一天的重新更新，弥补新股
        currMat = pd.DataFrame(np.zeros([currStkNum, currDayNum-savedDayNum+1]), index=stkCodes,columns=currTrds)
        for tdt in currTrds:
            swData = pd.read_csv(os.path.join(swPath, 'sw_industry_{}.csv'.format(tdt)), encoding='gbk').set_index('stkcd')
            swData['swCode1'] = swData['swName1'].map(CONSTATNS.swNameCodeDict)
            currMat.loc[swData.index, tdt] = swData['swCode1']
        patch = np.zeros([currStkNum-savedStkNum, savedDayNum - 1])
        currMat = np.column_stack([np.row_stack([currMatSaved[:,:-1],patch]), currMat.values])
        scio.savemat(file_name=currPath, mdict={'swIndustry': currMat})
        print('curr mat updated')

if __name__=='__main__':
    update_sw_mat()
    # pr = scio.loadmat(r'C:\Users\Jiapeng\Desktop\matlab.mat')['t']
    # old = pd.read_csv(r'E:\stocks_data\sw_industry\sw_data\sw_industry_20131231.csv',encoding='gbk').set_index('stkcd')
    # new = pd.read_csv(r'E:\stocks_data\sw_industry\sw_data\sw_industry_20140102.csv',encoding='gbk').set_index('stkcd')
    # data = pd.concat([old.loc[pr[:,0],'swName1'],new.loc[pr[:,0],'swName1']],axis=1)
    # data.to_csv(r'E:\stocks_data\sw_industry\tempdict.csv')
