import os
import datetime as dt
import traceback
from io import StringIO

import pandas as pd
from remotewind import w

import smtplib
from email.header import Header
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from history_sw_industry import update_sw_mat

def update_daily_sw(dateStr):
    stkcds = w.wset('sectorconstituent', 'date={};sectorid=a001010100000000;field=wind_code'.format(dateStr))
    stkcdsStr = ','.join(stkcds.Data[0])
    swinds = w.wss(stkcdsStr, 'industry_sw', 'industryType=4;tradeDate={}'.format(dateStr))
    output = {
        'stkcd': [],
        'swName1': [],
        'swName2': [],
        'swName3': [],
    }
    for dumi, stk in enumerate(swinds.Codes):
        print(dumi, stk)
        output['stkcd'].append(int(stk.split('.')[0]))
        if swinds.Data[0][dumi] is not None:
            swNames = swinds.Data[0][dumi].split('-')
            output['swName1'].append(swNames[0])
            output['swName2'].append(swNames[1])
            output['swName3'].append(swNames[2])
        else:
            output['swName1'].append(None)
            output['swName2'].append(None)
            output['swName3'].append(None)
    output = pd.DataFrame(output)
    output['swName2'] = output['swName2'].map(lambda x: x.strip('Ⅱ') if isinstance(x,str) else x)
    output['swName3'] = output['swName3'].map(lambda x: x.strip('Ⅲ') if isinstance(x,str) else x)
    dataPath = r'E:\stocks_data\sw_industry\sw_data'
    fileName = 'sw_industry_{}.csv'.format(dateStr)
    output.to_csv(os.path.join(dataPath, fileName),index=False)
    print('sw industry updated for date {}'.format(dateStr))


if __name__=='__main__':

    try:

        today = dt.datetime.today().strftime('%Y%m%d')
        isTradeDay = w.tdayscount(today,today).Data[0][0]==1
        # today = '20180709'

        if isTradeDay:
            update_daily_sw(dateStr=today)      # update today
            preTrdDay = w.tdaysoffset(-1, today).Data[0][0].strftime('%Y%m%d')
            update_daily_sw(dateStr=preTrdDay)  # patch new stocks of previous day
            update_sw_mat()

    except BaseException as e:
        fp = StringIO()
        traceback.print_exc(file=fp)
        msg = fp.getvalue()

        # 生成邮件
        now = dt.datetime.now()
        message = MIMEMultipart()
        message['From'] = Header("百泉投资", 'utf-8')
        message['Subject'] = Header('SW_INDUSTRY_{0}_ERROR!!!'.format(now), 'utf-8')
        message.attach(MIMEText('\n{}'.format(msg), 'plain', 'utf-8'))
        # 发送邮件
        sender = 'baiquaninvest@baiquaninvest.com'
        receivers = ['wangjp@baiquaninvest.com']
        smtpobj = smtplib.SMTP()
        smtpobj.connect(host='smtp.qiye.163.com',port=25)
        smtpobj.login(user=sender,password='Baiquan@1818')
        smtpobj.sendmail(sender,receivers,message.as_string())

        raise e

    else:

        # 生成邮件
        now = dt.datetime.now()
        message = MIMEMultipart()
        message['From'] = Header("百泉投资", 'utf-8')
        sub = 'SW_INDUSTRY_{0} updated sucessfully'.format(today) if isTradeDay else 'Not a trade day, sw not updated'
        message['Subject'] = Header(sub, 'utf-8')
        # 发送邮件
        sender = 'baiquaninvest@baiquaninvest.com'
        receivers = ['wangjp@baiquaninvest.com']
        smtpobj = smtplib.SMTP()
        smtpobj.connect(host='smtp.qiye.163.com',port=25)
        smtpobj.login(user=sender,password='Baiquan@1818')
        smtpobj.sendmail(sender,receivers,message.as_string())