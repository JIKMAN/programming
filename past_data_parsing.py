import sys

import happybase
from kafka import KafkaProducer

import time
import pandas as pd
from struct import unpack


reload(sys)
sys.setdefaultencoding('utf-8')

finance_categories = ['0301001', '0302001', '0302002', '0302006', '0303001', '0303003', '0303004', '0304001', '0304002', '0305001', '0306001', '0308001']
field_4bytes = ['contents:channelType', 'contents:documentCategory', 'contents:imageCount', 'contents:videoCount', 'contents:sourceType', 'contents:headlineType']
field_timestamp = ['contents:crawlTime', 'contents:createTime', 'contents:modifyTime']
servers = ['10.35.30.81:9092', '10.35.30.82:9092', '10.35.30.83:9092', '10.35.30.84:9092', '10.35.30.85:9092', '10.35.30.86:9092', '10.35.30.87:9092', '10.35.30.88:9092']

topic = 'unlisted_test'
start_time = time.time()


def past_data_parsing(start, stop):
    '''
    start and stop means first to last range of
    H-base's row-key what you want to scan
    '''
    row_start = "0" * (6 - len(start)) + start                              # format like "000000"
    row_stop = "0" * (6 - len(str(int(start) + 1))) + str(int(start) + 1)
    cnt = 0
    total = 0

    p = KafkaProducer(acks=0, bootstrap_servers=servers)

    for k in range(int(sys.argv[2]) - int(sys.argv[1]) + 1):
        conn = happybase.Connection(host='10.35.51.82', timeout=None, autoconnect=True)
        table = conn.table('T_NewsDocuments')
    
        for row_key, row_val in table.scan(row_start=row_start, row_stop=row_stop):   

            total += 1

            if row_val['identity:mode'] == 'D':
                continue

            if 'contents:zumCategoryList' not in row_val:
                continue

            flag = False
            for v in row_val['contents:zumCategoryList'].split(","):
                if v in finance_categories:
                    flag = True
            if flag == False:
                continue
            
            news_in = {}
            for k, v in row_val.items():
                if k.startswith('analysis') or k.startswith('morpheme'):
                    continue
                elif k == 'identity:indexable':
                    continue
                elif k in field_4bytes:
                    news_in[k] = str(unpack('>HH', v)[-1])
                elif k in field_timestamp:
                    news_in[k] = str(int(time.time()))
                else:
                    news_in[k] = v

            dic = pd.DataFrame([news_in])
            
            cols = dic.columns
            result = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<DOCUMENT>\n" # converting to xml format
            for i in cols:
                wrapper = str(i.split(":")[1])
                value = ','.join(dic[i].tolist())
                main = "<![CDATA[" + value  + "]]>"
                tmp = "<" + wrapper  + ">" + main + "<" + "/" + wrapper + ">\n"
                result += tmp
            result += "</DOCUMENT>"
            cnt += 1

            p.send(topic, value = result)
            #### debug point ####
            if cnt %  1000 == 0: 
                print("total: {}, from now: {}, key: {}".format(total, cnt, row_key))
                print(time.time() - start_time)
        print("row range : {} to {}".format(row_start, row_stop))

        row_start = row_stop
        row_stop = "0" * (6 - len(str(int(row_start) + 1))) + str(int(row_start) + 1)

        p.flush()
        
    print("total : {}, economy : {}".format(total, cnt))
    print(time.time() - start_time)


def main():
    past_data_parsing(sys.argv[1], sys.argv[2])


if __name__ == '__main__':
    main()
