

import json
import dateutil.parser


from confluent_kafka import Consumer
# host = 'rc1a-2ar1hqnl386tvq7k.mdb.yandexcloud.net:9091', 'rc1a-b5e65f36lm3an1d5.mdb.yandexcloud.net:9091'
cfg = {
    'bootstrap.servers': 'rc1a-b5e65f36lm3an1d5.mdb.yandexcloud.net:9091',
    'group.id': 'ExhausHub',
    'auto.offset.reset': 'earliest',
    'security.protocol': 'SASL_SSL',
    'ssl.ca.location' : 'CA.pem',
    'sasl.username' : '9433_reader',
    'sasl.password': 'eUIpgWu0PWTJaTrjhjQD3.hoyhntiK',
    'sasl.mechanisms':'SCRAM-SHA-512'
}
SM_Exgauster = []
C = Consumer(cfg)
C.subscribe(['zsmk-9433-dev-01'])
spisok = []
for _ in range(100):
    msg = C.poll(1)


    if msg:
        dat = {
            'msg_value': msg.value(),
            # 'msg_headers': msg.headers(),
            # 'msg_key': msg.key(),
            # 'msg_partition': msg.partition(),
            # 'msg_topic': msg.topic(),
        }


        rtn = json.loads(dat['msg_value'])
        trat = [dateutil.parser.isoparse((rtn['moment'])[0:19]), (str(rtn['SM_Exgauster\\[0:33]']))[0:4],  (str(rtn['SM_Exgauster\\[0:33]']))[0:4]]
        SM_Exgauster.append(trat)
C.close()
a = SM_Exgauster[0]
b=a[0]
print(b)
c = str(b)
print(c)
print(len(SM_Exgauster))
print(SM_Exgauster[0])

