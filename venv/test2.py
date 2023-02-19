





import json
import dateutil.parser



list_Exh = []

try:
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
            print(111111111111111111111111)
            sp_Exh = [dateutil.parser.isoparse((rtn['moment'])[0:19]), (str(rtn['SM_Exgauster\\[2:27]']))[0:4],
                      (str(rtn['SM_Exgauster\\[2:1]']))[0:4], (str(rtn['SM_Exgauster\\[2:28]']))[0:4],
                      (str(rtn['SM_Exgauster\\[2:4]']))[0:4], (str(rtn['SM_Exgauster\\[2:29]']))[0:4],
                      (str(rtn['SM_Exgauster\\[2:30]']))[0:4], (str(rtn['SM_Exgauster\\[2:31]']))[0:4],
                      (str(rtn['SM_Exgauster\\[2:32]']))[0:4], (str(rtn['SM_Exgauster\\[2:33]']))[0:4],
                      (str(rtn['SM_Exgauster\\[2:7]']))[0:4], (str(rtn['SM_Exgauster\\[2:34]']))[0:4],
                      (str(rtn['SM_Exgauster\\[2:10]']))[0:4], (str(rtn['SM_Exgauster\\[2:10]']))[0:4],
                      (str(rtn['SM_Exgauster\\[2:50]']))[0:4], (str(rtn['SM_Exgauster\\[2:19]']))[0:4],
                      (str(rtn['SM_Exgauster\\[2:51]']))[0:4], (str(rtn['SM_Exgauster\\[2:22]']))[0:4],
                      (str(rtn['SM_Exgauster\\[0:33]']))[0:4], (str(rtn['SM_Exgauster\\[0:7]']))[0:4],
                      (str(rtn['SM_Exgauster\\[0:34]']))[0:4], (str(rtn['SM_Exgauster\\[0:10]']))[0:4],
                      (str(rtn['SM_Exgauster\\[0:50]']))[0:4], (str(rtn['SM_Exgauster\\[0:19]']))[0:4],
                      (str(rtn['SM_Exgauster\\[0:51]']))[0:4], (str(rtn['SM_Exgauster\\[0:22]']))[0:4],
                      (str(rtn['SM_Exgauster\\[3:33]']))[0:4], (str(rtn['SM_Exgauster\\[3:7]']))[0:4],
                      (str(rtn['SM_Exgauster\\[3:34]']))[0:4], (str(rtn['SM_Exgauster\\[3:10]']))[0:4],
                      (str(rtn['SM_Exgauster\\[3:50]']))[0:4], (str(rtn['SM_Exgauster\\[3:19]']))[0:4],
                      (str(rtn['SM_Exgauster\\[3:51]']))[0:4], (str(rtn['SM_Exgauster\\[3:22]']))[0:4]]
            print(222222222222222222222222222222222)
            list_Exh.append(sp_Exh)
            print(33333333333333333333333333333333)

    C.close()

except Exception:  # отлавливаю потерю связи с БД и исключения и игнорирую их
    pass
finally:
    pass

