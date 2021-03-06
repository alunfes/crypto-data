import requests
import asyncio

class LineNotification:
    @classmethod
    def initialize(cls):
        cls.__read_keys()
        cls.last_error = ''
        cls.line_api = "https://notify-api.line.me/api/notify"
        cls.headers = {"Authorization": "Bearer " + cls.token}
        print('initialized LineNotification')

    @classmethod
    def __read_keys(cls):
        file = open('./ignore/line.txt', 'r')  # 読み込みモードでオープン
        cls.token = file.readline().split(':')[1]
        file.close()


    @classmethod
    def send_error(cls, message):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(cls.__send_error(message))


    @classmethod
    async def __send_error(cls, message):
        if len(message) > 0:
            await cls.__send_message('\r\n'+str(message))


    @classmethod
    async def __send_message(cls, message):
        payload = {"message": message}
        try:
            res = requests.post(cls.line_api, headers=cls.headers, data=payload, timeout=(6.0))
        except Exception as e:
            print('Line notify error!={}'.format(e))



if __name__ == '__main__':
    LineNotification.initialize()
    LineNotification.send_error('Total API access reached 500/sec! sleep for 60sec')
    #LineNotification.send_message('\r\n'+'pl=-59'+'\r\n'+'num_trade=100')
