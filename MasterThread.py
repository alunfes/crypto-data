from CryptowatchDataGetter import CryptowatchDataGetter
from WebsocketMaster import TickData
from S3Master import S3Master

class MasterThread:
    def start(self):
        S3Master.initialize()
        S3Master.remove_trush()
        #CryptowatchDataGetter.initialize()
        TickData.initialize()

if __name__ == '__main__':
    mt = MasterThread()
    mt.start()