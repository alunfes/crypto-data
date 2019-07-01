import boto3
import datetime

class S3Master:
    @classmethod
    def initialize(cls):
        cls.__bucket_name = 'fx-btc-jpy'

    @classmethod
    def get_file_list(cls):
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(cls.__bucket_name)
        #for object in bucket.objects.all():
            #print(object)
        return bucket.objects.all()

    @classmethod
    def save_file(cls, file_name):
        try:
            s3 = boto3.resource('s3')
            save_name =str(file_name).split('/')[2]
            print(save_name)
            s3.Object(cls.__bucket_name, save_name).upload_file(file_name)
            return 0
        except Exception as e:
            print('s3 bot trade log save file error!={}'.format(e))
            return -1

    @classmethod
    def remove_file(cls, file_name):
        files = cls.get_file_list()
        s3_client = boto3.client('s3')
        if files is not None:
            for f in files:
                if file_name in str(f.key):
                    print('removed ', f.key)
                    s3_client.delete_object(Bucket=cls.__bucket_name, Key=f.key)

    @classmethod
    def remove_all_files(cls):
        files = cls.get_file_list()
        s3_client = boto3.client('s3')
        if files is not None:
            for f in files:
                print('removed ', f.key)
                s3_client.delete_object(Bucket=cls.__bucket_name, Key=f.key)

    #remove s3 trush log files
    @classmethod
    def remove_trush(cls):
        files = cls.get_file_list()
        s3_client = boto3.client('s3')
        if files is not None:
            for f in files:
                if str(f.key)[0:4] == str(datetime.datetime.today().year):
                    print('removed ', f.key)
                    s3_client.delete_object(Bucket=cls.__bucket_name, Key=f.key)

if __name__ == '__main__':
    S3Master.initialize()
    S3Master.save_file('./Data/executions_0.csv')
