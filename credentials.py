from base64 import b64decode

database_login = {
    'DATABASE' : 'torkcapital',
    'USER' : 'postgres',
    'HOST' : 'ec2-18-229-106-103.sa-east-1.compute.amazonaws.com',
    'PASSWORD' : 'VG9ya0AyMDE4'
}

def setDatabaseLogin():
        return [database_login['DATABASE'], database_login['HOST'], database_login['USER'], b64decode(database_login['PASSWORD']).decode("utf-8")]

twitter_tokens = {
    'CONSUMER_KEY' : 'ZB95jjVzs5a799keFt7AYvRqR',
    'CONSUMER_SECRET' : 'BrbaSHiDhZ9C7ziIdoefcI0HRWkh3PK93aoI8JfTV8QFAc0YuE',
    'ACCESS_TOKEN' : '79050408-zsoXBGXkLtQzPOq1BZxFSo8QlTiyvKGIl1rvLjHg0',
    'ACCESS_TOKEN_SECRET' : 'SnxGb9MqlPErcdphvIc8BI1MGESsCUJGK1WPsf4SSD5j3'
}

def setTwitterTokens():
    return [twitter_tokens['CONSUMER_KEY'], twitter_tokens['CONSUMER_SECRET'], twitter_tokens['ACCESS_TOKEN'], twitter_tokens['ACCESS_TOKEN_SECRET']]