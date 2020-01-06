import os
import psycopg2
import credentials
import csv
from subprocess import call
from textblob import TextBlob
from unidecode import unidecode
from datetime import date,timedelta
import GetOldTweets3 as got
import boto3

DATABASE, HOST, USER, PASSWORD = credentials.setDatabaseLogin()
# CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET = credentials.setTwitterTokens()

### variaveis
outdir = '/home/ubuntu/scripts/load-dados-twitter/csv/'
file = 'twitter.csv'
query_company_id = "SELECT company_id FROM twitter_dw.company"
tablename = 'twitter.tweets_stg'
current_date = str(date.today())
yesterday = str(date.today() - timedelta(days=1))

### conecta no banco de dados
db_conn = psycopg2.connect("dbname='{}' user='{}' host='{}' password='{}'".format(DATABASE, USER, HOST, PASSWORD))
cursor = db_conn.cursor()
print('Connected to the database')

cursor.execute(query_company_id)
ids = [item[0] for item in cursor.fetchall()]
cursor.close()
db_conn.close()

# ###Autenticando no Twitter
# auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
# auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
# api = tweepy.API(auth,wait_on_rate_limit=True)

translate = boto3.client(service_name='translate',use_ssl=True, region_name='us-east-2') ### api de tradução do AWS

with open(outdir+file,'w', newline="\n", encoding="utf-8") as ofile:
    writer = csv.writer(ofile, delimiter=';')
    for id in ids:
        print('Parsing '+id+'...')
        tweetCriteria = got.manager.TweetCriteria().setQuerySearch(id).setSince(yesterday).setUntil(current_date).setTopTweets(True)
        tweets = got.manager.TweetManager.getTweets(tweetCriteria)
        for tweet in tweets:
            try:
                if tweet.username.lower() not in id.lower() and tweet.retweets == 0: ### ignorar RT e tweets da propria empresa
                    data = str(tweet.date)[:19]
                    # user
                    userid = tweet.id
                    username = tweet.username 
                    #Texto do Tweet
                    textPT = tweet.text
                    #Traduzindo para o Inglês
                    result = translate.translate_text(Text=textPT,SourceLanguageCode="pt", TargetLanguageCode="en")
                    textEN = result.get('TranslatedText')
                    sentiment = TextBlob(textEN)
                    writer.writerow([id,data,userid,username,textPT,sentiment.polarity])
            except:
                pass

### conecta no banco de dados
db_conn = psycopg2.connect("dbname='{}' user='{}' host='{}' password='{}'".format(DATABASE, USER, HOST, PASSWORD))
cursor = db_conn.cursor()

### copy
with open(outdir+file, 'r') as ifile:
    SQL_STATEMENT = "COPY %s FROM STDIN WITH CSV DELIMITER AS ';' NULL AS ''"
    print("Executing Copy in "+tablename)
    cursor.copy_expert(sql=SQL_STATEMENT % tablename, file=ifile)
    db_conn.commit()
cursor.close()
db_conn.close()
os.remove(outdir+file)

### VACUUM ANALYZE
call('psql -d torkcapital -c "VACUUM VERBOSE ANALYZE '+tablename+'";',shell=True)
cursor.close()
db_conn.close()