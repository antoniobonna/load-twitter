
-- twitter.datetime

\! echo "Carregando dados na tabela datetime..."

INSERT INTO twitter_dw.datetime
SELECT distinct datetime, date(datetime), date_part('week', datetime),date_part('month', datetime),date_part('year', datetime)
	FROM twitter.tweets_stg WHERE datetime not in (SELECT datetime FROM twitter_dw.datetime)
ORDER BY 1;

VACUUM ANALYZE twitter_dw.datetime;

----------------------------------------------------------------------------

-- twitter.username

\! echo "Carregando dados na tabela username..."

INSERT INTO twitter_dw.username
SELECT distinct userid,username
FROM twitter.tweets_stg
EXCEPT
SELECT userid,username FROM twitter_dw.username
ORDER BY 1;

VACUUM ANALYZE twitter.username;

----------------------------------------------------------------------------

-- twitter_dw.tweet

\! echo "Carregando dados na tabela tweet..."

INSERT INTO twitter_dw.tweet(tweet)
SELECT distinct tweet
FROM twitter.tweets_stg
EXCEPT
SELECT tweet FROM twitter_dw.tweet
ORDER BY 1;

VACUUM ANALYZE twitter_dw.tweet;

----------------------------------------------------------------------------

-- twitter_dw.twitter

\! echo "Carregando dados na tabela fato twitter..."

COPY(
SELECT c.company_id, d.datetime, u.userid, t.tweet_id, polarity
	FROM twitter.tweets_stg f
	JOIN twitter_dw.datetime d ON d.datetime=f.datetime
	JOIN twitter_dw.company c ON c.company_id=f.company_id
	JOIN twitter_dw.username u ON u.userid=f.userid
	JOIN twitter_dw.tweet t ON t.tweet=f.tweet
) to '/home/ubuntu/dump/twitter.txt';
COPY twitter_dw.twitter FROM '/home/ubuntu/dump/twitter.txt';

VACUUM ANALYZE twitter_dw.twitter;
