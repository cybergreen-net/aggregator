Aggregator for CyberGreen risk data.

## Architecture

```
  Disaggregated  --> Analytics --> Aggregate -->   Aggregate ---> Frontend DB
      Data           Datastore       via SQL          Data
  [private S3]      [Redshift]    [in Redshift]    [public S3]       [RDS]
```

## Source Data

private-bits-cybergreen-net/dev/clean/...

Example - from OpenNTP data
```
date,risk,ip,asn,place
2016-08-05 02:00:06.0+00,2,69.2.0.0,27005,US
```

## Installation

Install tooling:

* python

## Set up .env.json as in example:

```
{
"REDSHIFT_PASSWORD":<<passwordForRedshift>>,
"REDSHIFT_DBNAME":"dev",
"REDSHIFT_USER":"cybergreen",
"REDSHIFT_HOST":"cg-analytics.cqxchced59ta.eu-west-1.redshift.amazonaws.com",
"REDSHIFT_PORT":5439,
"REDSHIFT_ROLE_ARN":"arn:aws:iam::635396214416:role/RedshiftCopyUnload",
"RDS_PASSWORD": <<passwordForRDS>>,
"RDS_DBNAME":"frontend",
"RDS_USER":"cybergreen",
"RDS_HOST":"cg-stats-dev.crovisjepxcd.eu-west-1.rds.amazonaws.com",
"RDS_PORT":5432,
"SOURCE_S3_BUCKET": "private-bits-cybergreen-net",
"SOURCE_S3_KEY": "dev/clean/",
"DEST_S3_BUCKET": "bits.cybergreen.net",
"DEST_S3_KEY": "stats/latest/",
"AWS_ACCESS_KEY": <<AwsAccessKeYGoesHERE>>,
"AWS_ACCESS_SECRET_KEY": <<AwsAccessSEcretKeYGoesHerE>>
}
```

## To aggregate data and Load to RDS db
```
$ pip install -r requirements.txt
$ python main.py
```

If you want, you can access Redshift or RDS directly via psql to take a look at the data:

```
# Redshift
$ psql -h cg-analytics.cqxchced59ta.eu-west-1.redshift.amazonaws.com --port 5439 --user cybergreen -d dev
# RDS
$ psql -h cg-stats-dev.crovisjepxcd.eu-west-1.rds.amazonaws.com -U cybergreen -d frontend -p 5432

```

## Testing

### Set up testing environment

#### 1. Install test requirements

```
$ pip install -r tests/requirements.txt
```

#### 2. Create local postgres user and database

```
$ psql -U postgres -c "create user cg_test_user password 'secret' createdb;"
$ psql -U postgres -c "create database cg_test_db owner=cg_test_user;"
```

### Launch tests

From git root dir launch all tests with nosetests:

```
$ nosetests
```
