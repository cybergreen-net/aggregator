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

## Export following env variables as in example:

```
CYBERGREEN_BUILD_ENV=dev 
RDS_PASSWORD=secret
REDSHIFT_PASSWORD=secret
CYBERGREEN_SOURCE_ROOT=s3://private-bits-cybergreen-net/myenv/myfeed
CYBERGREEN_DEST_ROOT=s3://bits.cybergreen.net/myenv/myfeed/myversion
AWS_ACCESS_KEY_ID=awSAccesKey
AWS_SECRET_ACCESS_KEY=AwsSecretKey
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
