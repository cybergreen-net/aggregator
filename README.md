Aggregator for CyberGreen risk data.

## Architecture

```
  Disaggregated  --> Analytics --> Aggregate -->   Aggregate ---> Frontend DB
      Data           Datastore       via SQL          Data
  [private S3]      [Redshift]    [in Redshift]    [public S3]       [RDS]
```

## Source Data

private-bits-cybergreen-net/dev/clean/...

Example - from OpenNTP data - note header is added (missing in original data)

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
"REDSHIFT_PASSWORD": <<passwordForRedshift>>,
"RDS_PASSWORD": <<passwordForRDS>>, 
"AWS_ACCESS_KEY": <<AwsAccessKeYGoesHERE>>,
"AWS_ACCESS_SECRET_KEY": <<AwsAccessSEcretKeYGoesHerE>>,
"REDSHIFT_USER": "cybergreen",
"REDSHIFT_DBNAME": "dev",
"REDSHIFT_HOST": "cg-analytics.cqxchced59ta.eu-west-1.redshift.amazonaws.com",
"REDSHIFT_PORT": 5439
}

## Load, aggregate to Redshift and unload the Data to s3

```

Load
```
cd load
# probably want to setup a virtual env first ...
pip install -r requirements.txt
python main.py
```

If you want, you can access Redshift directly via psql to take a look at the data:

```
psql -h cg-analytics.cqxchced59ta.eu-west-1.redshift.amazonaws.com --port 5439 --user cybergreen -d dev
```
## Load data to RDS

```
cd load
$ python fromS3toRDS.py
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
