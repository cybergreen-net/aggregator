import json
import os
import boto
from boto.s3.key import Key

env = json.load(open('../.env.json'))

AWS_ACCESS_KEY = env['AWS_ACCESS_KEY']
AWS_ACCESS_SECRET_KEY = env['AWS_ACCESS_SECRET_KEY']

s3bucket = 'private-bits-cybergreen-net'
s3path = '/dev/agregated/latest/stats.csv'
fpath = 'aggregated.csv'
fo = open(fpath, 'r+')

def upload_to_s3(aws_access_key_id, aws_secret_access_key, file, bucket, key, callback=None, md5=None, reduced_redundancy=False, content_type=None):
    """
    Uploads the given file to the AWS S3
    bucket and key specified.
	"""
    try:
        size = os.fstat(file.fileno()).st_size
    except:
        # Not all file objects implement fileno(),
        # so we fall back on this
        file.seek(0, os.SEEK_END)
        size = file.tell()

    conn = boto.connect_s3(aws_access_key_id, aws_secret_access_key)
    bucket = conn.get_bucket(bucket, validate=True)
    k = Key(bucket)
    k.key = key
    if content_type:
        k.set_metadata('Content-Type', content_type)
    sent = k.set_contents_from_file(file, cb=callback, md5=md5, reduced_redundancy=reduced_redundancy, rewind=True)

    # Rewind for later use
    file.seek(0)

    if sent == size:
        return True
    return False

if __name__ == '__main__':
	success = upload_to_s3(AWS_ACCESS_KEY, AWS_ACCESS_SECRET_KEY, fo, s3bucket, s3path)
	if success:
		print 'File uploaded sucessfully!'
	else:
		print 'The upload failed...'

