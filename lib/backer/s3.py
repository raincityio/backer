#!/usr/bin/env python3

import json
import tempfile
import logging
import boto3
from datetime import datetime

from .common import VERSION, Meta

class S3Index:

    def __init__(self, meta):
        self.meta = meta

    def to_map(self):
        return {
            'meta': self.meta.to_map()
        }

    @staticmethod
    def from_map(indexmap):
        meta = Meta.from_map(indexmap['meta'])
        return S3Index(meta)

class S3Storage:

    def __init__(self, s3, bucket, prefix):
        self.s3 = s3
        self.bucket = bucket
        self.prefix = prefix

    def _get_data_path(self, metakey):
        id_ = metakey.id_
        fsguid = metakey.fsguid
        n = metakey.n
        return "%s/%s/fs/%s/data/%s/%s.data" % (self.prefix, VERSION, fsguid, id_, n)

    def put_data(self, metakey, stream):
        logging.info("s3 put %s" % metakey)
        self.s3.upload_fileobj(stream, self.bucket, self._get_data_path(metakey))

    def get_data(self, metakey, stream):
        logging.info("s3 get %s" % metakey)
        self.s3.download_fileobj(self.bucket, self._get_data_path(metakey), stream)

    def list(self):
        token = None
        metas = []
        while True:
            if token is None:
                response = self.s3.list_objects_v2(Bucket=self.bucket,
                        Prefix="%s/%s/fs/" % (self.prefix, VERSION),
                        Delimiter="/")
            else:
                response = self.s3.list_objects_v2(Bucket=self.bucket,
                        Prefix="%s/%s/fs/" % (self.prefix, VERSION),
                        Delimiter="/", ContinuationToken=token)
            if 'CommonPrefixes' in response:
                for cp in response['CommonPrefixes']:
                    fsguid = cp['Prefix'].split('/')[-2]
                    meta = self.get_current_meta(fsguid)
                    metas.append(meta)
            if response['IsTruncated']:
                token = response['NextContinuationToken']
            else:
                break
        return metas

    def index(self, backsnap):
        logging.info("s3 index %s" % backsnap.meta.key)
        index = S3Index(backsnap.meta)
        indexblob = json.dumps(index.to_map()).encode('utf8')
        now = datetime.utcnow()
        id_ = backsnap.meta.key.id_
        fsguid = backsnap.meta.key.fsguid
        named_indexes = {
            'current': "%s/%s/fs/%s/index/current.index" %
                (self.prefix, VERSION, fsguid),
            'month':   "%s/%s/fs/%s/index/%s-%s.index" %
                (self.prefix, VERSION, fsguid, now.year, now.month),
            'day':     "%s/%s/fs/%s/index/%s-%s-%s.index" %
                (self.prefix, VERSION, fsguid, now.year, now.month, now.day),
            'hour':    "%s/%s/fs/%s/index/%s-%s-%s-%s.index" % 
                (self.prefix, VERSION, fsguid, now.year, now.month, now.day, now.hour),

            'current_id': "%s/%s/fs/%s/index/%s/current.index" %
                (self.prefix, VERSION, fsguid, id_),
            'month_id':   "%s/%s/fs/%s/index/%s/%s-%s.index" %
                (self.prefix, VERSION, fsguid, id_, now.year, now.month),
            'day_id':     "%s/%s/fs/%s/index/%s/%s-%s-%s.index" %
                (self.prefix, VERSION, fsguid, id_, now.year, now.month, now.day),
            'hour_id':    "%s/%s/fs/%s/index/%s/%s-%s-%s-%s.index" % 
                (self.prefix, VERSION, fsguid, id_, now.year, now.month, now.day, now.hour),
        }
        
        indexes = backsnap.get_indexes()
        with tempfile.NamedTemporaryFile() as out:
            out.write(indexblob)
            out.flush()
            for name, path in named_indexes.items():
                if (name in indexes) and (indexes[name] == path):
                    continue
                logging.info("s3 index put %s" % path)
                out.seek(0)
                self.s3.upload_file(out.name, self.bucket, path)
                indexes[name] = path
        backsnap.set_indexes(indexes)

    def get_current_meta(self, fsguid, *, id_=None):
        path = "%s/%s/fs/%s/index" % (self.prefix, VERSION, fsguid)
        if id_ is not None:
            path = "%s/%s" % (path, id_)
        path = "%s/current.index" % path
        with tempfile.TemporaryFile() as out:
            self.s3.download_fileobj(self.bucket, path, out)
            out.flush()
            out.seek(0)
            indexblob = out.read()
        index = S3Index.from_map(json.loads(indexblob))
        return index.meta
