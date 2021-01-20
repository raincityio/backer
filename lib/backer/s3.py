#!/usr/bin/env python3

import json
import tempfile
import logging
import boto3
from datetime import datetime

from .common import VERSION, Meta

class S3Meta:

    def __init__(self, bucket, prefix):
        self.bucket = bucket
        self.prefix = prefix

    def __eq__(self, other):
        if not isinstance(other, S3Meta):
            return False
        if other.bucket != self.bucket:
            return False
        if other.prefix != self.prefix:
            return False
        return True

    def to_map(self):
        return {
            'type': 's3',
            'bucket': self.bucket,
            'prefix': self.prefix
        }

    @staticmethod
    def from_map(metamap):
        return S3Meta(metamap['bucket'], metamap['prefix'])

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

class S3Remote:

    def __init__(self, s3, bucket, prefix):
        self.s3 = s3
        self.bucket = bucket
        self.prefix = prefix
        self.meta = S3Meta(bucket, prefix)

    def _get_path(self):
        return "%s/%s" % (self.prefix, VERSION)

    def _get_fs_path(self):
        return "%s/fs" % self._get_path()

    def _get_fsguid_path(self, fsguid):
        return "%s/%s.fs" % (self._get_fs_path(), fsguid)

    def _get_type_path(self, fsguid, id_, type_):
        return "%s/%s.backup/%s" % (self._get_fsguid_path(fsguid), id_, type_)

    def _get_type_nodepath(self, fsguid, id_, fn, type_):
        return "%s/%s.%s" % (self._get_type_path(fsguid, id_, type_), fn, type_)

    def _get_data_nodepath(self, metakey):
        return self._get_type_nodepath(metakey.fsguid, metakey.id_, metakey.n, 'data')

    def _get_index_nodepath(self, fsguid, id_, index_name):
        return self._get_type_nodepath(fsguid, id_, index_name, 'index')

    def put_data(self, metakey, stream):
        logging.debug("s3 put %s" % metakey)
        self.s3.upload_fileobj(stream, self.bucket, self._get_data_nodepath(metakey))

    def get_data(self, metakey, stream):
        logging.debug("s3 get %s" % metakey)
        self.s3.download_fileobj(self.bucket, self._get_data_nodepath(metakey), stream)

    def _ls(self, path):
        names = []
        token = None
        while True:
            if token is None:
                response = self.s3.list_objects_v2(Bucket=self.bucket,
                        Prefix="%s/" % path, Delimiter="/")
            else:
                response = self.s3.list_objects_v2(Bucket=self.bucket,
                        Prefix="%s/" % path, Delimiter="/", ContinuationToken=token)
            if 'CommonPrefixes' in response:
                for cp in response['CommonPrefixes']:
                    name = cp['Prefix'].split('/')[-2]
                    names.append(name)
            if response['IsTruncated']:
                token = response['NextContinuationToken']
            else:
                break
        return names

    def list(self):
        metas = []
        for fsguid_node in self._ls(self._get_fs_path()):
            if not fsguid_node.endswith('.fs'):
                continue
            fsguid = fsguid_node[:-3]
            for id_node in self._ls(self._get_fsguid_path(fsguid)):
                if not id_node.endswith('.backup'):
                    continue
                id_ = id_node[:-7]
                meta = self.get_current_meta(fsguid, id_)
                metas.append(meta)
        return metas

    def index(self, backsnap):
        logging.debug("s3 index %s" % backsnap.meta.key)
        index = S3Index(backsnap.meta)
        indexblob = json.dumps(index.to_map()).encode('utf8')
        now = datetime.utcnow()
        fsguid = backsnap.meta.key.fsguid
        id_ = backsnap.meta.key.id_
        named_indexes = {
            'current': self._get_index_nodepath(fsguid, id_, "current"),
            'year': self._get_index_nodepath(fsguid, id_, "%s" % now.year),
            'month': self._get_index_nodepath(fsguid, id_, "%s-%s" % (now.year, now.month)),
            'day': self._get_index_nodepath(fsguid, id_, "%s-%s-%s" % (now.year, now.month, now.day)),
            'hour': self._get_index_nodepath(fsguid, id_, "%s-%s-%s-%s" % (now.year, now.month, now.day, now.hour))
        }
        
        indexes = backsnap.get_indexes()
        with tempfile.NamedTemporaryFile() as out:
            out.write(indexblob)
            out.flush()
            for name, path in named_indexes.items():
                if (name in indexes) and (indexes[name] == path):
                    continue
                logging.debug("s3 index put %s" % path)
                out.seek(0)
                self.s3.upload_file(out.name, self.bucket, path)
                indexes[name] = path
        backsnap.set_indexes(indexes)

    def get_current_meta(self, fsguid, id_):
        path = self._get_index_nodepath(fsguid, id_, "current")
        with tempfile.TemporaryFile() as out:
            self.s3.download_fileobj(self.bucket, path, out)
            out.flush()
            out.seek(0)
            indexblob = out.read()
        index = S3Index.from_map(json.loads(indexblob))
        return index.meta
