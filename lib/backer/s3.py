#!/usr/bin/env python3

import shutil
import lzma
import os
import io
import logging
import boto3
from datetime import datetime
import tempfile

from .common import VERSION, Meta

class S3Remote:

    type_ = 's3'

    def __init__(self, s3, bucket, prefix):
        self.s3 = s3
        self.bucket = bucket
        self.prefix = prefix
        self.cfg = {
            'bucket': bucket,
            'prefix': prefix
        }

    def _get_path(self):
        return "%s/%s" % (self.prefix, VERSION)

    def _get_fs_path(self, fsguid):
        return "%s/%s.fs" % (self._get_path(), fsguid)

    def _get_data_path(self, fsguid, id_, sid):
        return "%s/%s.backup/data/%s.series" % (self._get_fs_path(fsguid), id_, sid)

    def _get_data_datapath(self, metakey):
        fsguid = metakey.fsguid
        id_ = metakey.id_
        sid = metakey.sid
        n = metakey.n
        return "%s/%s.data.xz" % (self._get_data_path(fsguid, id_, sid), n)

    def _get_data_metapath(self, metakey):
        fsguid = metakey.fsguid
        id_ = metakey.id_
        sid = metakey.sid
        n = metakey.n
        return "%s/%s.meta" % (self._get_data_path(fsguid, id_, sid), n)

    def _get_index_path(self, fsguid, id_):
        return "%s/%s.backup/index" % (self._get_fs_path(fsguid), id_)

    def _get_index_metapath(self, fsguid, id_, name):
        return "%s/%s.meta" % (self._get_index_path(fsguid, id_), name)

    def put_data(self, metakey, stream):
        logging.debug("s3 put data %s" % metakey)
        datapath = self._get_data_datapath(metakey)
        with tempfile.SpooledTemporaryFile(max_size=1_000_000) as lzfile:
            with lzma.LZMAFile(lzfile, 'wb') as out:
                shutil.copyfileobj(stream, out)
            lzfile.flush()
            lzfile.seek(0)
            self.s3.upload_fileobj(lzfile, self.bucket, datapath)

    def put_meta(self, meta):
        logging.debug("s3 put meta %s" % meta.key)
        self._put_meta(meta, self._get_data_metapath(meta.key))

    def _put_meta(self, meta, path):
        metablob = meta.to_data().encode('utf8')
        metablob_f = io.BytesIO(metablob)
        self.s3.upload_fileobj(metablob_f, self.bucket, path)

    def get_data(self, metakey, stream):
        path = self._get_data_datapath(metakey)
        logging.debug("s3 get %s from %s" % (metakey, path))
        with tempfile.SpooledTemporaryFile(max_size=1_000_000) as lzfile:
            self.s3.download_fileobj(self.bucket, path, lzfile)
            lzfile.flush()
            lzfile.seek(0)
            with lzma.LZMAFile(lzfile, 'rb') as in_:
                shutil.copyfileobj(in_, stream)

    def get_meta(self, metakey):
        logging.debug("s3 get meta %s" % metakey)
        return self._get_meta(self._get_data_metapath(metakey))

    def _get_meta(self, path):
        metablob_f = io.BytesIO()
        self.s3.download_fileobj(self.bucket, path, metablob_f)
        metablob_f.seek(0)
        metablob = metablob_f.read()
        return Meta.from_data(metablob.decode('utf8'))

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
        for fsnode in self._ls(self._get_path()):
            fsguid, ext = os.path.splitext(fsnode)
            if ext != '.fs':
                continue
            for idnode in self._ls(self._get_fs_path(fsguid)):
                id_, ext = os.path.splitext(idnode)
                if ext != '.backup':
                    continue
                meta = self.get_current_meta(fsguid, id_)
                metas.append(meta)
        return metas

    def index(self, backsnap):
        logging.debug("s3 index %s" % backsnap.meta.key)
        now = datetime.utcnow()
        fsguid = backsnap.meta.key.fsguid
        id_ = backsnap.meta.key.id_
        named_indexes = {
            'current': self._get_index_metapath(fsguid, id_, "current"),
            'year': self._get_index_metapath(fsguid, id_, "%s" % now.year),
            'month': self._get_index_metapath(fsguid, id_, "%s-%s" % (now.year, now.month)),
            'day': self._get_index_metapath(fsguid, id_, "%s-%s-%s" % (now.year, now.month, now.day)),
        }
        
        state = backsnap.get_remote_state()
        if state is None:
            state = { 'indexes': {} }
        indexes = state['indexes']
        for name, path in named_indexes.items():
            if (name in indexes) and (indexes[name] == path):
                continue
            logging.debug("s3 put index %s to %s" % (backsnap.meta.key, path))
            self._put_meta(backsnap.meta, path)
            indexes[name] = path
        backsnap.set_remote_state(state)

    def get_current_meta(self, fsguid, id_):
        path = self._get_index_metapath(fsguid, id_, "current")
        return self._get_meta(path)
