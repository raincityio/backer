#!/usr/bin/env python3

import os
import shutil
import json
import logging
from datetime import datetime

from .common import VERSION, Meta

class FsMeta:

    def __init__(self, root):
        self.root = root

    def __eq__(self, other):
        if not isinstance(other, FsMeta):
            return False
        return other.root == self.root

    def to_map(self):
        return {
            'type': 'fs',
            'root': self.root
        }

    @staticmethod
    def from_map(metamap):
        return FsMeta(metamap['root'])

class FsRemote:

    def __init__(self, root):
        if not root.startswith('/'):
            raise Exception("root required to be absolute")
        self.root = root
        self.meta = FsMeta(self.root)

    def _get_path(self):
        path = "%s/%s" % (self.root, VERSION)
        os.makedirs(path, exist_ok=True)
        return path

    def _get_fs_path(self, fsguid):
        path = "%s/%s.fs" % (self._get_path(), fsguid)
        os.makedirs(path, exist_ok=True)
        return path

    def _get_data_path(self, fsguid, id_, sid):
        path = "%s/%s.backup/data/%s.series" % (self._get_fs_path(fsguid), id_, sid)
        os.makedirs(path, exist_ok=True)
        return path

    def _get_data_nodepath(self, metakey):
        id_ = metakey.id_
        fsguid = metakey.fsguid
        sid = metakey.sid
        n = metakey.n
        return "%s/%s.data" % (self._get_data_path(fsguid, id_, sid), n)

    def _get_index_path(self, fsguid, id_):
        path = "%s/%s.backup/index" % (self._get_fs_path(fsguid), id_)
        os.makedirs(path, exist_ok=True)
        return path

    def _get_index_nodepath(self, fsguid, id_, nodename):
        return "%s/%s.meta" % (self._get_index_path(fsguid, id_), nodename)

    def put_data(self, metakey, stream):
        logging.debug("fs put %s" % metakey)
        fh = os.open(self._get_data_nodepath(metakey),
                os.O_CREAT | os.O_WRONLY, 0o600)
        with open(fh, 'wb') as out:
            shutil.copyfileobj(stream, out)

    def get_data(self, metakey, stream):
        logging.debug("fs get %s" % metakey)
        with open(self._get_data_nodepath(metakey), 'rb') as in_:
            shutil.copyfileobj(in_, stream)

    def list(self):
        metas = []
        for fsnode in os.listdir(self._get_path()):
            fsguid, ext = os.path.splitext(fsnode)
            if ext != '.fs':
                continue
            for idnode in os.listdir(self._get_fs_path(fsguid)):
                id_, ext = os.path.splitext(idnode)
                if ext != '.backup':
                    continue
                meta = self.get_current_meta(fsguid, id_)
                metas.append(meta)
        return metas

    # TODO, this should be a noop
    def index(self, backsnap):
        logging.debug("fs index %s" % backsnap.meta.key)
        metablob = json.dumps(backsnap.meta.to_map()).encode('utf8')
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
        for name, path in named_indexes.items():
            if (name in indexes) and (indexes[name] == path):
                continue
            logging.debug("fs index put %s" % path)
            with open(path, 'wb') as out:
                out.write(metablob)
            indexes[name] = path
        backsnap.set_indexes(indexes)

    # TODO, this should grab the snapshot based on creation date,
    # possibly use an xattr to store the FsIndex structure
    def get_current_meta(self, fsguid, id_):
        with open(self._get_index_nodepath(fsguid, id_, 'current'), 'rb') as in_:
            metablob = in_.read()
        return Meta.from_map(json.loads(metablob.decode('utf8')))
