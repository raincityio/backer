#!/usr/bin/env python3

import os
import shutil
import json
import tempfile
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

class FsIndex:

    def __init__(self, meta):
        self.meta = meta

    def to_map(self):
        return {
            'meta': self.meta.to_map()
        }

    @staticmethod
    def from_map(indexmap):
        meta = Meta.from_map(indexmap['meta'])
        return FsIndex(meta)

def mkdirs(path):
    try:
        os.makedirs(path)
    except FileExistsError:
        pass

class FsRemote:

    def __init__(self, root):
        if not root.startswith('/'):
            raise Exception("root required to be absolute")
        self.root = root
        self.meta = FsMeta(self.root)

    def _get_fs_path(self):
        path = "%s/%s/fs" % (self.root, VERSION)
        mkdirs(path)
        return path

    def _get_fsguid_path(self, fsguid):
        path = "%s/%s.fs" % (self._get_fs_path(), fsguid)
        mkdirs(path)
        return path

    def _get_data_path(self, id_, fsguid):
        path = "%s/%s.backup/data" % (self._get_fsguid_path(fsguid), id_)
        mkdirs(path)
        return path

    def _get_data_nodepath(self, metakey):
        id_ = metakey.id_
        fsguid = metakey.fsguid
        n = metakey.n
        return "%s/%s.data" % (self._get_data_path(id_, fsguid), n)

    def _get_index_path(self, fsguid, id_):
        path = "%s/%s.backup/index" % (self._get_fsguid_path(fsguid), id_)
        mkdirs(path)
        return path

    def _get_index_nodepath(self, fsguid, id_, prefix):
        return "%s/%s.index" % (self._get_index_path(fsguid, id_), prefix)

    def put_data(self, metakey, stream):
        logging.debug("fs put %s" % metakey)
        with open(self._get_data_nodepath(metakey), 'wb') as out:
            shutil.copyfileobj(stream, out)

    def get_data(self, metakey, stream):
        logging.debug("fs get %s" % metakey)
        with open(self._get_data_nodepath(metakey), 'rb') as in_:
            shutil.copyfileobj(in_, stream)

    def list(self):
        metas = []
        for fsnode in os.listdir(self._get_fs_path()):
            if not fsnode.endswith('.fs'):
                continue
            fsguid = fsnode[:-3]
            for idnode in os.listdir(self._get_fsguid_path(fsguid)):
                if not idnode.endswith('.backup'):
                    continue
                id_ = idnode[:-7]
                meta = self.get_current_meta(fsguid, id_)
                metas.append(meta)
        return metas

    # TODO, this should be a noop
    def index(self, backsnap):
        logging.debug("fs index %s" % backsnap.meta.key)
        index = FsIndex(backsnap.meta)
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
        for name, path in named_indexes.items():
            if (name in indexes) and (indexes[name] == path):
                continue
            logging.debug("fs index put %s" % path)
            with open(path, 'wb') as out:
                out.write(indexblob)
            indexes[name] = path
        backsnap.set_indexes(indexes)

    # TODO, this should grab the snapshot based on creation date,
    # possibly use an xattr to store the FsIndex structure
    def get_current_meta(self, fsguid, id_):
        with open(self._get_index_nodepath(fsguid, id_, 'current')) as in_:
            indexblob = in_.read()
        return FsIndex.from_map(json.loads(indexblob)).meta
