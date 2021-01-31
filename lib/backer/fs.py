#!/usr/bin/env python3

import os
import shutil
import logging
import lzma
import datetime

from .common import VERSION, Meta

class FsRemote:

    type_ = 'fs'

    def __init__(self, root):
        if not root.startswith('/'):
            raise Exception("root required to be absolute")
        self.root = root
        self.cfg = {
            'root': root
        }

    def _get_path(self, *, make=False):
        path = "%s/%s" % (self.root, VERSION)
        if make:
            os.makedirs(path, exist_ok=True)
        return path

    def _get_fs_path(self, fsguid, *, make=False):
        path = "%s/%s.fs" % (self._get_path(make=make), fsguid)
        if make:
            os.makedirs(path, exist_ok=True)
        return path

    def _get_data_path(self, fsguid, id_, sid, *, make=False):
        path = "%s/%s.backup/data/%s.series" % \
                (self._get_fs_path(fsguid, make=make), id_, sid)
        if make:
            os.makedirs(path, exist_ok=True)
        return path

    def _get_data_datapath(self, metakey, *, make=False):
        id_ = metakey.id
        fsguid = metakey.fsguid
        sid = metakey.sid
        n = metakey.n
        return "%s/%s.data.xz" % (self._get_data_path(fsguid, id_, sid, make=make), n)

    def _get_data_metapath(self, metakey, *, make=False):
        id_ = metakey.id
        fsguid = metakey.fsguid
        sid = metakey.sid
        n = metakey.n
        return "%s/%s.meta" % (self._get_data_path(fsguid, id_, sid, make=make), n)

    def _get_index_path(self, fsguid, id_, *, make=False):
        path = "%s/%s.backup/index" % \
                (self._get_fs_path(fsguid, make=make), id_)
        if make:
            os.makedirs(path, exist_ok=True)
        return path

    def _get_index_metapath(self, fsguid, id_, nodename, *, make=False):
        return "%s/%s.meta" % (self._get_index_path(fsguid, id_, make=make), nodename)

    def put_data(self, metakey, stream):
        logging.debug("fs put %s" % metakey)
        fh = os.open(self._get_data_datapath(metakey, make=True),
                os.O_CREAT | os.O_WRONLY, 0o600)
        with open(fh, 'wb') as out:
            with lzma.LZMAFile(out, 'wb') as lzout:
                shutil.copyfileobj(stream, lzout)

    def put_meta(self, meta):
        self._put_meta(meta, self._get_data_metapath(meta.key, make=True))

    def _put_meta(self, meta, path):
        metablob = meta.to_data().encode('utf8')
        with open(path, 'wb') as out:
            out.write(metablob)

    def get_data(self, metakey, stream):
        logging.debug("fs get %s" % metakey)
        with lzma.open(self._get_data_datapath(metakey), 'rb') as in_:
            shutil.copyfileobj(in_, stream)

    def get_meta(self, metakey):
        return self._get_meta(self._get_data_metapath(metakey))

    def _get_meta(self, path):
        with open(path, 'rb') as in_:
            metablob = in_.read()
        return Meta.from_data(metablob.decode('utf8'))

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
        now = datetime.datetime.utcnow()
        fsguid = backsnap.meta.key.fsguid
        id_ = backsnap.meta.key.id
        named_indexes = {
            'current': self._get_index_metapath(fsguid, id_, "current", make=True),
            'day': self._get_index_metapath(fsguid, id_, "%s-%s-%s" % (now.year, now.month, now.day), make=True),
        }
        
        state = backsnap.get_remote_state()
        if state is None:
            state = { 'indexes': {} }
        indexes = state['indexes']
        for name, path in named_indexes.items():
            if (name in indexes) and (indexes[name] == path):
                continue
            logging.debug("fs index put %s" % path)
            self._put_meta(backsnap.meta, path)
            indexes[name] = path
        backsnap.set_remote_state(state)

    # TODO, this should grab the snapshot based on creation date,
    # possibly use an xattr to store the FsIndex structure
    def get_current_meta(self, fsguid, id_):
        path = self._get_index_metapath(fsguid, id_, 'current')
        return self._get_meta(path)
