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
        make and os.makedirs(path, exist_ok=True)
        return path

    def _get_fs_path(self, fsid, *, make=False):
        path = "%s/fs/%s.fs" % (self._get_path(make=make), fsid)
        make and os.makedirs(path, exist_ok=True)
        return path

    def _get_backup_path(self, fsid, bid, *, make=False):
        path = "%s/backup/%s.backup" % (self._get_fs_path(fsid, make=make), bid)
        make and os.makedirs(path, exist_ok=True)
        return path

    def _get_series_path(self, fsid, bid, sid, *, make=False):
        path = "%s/series/%s.series" % (self._get_backup_path(fsid, bid, make=make), sid)
        make and os.makedirs(path, exist_ok=True)
        return path

    def _get_data_path(self, fsid, bid, sid, *, make=False):
        path = "%s/data" % (self._get_series_path(fsid, bid, sid, make=make))
        make and os.makedirs(path, exist_ok=True)
        return path

    def _get_index_path(self, fsid, bid, *, make=False):
        path = "%s/index" % (self._get_backup_path(fsid, bid, make=make))
        make and os.makedirs(path, exist_ok=True)
        return path

    def _get_data_datapath(self, metakey, *, make=False):
        dpath = self._get_data_path(metakey.fsid, metakey.bid, metakey.sid, make=make)
        return "%s/%s.data.xz" % (dpath, metakey.n)

    def _get_data_metapath(self, metakey, *, make=False):
        dpath = self._get_data_path(metakey.fsid, metakey.bid, metakey.sid, make=make)
        return "%s/%s.meta" % (dpath, metakey.n)

    def _get_index_metapath(self, fsid, bid, nodename, *, make=False):
        return "%s/%s.meta" % (self._get_index_path(fsid, bid, make=make), nodename)

    def _get_currentpath(self, fsid, *, bid=None, sid=None, make=False):
        if bid is None:
            return "%s/current.meta" % (self._get_fs_path(fsid, make=make))
        if sid is None:
            return "%s/current.meta" % (self._get_backup_path(fsid, bid, make=make))
        return "%s/current.meta" % (self._get_series_path(fsid, bid, sid, make=make))

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

    def list(self, *, fsid=None, bid=None):
        metas = []
        if fsid is None:
            for fsnode in os.listdir("%s/fs" % self._get_path()):
                fsid, ext = os.path.splitext(fsnode)
                if ext != '.fs':
                    continue
                metas.append(self.get_current_meta(fsid))
        elif bid is None:
            for backupnode in os.listdir("%s/backup" % (self._get_fs_path(fsid))):
                bid, ext = os.path.splitext(backupnode)
                if ext != '.backup':
                    continue
                metas.append(self.get_current_meta(fsid, bid=bid))
        else:
            for seriesnode in os.listdir("%s/series" % (self._get_backup_path(fsid, bid))):
                sid, ext = os.path.splitext(seriesnode)
                if ext != '.series':
                    continue
                metas.append(self.get_current_meta(fsid, bid=bid, sid=sid))
        return metas

    # TODO, this should be a noop
    def index(self, backsnap):
        logging.debug("fs index %s" % backsnap.meta.key)
        fsid = backsnap.meta.key.fsid
        bid = backsnap.meta.key.bid
        sid = backsnap.meta.key.sid

        now = datetime.datetime.utcnow()
        named_indexes = {
            'current': self._get_currentpath(fsid),
            'bid_current': self._get_currentpath(fsid, bid=bid),
            'bid_sid_current': self._get_currentpath(fsid, bid=bid, sid=sid),
            'bid_day': self._get_index_metapath(fsid, bid, "%s-%s-%s" % (now.year, now.month, now.day), make=True),
        }
        
        state = backsnap.get_remote_state()
        if state is None:
            state = {}
        if 'indexes' not in state:
            state['indexes'] = {}
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
    def get_current_meta(self, fsid, *, bid=None, sid=None, n=None):
        if n is None:
            return self._get_meta(self._get_currentpath(fsid, bid=bid, sid=sid))
        metakey = Meta.Key(fsid, bid, sid, n)
        return self.get_meta(metakey)
