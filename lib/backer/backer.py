#!/usr/bin/env python3

import os
import argparse
import json
import tempfile
import time
import logging
import threading
import signal

from .common import VERSION, Meta
from . import zfs

VERSION_PROP = "backer:version"
STATE_PROP = "backer:state"

class State:

    def __init__(self, meta, stored, indexes):
        self.meta = meta
        self.stored = stored
        self.indexes = indexes

    def to_map(self):
        return {
            'meta': self.meta.to_map(),
            'stored': self.stored,
            'indexes': self.indexes
        }

    @staticmethod
    def from_map(statemap):
        meta = Meta.from_map(statemap['meta'])
        return State(meta, statemap['stored'], statemap['indexes'])

    @staticmethod
    def create(fs, metakey):
        return State(Meta.create(fs, metakey), False, {})
        
class Backsnap:

    def __init__(self, snapshot):
        self.snapshot = snapshot
        statedata = snapshot.get(STATE_PROP)
        self._state = State.from_map(json.loads(statedata))
        self.meta = self._state.meta

    def get_indexes(self):
        return self._state.indexes

    def set_indexes(self, indexes):
        self._state.indexes = indexes
        self._apply_state()

    def is_stored(self):
        return self._state.stored

    def set_stored(self, stored):
        self._state.stored = stored
        self._apply_state()

    def _apply_state(self):
        statedata = json.dumps(self._state.to_map())
        self.snapshot.set(STATE_PROP, statedata)

    @staticmethod
    def name(metakey):
        return "backer:%s-%s-%s" % (VERSION, metakey.id_, metakey.n)

    @staticmethod
    def create(fs, metakey):
        state = State.create(fs, metakey)
        snapshot = fs.snapshot(Backsnap.name(metakey), props={
                VERSION_PROP: VERSION,
                STATE_PROP: json.dumps(state.to_map())})
        return Backsnap(snapshot)

# return list of backsnaps sort in ascending order
def get_backsnaps(fs, id_):
    backsnaps = []
    for name, props in fs.list_snapshots(keys=[VERSION_PROP]).items():
        if VERSION_PROP not in props:
            continue
        if props[VERSION_PROP] != VERSION:
            continue
        snapshot = fs.get_snapshot(name)
        backsnap = Backsnap(snapshot)
        if backsnap.meta.key.id_ != id_:
            continue
        backsnaps.append(backsnap)
    return sorted(backsnaps, key=lambda x: x.meta.key.n)

def get_latest_stored(fs, id_):
    backsnaps = list(filter(lambda x: x.is_stored(), get_backsnaps(fs, id_)))
    if len(backsnaps) == 0:
        return None
    return backsnaps[-1]

def index(storage, fsname, id_):
    fs = zfs.get_filesystem(fsname)
    latest = get_latest_stored(fs, id_)
    if latest is not None:
        storage.index(latest)

def backup(storage, fsname, id_, *, force=False):
    fs = zfs.get_filesystem(fsname)

    backsnaps = get_backsnaps(fs, id_)
    if len(backsnaps) == 0:
        metakey = Meta.Key(fs.get('guid'), id_, 0)
        backsnaps.append(Backsnap.create(fs, metakey))
    else:
        latest = backsnaps[-1]
        if force or (not latest.snapshot.check_is_current()):
            metakey = Meta.Key(fs.get('guid'), id_, latest.meta.key.n+1)
            backsnaps.append(Backsnap.create(fs, metakey))

    previous = None
    for backsnap in backsnaps:
        if not backsnap.is_stored():
            with tempfile.TemporaryFile() as out:
                if previous is None:
                    backsnap.snapshot.send(out)
                else:
                    backsnap.snapshot.send(out, other=previous.snapshot)
                out.flush()
                out.seek(0)
                storage.put_data(backsnap.meta.key, out)
            storage.index(backsnap)
            backsnap.set_stored(True)
        if previous is not None:
            previous.snapshot.destroy()
        previous = backsnap
            
def restore(storage, meta_discovery, fsguid, id_, restore_fsname):
    latest_meta = meta_discovery(fsguid, id_)
    if latest_meta is None:
        raise Exception("latest not found")
    fsguid = latest_meta.key.fsguid
    for n in range(latest_meta.key.n+1):
        metakey = Meta.Key(fsguid, id_, n)
        logging.debug("restore recv %s" % metakey)
        with tempfile.TemporaryFile() as out:
            storage.get_data(metakey, out)
            out.flush()
            out.seek(0)
            zfs.recv(restore_fsname, out)
    fs = zfs.get_filesystem(restore_fsname)
    for name in fs.list_snapshots():
        snapshot = fs.get_snapshot(name)
        snapshot.destroy()

class Config:

    def __init__(self, *, cfg=None, filename=None):
        if not cfg is None:
            self.cfg = cfg
        elif not filename is None:
            if os.path.lexists(filename):
                with open(filename, 'r') as in_:
                    self.cfg = json.load(in_)
            else:
                self.cfg = {}
        else:
            self.cfg = {}

    def get(self, key, *, default=None):
        parts = key.split(".")
        current = self.cfg
        for part in parts[:-1]:
            if part not in current:
                raise KeyError(key)
            elif type(current[part]) is not dict:
                raise KeyError(key)
            current = current[part]
        part = parts[-1]                
        if part in current:
            return current[part]
        if default is not None:
            return default
        raise KeyError(key)

    def __getitem__(self, key):
        return self.get(key)

    def __contains__(self, key):
        try:
            self.get(key)
        except KeyError:
            return False
        return True

def main():
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument('-c', default='/usr/local/etc/backer.json', help='config')
    parser.add_argument('--backup', help='backup')
    parser.add_argument('--index', help='index')
    parser.add_argument('-i', default='default', help='backup id')
    parser.add_argument('--force', action='store_true', help='force')
    parser.add_argument('--backup-all', action='store_true', help='backup all')
    parser.add_argument('--index-all', action='store_true', help='index all')
    parser.add_argument('--restore', nargs=2, help='restore')
    parser.add_argument('--list', action='store_true', help='list')
    parser.add_argument('--daemon', action='store_true', help='daemon')
    args = parser.parse_args()

    cfg = Config(filename=args.c)

    if True:
        import boto3
        from .s3 import S3Storage
        if 'aws:creds' in cfg:
            os.environ['AWS_SHARED_CREDENTIALS_FILE'] = cfg['aws:creds']
        if 'aws:profile' in cfg:
            os.environ['AWS_PROFILE'] = cfg['aws:profile']
        if 'aws:region' in cfg:
            os.environ['AWS_REGION'] = cfg['aws:region']

        session = boto3.Session()
        s3 = session.client("s3")
        bucket = cfg.get('s3:bucket')
        prefix = cfg.get('s3:prefix')
        storage = S3Storage(s3, bucket, prefix)

    meta_discovery = storage.get_current_meta

    if args.backup:
        fsname = args.backup
        backup(storage, fsname, args.i, force=args.force) 
    elif args.index:
        fsname = args.index
        index(storage, fsname, args.i)
    elif args.backup_all:
        for fsname in cfg.get('filesystems', default={}).keys():
            id_ = cfg.get("filesystems.%s.id" % fsname, default='default')
            backup(storage, fsname, id_, force=args.force)
    elif args.index_all:
        for fsname in cfg.get('filesystems', default={}).keys():
            id_ = cfg.get("filesystems.%s.id" % fsname, default='default')
            index(storage, fsname, id_)
    elif args.restore:
        fsguid = args.restore[0]
        restore_fsname = args.restore[1]
        restore(storage, meta_discovery, fsguid, args.i, restore_fsname)
    elif args.list:
        for meta in storage.list():
            print(meta)
    elif args.daemon:
        period = cfg.get('daemon_period', default=60)
        finished = threading.Event()

        def indexer_daemon():
            while not finished.is_set():
                for fsname in cfg.get('filesystems', default={}).keys():
                    try:
                        id_ = cfg.get("filesystems.%s.id" % fsname, default='default')
                        index(storage, fsname, id_)
                    except Exception as e:
                        logging.exception(e)
                if finished.wait(timeout=period):
                    break

        def backer_daemon():
            while not finished.is_set():
                for fsname in cfg.get('filesystems', default={}).keys():
                    try:
                        id_ = cfg.get("filesystems.%s.id" % fsname, default='default')
                        backup(storage, fsname, id_)
                    except Exception as e:
                        logging.exception(e)
                if finished.wait(timeout=period):
                    break

        def signal_handler(*args):
            finished.set()

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        threading.Thread(target=indexer_daemon).start()
        threading.Thread(target=backer_daemon).start()

        finished.wait()
    else:
        raise Exception("action not specified")
