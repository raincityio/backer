#!/usr/bin/env python3

import os
import argparse
import json
import tempfile
import logging
import threading
import signal

from .common import VERSION, Meta

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
        statedata = str(snapshot.get(STATE_PROP))
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
        self.snapshot.set(STATE_PROP, self.snapshot.Value(statedata))

    @staticmethod
    def name(metakey):
        return "backer:%s-%s-%s" % (VERSION, metakey.id_, metakey.n)

    @staticmethod
    def create(fs, metakey):
        state = State.create(fs, metakey)
        snapshot = fs.snapshot(Backsnap.name(metakey), props={
                VERSION_PROP: fs.Value(VERSION),
                STATE_PROP: fs.Value(json.dumps(state.to_map()))})
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

def index(fs, remote, id_):
    latest = get_latest_stored(fs, id_)
    if latest is not None:
        remote.index(latest)

def backup(fs, remote, id_, *, force=False):
    backsnaps = get_backsnaps(fs, id_)
    if len(backsnaps) == 0:
        metakey = Meta.Key(str(fs.get('guid')), id_, 0)
        backsnaps.append(Backsnap.create(fs, metakey))
    else:
        latest = backsnaps[-1]
        if force or (not latest.snapshot.check_is_current()):
            metakey = Meta.Key(str(fs.get('guid')), id_, latest.meta.key.n+1)
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
                remote.put_data(backsnap.meta.key, out)
            remote.index(backsnap)
            backsnap.set_stored(True)
        if previous is not None:
            previous.snapshot.destroy()
        previous = backsnap
            
def restore(local, remote, meta_discovery, fsguid, id_, restore_fsname):
    latest_meta = meta_discovery(fsguid, id_)
    if latest_meta is None:
        raise Exception("latest not found")
    fsguid = latest_meta.key.fsguid
    for n in range(latest_meta.key.n+1):
        metakey = Meta.Key(fsguid, id_, n)
        logging.debug("restore recv %s" % metakey)
        with tempfile.TemporaryFile() as out:
            remote.get_data(metakey, out)
            out.flush()
            out.seek(0)
            local.recv(restore_fsname, out)
    fs = local.get_filesystem(restore_fsname)
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

    def getfs(self, fsname, key, *, default=None):
        if fsname is None:
            value = None
        else:
            try:
                value = self.get("filesystems.%s.%s" % (fsname, key))
            except KeyError:
                value = None
        if value is None:
            return self.get(key, default=default)
        return value

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
    parser.add_argument('--id', default='default', help='backup id')
    parser.add_argument('--force', action='store_true', help='force')
    parser.add_argument('--backup-all', action='store_true', help='backup all')
    parser.add_argument('--index-all', action='store_true', help='index all')
    parser.add_argument('--restore', nargs=2, help='restore')
    parser.add_argument('--list', action='store_true', help='list')
    parser.add_argument('--daemon', action='store_true', help='daemon')
    parser.add_argument('--remote', help='remote source')
    parser.add_argument('--local', help='local source')
    args = parser.parse_args()

    cfg = Config(filename=args.c)
    if ('version' in cfg) and (VERSION != cfg['version']):
        raise Exception("version mismatch: %s != %s" % (VERSION, cfg['version']))

    def get_id(fsname):
        if args.id is None:
            id_ = cfg.getfs(fsname, 'id', default='default')
        else:
            id_ = args.id
        return id_

    remotes = {}
    def get_remote(*, fsname=None):
        if fsname in remotes:
            return remotes[fsname]
        if args.remote is None:
            remotename = cfg.getfs(fsname, 'remote', default='s3')
        else:
            remotename = args.remote
        if remotename == 'local':
            from .local import LocalStorage
            root = cfg.getfs(fsname, 'local:root')
            remote = LocalStorage(root)
        elif remotename == 's3':
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
            bucket = cfg.getfs(fsname, 's3:bucket')
            prefix = cfg.getfs(fsname, 's3:prefix')
            remote = S3Storage(s3, bucket, prefix)
        else:
            raise Exception("unknown remote: %s" % fsname)
        remotes[fsname] = remote
        return remote

    locals_ = {}
    def get_local(fsname):
        if fsname in locals_:
            return locals_[fsname]
        if args.local is None:
            localname = cfg.getfs(fsname, 'local', default='zfs')
        else:
            localname = args.local
        if localname == 'zfs':
            from . import zfs
            local = zfs
        else:
            raise Exception("unknown local: %s" % fsname)
        locals_[fsname] = local
        return local

    if args.backup:
        fsname = args.backup
        fs = get_local(fsname).get_filesystem(fsname)
        backup(fs, get_remote(fsname=fsname), get_id(fsname), force=args.force) 
    elif args.index:
        fsname = args.index
        fs = get_local(fsname).get_filesystem(fsname)
        index(fs, get_remote(fsname=fsname), get_id(fsname))
    elif args.backup_all:
        for fsname in cfg.get('filesystems', default={}).keys():
            fs = get_local(fsname).get_filesystem(fsname)
            backup(fs, get_remote(fsname=fsname), get_id(fsname), force=args.force)
    elif args.index_all:
        for fsname in cfg.get('filesystems', default={}).keys():
            fs = get_local(fsname).get_filesystem(fsname)
            index(fs, get_remote(fsname=fsname), get_id(fsname))
    elif args.restore:
        fsguid = args.restore[0]
        restore_fsname = args.restore[1]
        local = get_local(restore_fsname)
        remote = get_remote(fsname=restore_fsname)
        meta_discovery = remote.get_current_meta
        id_ = get_id(restore_fsname)
        restore(local, remote, meta_discovery, fsguid, id_, restore_fsname)
    elif args.list:
        for meta in get_remote().list():
            print(meta)
    elif args.daemon:
        period = cfg.get('daemon:period', default=60)
        finished = threading.Event()

        def indexer_daemon():
            while not finished.is_set():
                for fsname in cfg.get('filesystems', default={}).keys():
                    try: 
                        fs = get_local(fsname).get_filesystem(fsname)
                        index(fs, get_remote(fsname=fsname), get_id(fsname))
                    except Exception as e:
                        logging.exception(e)
                if finished.wait(timeout=period):
                    break

        def backer_daemon():
            while not finished.is_set():
                for fsname in cfg.get('filesystems', default={}).keys():
                    try:
                        fs = get_local(fsname).get_filesystem(fsname)
                        backup(fs, get_remote(fsname=fsname), get_id(fsname))
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
