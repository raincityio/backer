#!/usr/bin/env python3

import uuid
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

    def __init__(self, meta, stored, indexes, remote_meta):
        self.meta = meta
        self.stored = stored
        self.indexes = indexes
        self.remote_meta = remote_meta

    def to_map(self):
        return {
            'meta': self.meta.to_map(),
            'stored': self.stored,
            'indexes': self.indexes,
            'remote': self.remote_meta.to_map()
        }

    @staticmethod
    def from_map(statemap):
        meta = Meta.from_map(statemap['meta'])
        remote_type = statemap['remote']['type']
        if remote_type == 'local':
            from .fs import FsMeta
            RemoteMeta = FsMeta
        elif remote_type == 's3':
            from .s3 import S3Meta
            RemoteMeta = S3Meta
        else:
            raise Exception("unknown remote: %s" % remote)
        remote_meta = RemoteMeta.from_map(statemap['remote'])
        return State(meta, statemap['stored'], statemap['indexes'], remote_meta)

    @staticmethod
    def create(fs, metakey, remote):
        return State(Meta.create(fs, metakey), False, {}, remote.meta)
        
class Backsnap:

    def __init__(self, snapshot):
        self.snapshot = snapshot
        statedata = str(snapshot.get(STATE_PROP))
        self._state = State.from_map(json.loads(statedata))
        self.meta = self._state.meta

    def get_remote_meta(self):
        return self._state.remote_meta

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
    def create(fs, remote, metakey):
        state = State.create(fs, metakey, remote)
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
        backsnaps.append(Backsnap.create(fs, remote, metakey))
    else:
        latest = backsnaps[-1]
        if force or (not latest.snapshot.check_is_current()):
            metakey = Meta.Key(str(fs.get('guid')), id_, latest.meta.key.n+1)
            if remote.meta != latest.get_remote_meta():
                raise Exception("incompatible remote: %s" % remote.meta)
            backsnaps.append(Backsnap.create(fs, remote, metakey))

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

_none = uuid.uuid4()
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

    def list_backups(self):
        return self.get('backups', default={}).keys()

    def get(self, key, *, default=_none):
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
        if default is not _none:
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
    parser.add_argument('-c', default='/usr/local/etc/backer.json', metavar='config')

    subparsers = parser.add_subparsers(dest='action')
    
    backup_parser = subparsers.add_parser('backup')
    backup_parser.add_argument('-n', metavar='[backup name]', required=True)
    backup_parser.add_argument('--force', action='store_true')

    index_parser = subparsers.add_parser('index')
    index_parser.add_argument('-n', metavar='[backup name]', required=True)

    backup_all_parser = subparsers.add_parser('backup-all')
    backup_all_parser.add_argument('--force', action='store_true')

    subparsers.add_parser('index-all')

    list_parser = subparsers.add_parser('list')
    list_parser.add_argument('-r', metavar='remote')

    restore_parser = subparsers.add_parser('restore')
    restore_parser.add_argument('-l', metavar='local')
    restore_parser.add_argument('-r', metavar='remote')
    restore_parser.add_argument('-i', default='default', metavar='id')
    restore_parser.add_argument('-g', metavar='fsguid', required=True)
    restore_parser.add_argument('-f', metavar='fsname', required=True)

    subparsers.add_parser('daemon') 

    args = parser.parse_args()

    cfg = Config(filename=args.c)
    if ('version' in cfg) and (VERSION != cfg['version']):
        raise Exception("version mismatch: %s != %s" % (VERSION, cfg['version']))

    remotes = {}
    def get_remote(remote_name):
        if remote_name is None:
            remote_name = cfg["default_remote"]
        if remote_name in remotes:
            return remotes[remote_name]
        type_ = cfg["remotes.%s.type" % remote_name]
        if type_ == 'fs':
            from .fs import FsRemote
            root = cfg["remotes.%s.fs:root" % remote_name]
            remote = FsRemote(root)
        elif type_ == 's3':
            import boto3
            from .s3 import S3Remote
            aws_creds = cfg.get("remotes.%s.aws:creds" % remote_name, default=None)
            if aws_creds is not None:
                os.environ['AWS_SHARED_CREDENTIALS_FILE'] = aws_creds
            aws_profile = cfg.get("remotes.%s.aws:profile" % remote_name, default=None)
            if aws_profile is not None:
                os.environ['AWS_PROFILE'] = aws_profile
            aws_region = cfg.get("remotes.%s.aws:region" % remote_name, default=None)
            if aws_region is not None:
                os.environ['AWS_REGION'] = aws_region

# TODO, feed creds/profile/region directly into session, or make them global
            session = boto3.Session()
            s3 = session.client("s3")
            bucket = cfg["remotes.%s.s3:bucket" % remote_name]
            prefix = cfg["remotes.%s.s3:prefix" % remote_name]
            remote = S3Remote(s3, bucket, prefix)
        else:
            raise Exception("unknown remote: %s" % remote_name)
        remotes[remote_name] = remote 
        return remote

    locals_ = {}
    def get_local(local_name):
        if local_name is None:
            local_name = cfg["default_local"]
        if local_name in locals_:
            return locals_[local_name]
        type_ = cfg["locals.%s.type" % local_name]
        if type_ == 'zfs':
            from . import zfs
            local = zfs
        else:
            raise Exception("unknown local: %s" % local_name)
        locals_[local_name] = local
        return local

    backups = {}
    def get_backup(backup_name):
        if backup_name in backups:
            return backups[backup_name]
        local_name = cfg.get("backups.%s.local" % backup_name, default=None)
        local = get_local(local_name)
        remote_name = cfg.get("backups.%s.remote" % backup_name, default=None)
        remote = get_remote(remote_name)
        fsname = cfg["backups.%s.fs:name" % backup_name]
        id_ = cfg.get("backups.%s.id" % backup_name, default='default')
        fs = local.get_filesystem(fsname)
        backup = (fs, remote, id_,)
        backups[backup_name] = backup
        return backup

    action = args.action
    if action == 'backup':
        backup_name = args.n
        fs, remote, id_ = get_backup(backup_name)
        backup(fs, remote, id_, force=args.force) 
    elif action == 'index':
        backup_name = args.n
        fs, remote, id_ = get_backup(backup_name)
        index(fs, remote, id_)
    elif action == 'backup-all':
        for backup_name in cfg.list_backups():
            fs, remote, id_ = get_backup(backup_name)
            backup(fs, remote, id_, force=args.force)
    elif action == 'index-all':
        for backup_name in cfg.list_backups():
            fs, remote, id_ = get_backup(backup_name)
            index(fs, remote, id_)
    elif action == 'restore':
        fsguid = args.g
        restore_fsname = args.f
        local = get_local(args.l)
        remote = get_remote(args.r)
        meta_discovery = remote.get_current_meta
        id_ = args.i
        restore(local, remote, meta_discovery, fsguid, id_, restore_fsname)
    elif action == 'list':
        remote = get_remote(args.r)
        for meta in remote.list():
            print(meta)
    elif action == 'daemon':
        period = cfg.get('daemon:period', default=60)
        finished = threading.Event()

        def indexer_daemon():
            while not finished.is_set():
                for backup_name in cfg.list_backups():
                    fs, remote, id_ = get_backup(backup_name)
                    try: 
                        index(fs, remote, id_)
                    except Exception as e:
                        logging.exception(e)
                if finished.wait(timeout=period):
                    break

        def backer_daemon():
            while not finished.is_set():
                for backup_name in cfg.list_backups():
                    fs, remote, id_ = get_backup(backup_name)
                    try:
                        backup(fs, remote, id_)
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
