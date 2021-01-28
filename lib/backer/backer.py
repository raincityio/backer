#!/usr/bin/env python3

import time
import fcntl
import uuid
import os
import argparse
import json
import logging
import threading
import signal
import yaml

from .common import VERSION, Meta

VERSION_PROP = "backer:version"
STATE_PROP = "backer:state"

class State:

    def __init__(self, meta, stored, remote_type, remote_cfg, remote_state):
        self.meta = meta
        self.stored = stored
        self.remote_type = remote_type
        self.remote_cfg = remote_cfg
        self.remote_state = remote_state

    def to_data(self):
        return json.dumps({
            'meta': self.meta.to_map(),
            'stored': self.stored,
            'remote_type': self.remote_type,
            'remote_cfg': self.remote_cfg,
            'remote_state': self.remote_state
        })

    @staticmethod
    def from_data(data):
        statemap = json.loads(data)
        meta = Meta.from_map(statemap['meta'])
        return State(meta, statemap['stored'], statemap['remote_type'], 
                statemap['remote_cfg'], statemap['remote_state'])

    @staticmethod
    def create(fs, metakey, remote):
        return State(Meta.create(fs, metakey), False,
                remote.type_, remote.cfg, None)
        
class Backsnap:

    def __init__(self, snapshot):
        self.snapshot = snapshot
        statedata = str(snapshot.get(STATE_PROP))
        self._state = State.from_data(statedata)
        self.meta = self._state.meta

    # ensure that the remote provided is compatible with
    # the remote in this backsnap
    def validate_remote(self, remote):
        if remote.type_ != self._state.remote_type:
            raise Exception("invalid remote")
        if remote.cfg != self._state.remote_cfg:
            raise Exception("invalid remote")

    def get_remote_state(self):
        return self._state.remote_state

    def set_remote_state(self, remote_state):
        self._state.remote_state = remote_state
        self._apply_state()

    def is_stored(self):
        return self._state.stored

    def set_stored(self, stored):
        self._state.stored = stored
        self._apply_state()

    def _apply_state(self):
        statedata = self._state.to_data()
        self.snapshot.set(STATE_PROP, self.snapshot.Value(statedata))

    @staticmethod
    def name(metakey):
        return "backer:%s-%s-%s-%s" % (VERSION, metakey.id_, metakey.sid, metakey.n)

    @staticmethod
    def create(fs, remote, metakey):
        state = State.create(fs, metakey, remote)
        snapshot = fs.snapshot(Backsnap.name(metakey), props={
                VERSION_PROP: fs.Value(VERSION),
                STATE_PROP: fs.Value(state.to_data())})
        return Backsnap(snapshot)

def get_all_backsnaps(fs, id_):
    unsorted_backsnaps = {}
    for name, props in fs.list_snapshots(keys=[VERSION_PROP]).items():
        if VERSION_PROP not in props:
            continue
        if props[VERSION_PROP] != VERSION:
            continue
        snapshot = fs.get_snapshot(name)
        backsnap = Backsnap(snapshot)
        if backsnap.meta.key.id_ != id_:
            continue
        sid = backsnap.meta.key.sid
        if sid not in unsorted_backsnaps:
            unsorted_backsnaps[sid] = []
        unsorted_backsnaps[sid].append(backsnap)
    sorted_backsnaps = {}
    for sid, backsnaps in unsorted_backsnaps.items():
        sorted_backsnaps[sid] = sorted(backsnaps, key=lambda x: x.snapshot.get_creation())
    return sorted_backsnaps

# latest is the list of backsnaps which also has a
# backsnap with the most recent creation
def get_latest_backsnaps(fs, id_):
    all_backsnaps = get_all_backsnaps(fs, id_)
    latest_backsnap = None
    for sid, backsnaps in all_backsnaps.items():
        if (latest_backsnap is None) or \
                (latest_backsnap.snapshot.get_creation() < backsnaps[-1].snapshot.get_creation()):
            latest_backsnap = backsnaps[-1]
    if latest_backsnap is None:
        return []
    return all_backsnaps[latest_backsnap.meta.key.sid]

def get_latest_stored(fs, id_):
    backsnaps = list(filter(lambda x: x.is_stored(), get_latest_backsnaps(fs, id_)))
    if len(backsnaps) == 0:
        return None
    return backsnaps[-1]

class Backup:

    def __init__(self, fs, remote, id_, period):
        self.fs = fs
        self.remote = remote
        self.id_ = id_
        self.period = period
        os.makedirs('/var/run/backer', exist_ok=True)

    def index(self):
        latest = get_latest_stored(self.fs, self.id_)
        if latest is not None:
            self.remote.index(latest)

    def backup(self, *, force=False):
        lock_filename = "/var/run/backer/backer-%s-%s-%s.lock" % \
                (VERSION, self.fs.get('guid'), self.id_)
        with open(lock_filename, 'w') as lock_file:
            fcntl.lockf(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self._backup(force=force)
    
    def _backup(self, *, force=False):
        backsnaps = get_latest_backsnaps(self.fs, self.id_)
        if len(backsnaps) == 0:
            metakey = Meta.Key.create(str(self.fs.get('guid')), self.id_)
            backsnaps.append(Backsnap.create(self.fs, self.remote, metakey))
        else:
            latest = backsnaps[-1]
            if force or (not latest.snapshot.check_is_current()):
                metakey = Meta.Key.from_key(latest.meta.key, n=latest.meta.key.n+1)
                latest.validate_remote(self.remote)
                backsnaps.append(Backsnap.create(self.fs, self.remote, metakey))
    
        previous = None
        for i in range(len(backsnaps)):
            backsnap = backsnaps[i]
            if not backsnap.is_stored():
                def streamer(stream, backsnap=backsnap):
                    self.remote.put_data(backsnap.meta.key, stream)
                if previous is None:
                    backsnap.snapshot.send(streamer)
                else:
                    backsnap.snapshot.send(streamer, other=previous.snapshot)
                self.remote.put_meta(backsnap.meta)
                # only index last one
                if i == (len(backsnaps) - 1):
                    self.remote.index(backsnap)
                backsnap.set_stored(True)
            if previous is not None:
                previous.snapshot.destroy()
            previous = backsnap
            
def restore(local, remote, meta_discovery, fsguid, id_, restore_fsname):
    latest_meta = meta_discovery(fsguid, id_)
    if latest_meta is None:
        raise Exception("latest not found")
    for n in range(latest_meta.key.n+1):
        metakey = Meta.Key.from_key(latest_meta.key, n=n)
        def streamer(stream, metakey=metakey):
            remote.get_data(metakey, stream)
        logging.debug("restore recv %s" % metakey)
        local.recv(restore_fsname, streamer)

_none = uuid.uuid4()
class Config:

    def __init__(self, *, cfg=None, filename=None):
        if not cfg is None:
            self.cfg = cfg
        elif not filename is None:
            if os.path.lexists(filename):
                with open(filename, 'r') as in_:
                    self.cfg = yaml.load(in_, Loader=yaml.FullLoader)
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
    parser.add_argument('-c', default='/usr/local/etc/backer.yaml', metavar='config')
    parser.add_argument('-d', action='store_true', help='debug')

    subparsers = parser.add_subparsers(dest='action')
    
    backup_parser = subparsers.add_parser('backup')
    backup_parser.add_argument('-n', metavar='[backup name]', required=True)
    backup_parser.add_argument('--force', action='store_true', help='force')

    index_parser = subparsers.add_parser('index')
    index_parser.add_argument('-n', metavar='[backup name]', required=True)

    backup_all_parser = subparsers.add_parser('backup-all')
    backup_all_parser.add_argument('--force', action='store_true', help='force')

    subparsers.add_parser('index-all')

    list_parser = subparsers.add_parser('list')
    list_parser.add_argument('-r', metavar='remote')
    list_parser.add_argument('-f', metavar='fsname')

    restore_parser = subparsers.add_parser('restore')
    restore_parser.add_argument('-l', metavar='local')
    restore_parser.add_argument('-r', metavar='remote')
    restore_parser.add_argument('-i', default='default', metavar='id')
    restore_parser.add_argument('-g', metavar='fsguid', required=True)
    restore_parser.add_argument('-f', metavar='fsname', required=True)

    subparsers.add_parser('daemon') 

    args = parser.parse_args()

    if args.d:
        logging.root.setLevel(logging.DEBUG)

    cfg = Config(filename=args.c)
    if VERSION != cfg['version']:
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
        period = cfg.get("backups.%s.period" % backup_name, default=60)
        backup = Backup(fs, remote, id_, period)
        backups[backup_name] = backup
        return backup

    action = args.action
    if action == 'backup':
        backup_name = args.n
        get_backup(backup_name).backup(force=args.force)
    elif action == 'index':
        backup_name = args.n
        get_backup(backup_name).index()
    elif action == 'backup-all':
        for backup_name in cfg.list_backups():
            get_backup(backup_name).backup(force=args.force)
    elif action == 'index-all':
        for backup_name in cfg.list_backups():
            get_backup(backup_name).index()
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
        metas = []
        for meta in remote.list():
            if (not args.f) or (args.f == meta.fsname):
                metas.append(meta.to_map())
        print(json.dumps(metas, indent=2, sort_keys=True))
    elif action == 'daemon':
        finished = threading.Event()

        def indexer_daemon():
            period = cfg.get('indexer:period', default=60)
            while not finished.is_set():
                for backup_name in cfg.list_backups():
                    backup = get_backup(backup_name)
                    try: 
                        backup.index()
                    except Exception as e:
                        logging.exception(e)
                if finished.wait(timeout=period):
                    break

        def backer_daemon():
            backup_times = {}
            for backup_name in cfg.list_backups():
                backup = get_backup(backup_name)
                latest = get_latest_stored(backup.fs, backup.id_)
                if latest is None:
                    next_ = 0
                else:
                    next_ = latest.snapshot.get_creation() + backup.period
                backup_times[backup_name] = (backup, next_)
            while not finished.is_set():
                for backup_name, (backup, next_) in backup_times.items():
                    if next_ < time.time():
                        try:
                            backup.backup()
                            next_ = time.time() + backup.period
                            backup_times[backup_name] = (backup, next_,)
                        except Exception as e:
                            logging.exception(e)
                if finished.wait(60):
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
