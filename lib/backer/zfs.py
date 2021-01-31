#!/usr/bin/env python3

import time
import shutil
import os
import socket
import uuid
import subprocess
from subprocess import check_output
from datetime import datetime

def toutc(timestamp):
    return timestamp + time.timezone

class Value:

    def __init__(self, raw):
        if type(raw) is not str:
            raise Exception()
        if raw == '-':
            raise Exception()
        self.raw = raw

    def __str__(self):
        return self.raw

    def __repr__(self):
        return str(self)

    @staticmethod
    def parse(raw):
        if raw == '-':
            return None
        return Value(raw)

def validate_prop(key, value):
    if type(key) is not str:
        raise TypeError(key)
    if len(key) == 0:
        raise ValueError(key)
    if type(value) is not Value:
        raise TypeError(value)

def validate_props(props):
    for key, value in props.items():
        validate_prop(key, value)

def validate_snapshot_name(name):
    if type(name) is not str:
        raise TypeError(name)
    if len(name) == 0:
        raise ValueError(name)

def validate_snapshot_fullname(name):
    if type(name) is not str:
        raise TypeError(name)
    if len(name) == 0:
        raise ValueError(name)
    try:
        name.index('@')
    except ValueError:
        raise ValueError(name)

class Snapshot:

    Value = Value

    def __init__(self, fs, name):
        # sanity check to make sure that we are actually
        # dealing with a snapshot
        validate_snapshot_fullname(name)
        self.fs = fs
        self.name = name

    def set(self, key, value):
        validate_prop(key, value)
        subprocess.run(["zfs", "set", "%s=%s" % (key, value),
                self.name], check=True)

    def get(self, key):
        output = check_output(["zfs", "get", "-p", "-H", "-o", "value",
                key, self.name])
        return Value.parse(output.rstrip().decode('utf8'))

    # TODO return datetime with tz
    def get_creation(self):
        return toutc(int(str(self.get('creation'))))

    def send(self, streamer, *, other=None):
        if other is None:
            args = ['zfs', "send", "-p", self.name]
        else:
            args = ['zfs', "send", "-p", "-i", other.name, self.name]
        with subprocess.Popen(args, stdout=subprocess.PIPE) as proc:
            streamer(proc.stdout)
            rc = proc.wait()
        if rc != 0:
            raise Exception(rc)

    def check_is_current(self):
        output = check_output(['zfs', 'diff', self.name])
        output = output.rstrip().decode('utf8')
        if len(output) == 0:
            return True
        return False

    def destroy(self):
        subprocess.run(['zfs', 'destroy', self.name], check=True)

    def __str__(self):
        return "Snapshot(%s)" % self.name

    def __repr__(self):
        return str(self)

class Filesystem:

    Value = Value

    def __init__(self, name):
        self.name = name 

    def _full_snapshot_name(self, name):
        return "%s@%s" % (self.name, name)

    def snapshot(self, name, *, props=None):
        validate_snapshot_name(name)
        if props is not None:
            validate_props(props)
        fullname = self._full_snapshot_name(name)
        args = ['zfs', 'snapshot']
        if props is not None:
            for key, value in props.items():
                args.extend(['-o', "%s=%s" % (key, value)])
        args.append(fullname)
        subprocess.run(args, check=True)
        return Snapshot(self, fullname)

    def get_snapshot(self, name):
        if not self.check_snapshot_exists(name):
            raise Exception("unknown snapshot: %s" % name)
        return Snapshot(self, self._full_snapshot_name(name))

    def list_snapshots(self, *, keys=None):
        args = ['zfs', "list", "-t", "snapshot", "-H", '-r']
        if keys is None:
            keysarg = 'name'
        else:
            keysarg = "name,%s" % ','.join(keys)
        args.extend(['-o', keysarg])
        args.append(self.name)
        output = check_output(args)
        output = output.rstrip().decode('utf8')
        snapshots = {}
        if len(output) != 0:
            for line in output.split("\n"):
                values = line.split('\t')
                fullname = values[0]
                name = fullname.split("@", 1)[1]
                props = {}
                for i in range(1, len(values)):
                    key = keys[i-1]
                    value = values[i]
                    if value != '-':
                        props[key] = value
                snapshots[name] = props
        return snapshots

    def get(self, key):
        output = check_output(['zfs', 'get', '-p', '-H', '-o', 'value', key, self.name])
        return Value.parse(output.rstrip().decode('utf8'))

    # TODO return datetime with tz
    def get_creation(self):
        return toutc(int(str(self.get('creation'))))

    def get_all(self):
        props = {}
        output = check_output(['zfs', 'get', '-H', '-p', 'all', self.name])
        output = output.rstrip().decode('utf8')
        for line in output.split('\n'):
            parts = line.split('\t')
            key = parts[1]
            value = Value.parse(parts[2])
            props[key] = value
        return props

    def check_snapshot_exists(self, name):
        proc = subprocess.run(['zfs', 'list', '-t', 'snapshot', self._full_snapshot_name(name)],
                stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)
        if proc.returncode == 0:
            return True
        return False

    def __str__(self):
        return "Filesystem(%s)" % self.name

    def __repr__(self):
        return str(self)

def check_filesystem_exists(name):
    proc = subprocess.run(['zfs', 'list', '-t', 'filesystem', name],
                stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)
    if proc.returncode == 0:
        return True
    return False

def list_filesystems():
    output = check_output(['zfs', 'list', '-t', 'filesystem', '-H', '-o', 'name'])
    output = output.rstrip().decode('utf8')
    filesystems = {}
    if len(output) != 0:
        for name in output.split('\n'):
            filesystems[name] = {}
    return filesystems

def get_filesystem(name):
    if not check_filesystem_exists(name):
        raise Exception("unknown filesystem: %s" % name)
    return Filesystem(name)

def recv(name, streamer):
    args = ['zfs', "recv", "-u", name]
    with subprocess.Popen(args, stdin=subprocess.PIPE) as proc:
        streamer(proc.stdin)
        proc.stdin.close()
        rc = proc.wait()
    if rc != 0:
        raise Exception(rc)
    return Filesystem(name)
