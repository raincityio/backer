#!/usr/bin/env python3

import time
import socket

VERSION='5'

class Meta:

    class Key:

        def __init__(self, fsguid, id_, n):
            self.fsguid = fsguid
            self.id_ = id_
            self.n = n

        def to_map(self):
            return {
                'fsguid': self.fsguid,
                'id': self.id_,
                'n': self.n
            }

        def __str__(self):
            return "[fsguid=%s, id=%s, n=%s]" % (self.fsguid, self.id_, self.n)

        @staticmethod
        def from_map(keymap):
            return Meta.Key(keymap['fsguid'], keymap['id'], keymap['n'])

    def __init__(self, key, fsname, created, version, hostname, base):
        self.key = key
        self.fsname = fsname
        self.created = created
        self.version = version
        self.hostname = hostname
        self.base = base

    def __str__(self):
        return "[key=%s, fsname=%s, created=%s, version=%s, hostname=%s, base=%s]" % \
            (self.key, self.fsname, self.created, self.version, self.hostname, self.base)

    def to_map(self):
        return {
            'key': self.key.to_map(),
            'fsname': self.fsname,
            'created': self.created,
            'version': self.version,
            'hostname': self.hostname,
            'base': self.base
        }

    @staticmethod
    def from_map(metamap):
        key = Meta.Key.from_map(metamap['key'])
        fsname = metamap['fsname']
        created = metamap['created']
        version = metamap['version']
        hostname = metamap['hostname']
        base = metamap['base']
        return Meta(key, fsname, created, version, hostname, base)

    @staticmethod
    def create(fs, key):
        return Meta(key, fs.name, time.time(), VERSION, socket.gethostname(), 0)
