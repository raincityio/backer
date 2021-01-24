#!/usr/bin/env python3

import time
import socket

VERSION='7e'

class Meta:

    class Key:

        def __init__(self, fsguid, id_, sid, n):
            self.fsguid = fsguid
            self.id_ = id_
            self.sid = str(sid).replace('-', '')
            self.n = n

        def to_map(self):
            return {
                'fsguid': self.fsguid,
                'id': self.id_,
                'sid': self.sid,
                'n': self.n
            }

        def __str__(self):
            return "[fsguid=%s, id=%s, sid=%s, n=%s]" % (self.fsguid, self.id_, self.sid, self.n)

        @staticmethod
        def from_map(keymap):
            return Meta.Key(keymap['fsguid'], keymap['id'], keymap['sid'], keymap['n'])

    def __init__(self, key, fsname, fscreation, hostname, creation):
        self.key = key
        self.fsname = fsname
        self.fscreation = fscreation
        self.hostname = hostname
        self.creation = creation

    def __str__(self):
        return "[key=%s, fsname=%s, fscreation=%s, hostname=%s, creation=%s]" % \
            (self.key, self.fsname, self.fscreation, self.hostname, self.creation)

    def to_map(self):
        return {
            'key': self.key.to_map(),
            'fsname': self.fsname,
            'fscreation': self.fscreation,
            'hostname': self.hostname,
            'creation': self.creation
        }

    @staticmethod
    def from_map(metamap):
        key = Meta.Key.from_map(metamap['key'])
        fsname = metamap['fsname']
        fscreation = metamap['fscreation']
        hostname = metamap['hostname']
        creation = metamap['creation']
        return Meta(key, fsname, fscreation, hostname, creation)

    @staticmethod
    def create(fs, key):
        return Meta(key, fs.name, fs.get_creation(), socket.gethostname(), time.time())
