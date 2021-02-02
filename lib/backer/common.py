#!/usr/bin/env python3

import json
import uuid
import datetime
import socket

VERSION='11'

def utcnow():
    return datetime.datetime.utcnow().timestamp()

class Meta:

    class Key:

        def __init__(self, fsid, bid, sid, n):
            self.fsid = fsid
            self.bid = bid
            self.sid = sid
            self.n = n

        def to_map(self):
            return {
                'fsid': self.fsid,
                'bid': self.bid,
                'sid': self.sid,
                'n': self.n
            }

        def __str__(self):
            return "[fsid=%s, bid=%s, sid=%s, n=%s]" % (self.fsid, self.bid, self.sid, self.n)

        @staticmethod
        def from_map(keymap):
            return Meta.Key(keymap['fsid'], keymap['bid'], keymap['sid'], keymap['n'])

        @staticmethod
        def from_key(key, *, n=None):
            n = key.n if n is None else n
            return Meta.Key(key.fsid, key.bid, key.sid, n)

        @staticmethod
        def create(fsid, bid):
            sid = str(uuid.uuid4()).replace('-', '')
            return Meta.Key(fsid, bid, sid, 0)

    def __init__(self, key, fsname, fscreation, hostname, creation, screation):
        self.key = key
        self.fsname = fsname
        self.fscreation = fscreation
        self.hostname = hostname
        self.creation = creation
        self.screation = screation

    def __str__(self):
        return "[key=%s, fsname=%s, fscreation=%s, hostname=%s, creation=%s, screation=%s]" % \
            (self.key, self.fsname, self.fscreation, self.hostname, self.creation, self.screation)

    def to_data(self):
        return json.dumps(self.to_map())

    def to_map(self):
        return {
            'key': self.key.to_map(),
            'fsname': self.fsname,
            'fscreation': self.fscreation,
            'hostname': self.hostname,
            'creation': self.creation,
            'screation': self.screation
        }

    @staticmethod
    def from_data(data):
        return Meta.from_map(json.loads(data))

    @staticmethod
    def from_map(metamap):
        key = Meta.Key.from_map(metamap['key'])
        fsname = metamap['fsname']
        fscreation = metamap['fscreation']
        hostname = metamap['hostname']
        creation = metamap['creation']
        screation = metamap['screation']
        return Meta(key, fsname, fscreation, hostname, creation, screation)

    @staticmethod
    def from_meta(meta, *, key=None, fsname=None, hostname=None):
        creation = int(utcnow())
        key = meta.key if key is None else key
        fsname = meta.fsname if fsname is None else fsname
        hostname = meta.hostname if hostname is None else hostname
        return Meta(key, fsname, meta.fscreation, hostname, creation, meta.screation)

    @staticmethod
    def create(fs, bid):
        creation = int(utcnow())
        key = Meta.Key.create(str(fs.get('guid')), bid)
        fscreation = fs.get_creation()
        return Meta(key, fs.name, fscreation, socket.gethostname(), creation, creation)
