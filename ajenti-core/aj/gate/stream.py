from gevent.lock import RLock
import binascii
import gevent.socket
import json
import os
import six
import logging

MSG_SIZE_LIMIT = 1024 * 1024 * 128  # 128 MB
MSG_CONTINUATION_MARKER = '\x00\x00\x00continued\x00\x00\x00'
MSG_STREAM_FINISHED = '\x00\x00\x00stream_end\x00\x00\x00'


def _seq_split(object):
    while object:
        yield object[:MSG_SIZE_LIMIT] + (MSG_CONTINUATION_MARKER if len(object) > MSG_SIZE_LIMIT else '')
        object = object[MSG_SIZE_LIMIT:]


def _seq_is_continued(part):
    return part.endswith(MSG_CONTINUATION_MARKER)


def _seq_combine(parts):
    object = ''
    for part in parts[:-1]:
        object += part[len(MSG_CONTINUATION_MARKER):]
    object += parts[-1]
    return object


def _pipe_get_generator(pipe, timeout=None):
    # Get data from pipe and share it as generator

    while True:
        if timeout:
            with gevent.Timeout(timeout) as t:
                part = pipe.get(t)
        else:
            part = pipe.get()
        if part == MSG_STREAM_FINISHED:
            # print ('get', 'TERMINATED')
            break
        # print ('get', part)
        yield part


def _pipe_put_iterator(pipe, generator):
    # Put data from generator to the pipe

    for chunk in generator:
        # print ('put', chunk)
        pipe.put(chunk)
    pipe.put(MSG_STREAM_FINISHED)
    # print ('put', 'TERMINATED')


class GateStreamRequest(object):
    def __init__(self, obj, endpoint, payload_iter):
        self.id = binascii.hexlify(os.urandom(32)).decode()
        self.object = obj
        self.endpoint = endpoint
        self.payload_iter = payload_iter

    def serialize(self):
        object_tmp = {k : (v.decode()  if isinstance(v, six.binary_type) else v) for k, v in self.object.items()}
        return {
            'id': self.id,
            'object': object_tmp,
        }

    @classmethod
    def deserialize(cls, data, payload_iter):
        self = cls(data['object'], None, payload_iter)
        self.id = data['id']
        return self


class GateStreamResponse(object):
    def __init__(self, _id, obj, payload_iter):
        self.id = _id
        self.object = obj
        self.payload_iter = payload_iter

    def serialize(self):
        return {
            'id': self.id,
            'object': self.object,
        }

    @classmethod
    def deserialize(cls, data, payload_iter):
        self = cls(data['id'], data['object'], payload_iter)
        return self


class GateStreamServerEndpoint(object):
    def __init__(self, pipe, payload_pipe):
        self.pipe = pipe
        self.payload_pipe = payload_pipe
        self.buffer = {}
        self.buffer_lock = RLock()
        self.log = False

    def send(self, obj, payload_iter=[]):
        # sending request data to the worker

        rq = GateStreamRequest(obj, self, payload_iter)
        for part in _seq_split(json.dumps(rq.serialize())):
            self.pipe.put(part)

        if payload_iter:
            _pipe_put_iterator(self.payload_pipe, payload_iter)

        if self.log:
            logging.debug('%s: >> %s', self, rq.id)
        return rq

    def recv_single(self, timeout):
        # read request data
        try:
            if timeout:
                with gevent.Timeout(timeout) as t:
                    data = self.pipe.get(t)
            else:
                data = self.pipe.get()
            payload_iter = _pipe_get_generator(self.payload_pipe, timeout)
            resp = GateStreamResponse.deserialize(data, payload_iter)
            return resp

        # pylint: disable=E0712
        except gevent.Timeout:
            return None
        except EOFError:
            return None

    def buffer_single_response(self, timeout):
        try:
            with self.buffer_lock:
                resp = self.recv_single(timeout)
                if not resp:
                    return None
                if self.log:
                    logging.debug('%s: << %s', self, resp.id)
                self.buffer[resp.id] = resp
                return resp
        except IOError:
            return None

    def list_responses(self):
        with self.buffer_lock:
            return list(self.buffer)

    def has_response(self, _id):
        with self.buffer_lock:
            return _id in self.buffer

    def ack_response(self, _id):
        with self.buffer_lock:
            return self.buffer.pop(_id)

    def destroy(self):
        self.pipe.close()
        self.payload_pipe.close()

class GateStreamWorkerEndpoint(object):
    def __init__(self, pipe, payload_pipe):
        self.pipe = pipe
        self.payload_pipe = payload_pipe
        self.log = False

    def reply(self, request, obj, payload_iter=[]):
        # Send response data to the worker

        resp = GateStreamResponse(request.id if request else None, obj, payload_iter)
        self.pipe.put(resp.serialize())
        if payload_iter:
            _pipe_put_iterator(self.payload_pipe, payload_iter)

        if self.log:
            logging.debug('%s: >> %s', self, resp.id)

    def recv(self):
        # Receive response data

        parts = [self.pipe.get()]
        while _seq_is_continued(parts[-1]):
            parts.append(self.pipe.get())

        json_object = _seq_combine(parts)
        obj = json.loads(json_object)
        payload_iter = _pipe_get_generator(self.payload_pipe)

        rq = GateStreamRequest.deserialize(obj, payload_iter)
        if self.log:
            logging.debug('%s: << %s', self, rq.id)
        return rq
