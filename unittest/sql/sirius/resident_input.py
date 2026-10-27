"""Bounded, immutable benchmark snapshots; no production cache invalidation."""
import ctypes as ct
import json

import pyarrow as pa
from substrait import plan_pb2


class ResidentInput:
    def __init__(self, storage):
        self.storage = storage
        self.used = 0
        self.records = {}

    def copy_batch(self, batch):
        arrays = []
        for array in batch.columns:
            if array.offset or array.type not in (pa.int64(), pa.float64()):
                raise ValueError('Require unsliced int64/float64 native input')
            buffers = []
            for buffer in array.buffers():
                if buffer is None:
                    buffers.append(None)
                    continue
                start = (self.used + 63) // 64 * 64
                end = start + buffer.size
                if end > len(self.storage):
                    raise MemoryError('Resident benchmark input capacity exceeded')
                destination = self.storage[start:end]
                if buffer.size:
                    ct.memmove(ct.addressof(ct.c_char.from_buffer(destination)), buffer.address, buffer.size)
                buffers.append(pa.py_buffer(destination))
                self.used = end
            arrays.append(pa.Array.from_buffers(array.type, len(array), buffers, null_count=array.null_count))
        return pa.RecordBatch.from_arrays(arrays, schema=batch.schema)

    def clear(self):
        self.records.clear()
        self.used = 0


def merge_results(results, sums):
    """One BIGINT key, optional DOUBLE sum, and COUNT; matching live adapter."""
    merged = {}
    for result in results:
        for row in result['rows']:
            if len(row) != 2 + sums:
                raise ValueError('Unexpected aggregate result width')
            key = row[0]
            if key not in merged:
                merged[key] = list(row)
            else:
                prior = merged[key]
                if sums and row[1] is not None:
                    prior[1] = row[1] if prior[1] is None else prior[1] + row[1]
                prior[-1] += row[-1]
    return {'rows': list(merged.values())}


class RecordingBackend:
    def __init__(self, backend, cache):
        self.backend = backend
        self.cache = cache
        self.input_buffer = None
        self.native_chunk_bytes = 128 * 1024**2
        self.native_fragment_bytes = getattr(backend, 'native_fragment_bytes', 512 * 1024**2)
        self.capture = None
        self.tokens = {}
        self.events = []

    def token(self, key):
        record = self.cache.records[key]
        plan = plan_pb2.Plan.FromString(record[0]['plan'])
        plan.version.producer = 'seekdb-resident-benchmark:' + key
        encoded = plan.SerializeToString()
        self.tokens[encoded] = key
        return encoded

    def execute(self, plan, reader=None):
        if reader is None and bytes(plan) in self.tokens:
            key = self.tokens[bytes(plan)]
            results = []
            for record in self.cache.records[key]:
                with pa.RecordBatchReader.from_batches(record['schema'], record['batches']) as replay:
                    results.append(json.loads(self.backend.execute(record['plan'], replay)))
                self.events.append({'mode': 'resident', 'rows': record['rows']})
            return json.dumps(merge_results(results, int(key == 'sum_count'))).encode()
        if reader is None:
            return self.backend.execute(plan)
        record = {'plan': bytes(plan), 'schema': reader.schema, 'batches': [], 'rows': 0}
        capture = self.capture
        def batches():
            for batch in reader:
                record['rows'] += batch.num_rows
                if capture is not None:
                    record['batches'].append(self.cache.copy_batch(batch))
                yield batch
        with pa.RecordBatchReader.from_batches(reader.schema, batches()) as wrapped:
            result = self.backend.execute(plan, wrapped)
        if capture is not None:
            self.cache.records.setdefault(capture, []).append(record)
        self.events.append({'mode': 'capture' if capture else 'live', 'rows': record['rows']})
        return result
