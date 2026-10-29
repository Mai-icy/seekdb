"""Run exact scalar transport cases against the real Sirius GPU backend."""
import os
import unittest
import test_native_exact
from sidecar import Backend


class GPUExact(test_native_exact.NativeExact):
    @classmethod
    def setUpClass(cls):
        library = os.getenv('SIRIUS_TEST_LIBRARY')
        if not library: raise unittest.SkipTest('Set SIRIUS_TEST_LIBRARY for real GPU validation')
        cls.backend = Backend(library)

    @classmethod
    def tearDownClass(cls):
        cls.backend.close()

    def run_stream(self, batches, columns=1, chunk_bytes=1, backend=None):
        if backend is None:
            backend = self.backend
            backend.native_chunk_bytes = chunk_bytes
        return super().run_stream(batches, columns, chunk_bytes, backend)


if __name__ == '__main__': unittest.main()
