import os
import unittest
from spooler.config import SpoolerConfig, parse_size_bytes


class TestSpoolerConfig(unittest.TestCase):
    def setUp(self):
        self._orig_env = os.environ.copy()

    def tearDown(self):
        os.environ.clear()
        os.environ.update(self._orig_env)

    def test_parse_size_bytes(self):
        self.assertEqual(parse_size_bytes(1024), 1024)
        self.assertEqual(parse_size_bytes("1024"), 1024)
        self.assertEqual(parse_size_bytes("1KB"), 1024)
        self.assertEqual(parse_size_bytes("1K"), 1024)
        self.assertEqual(parse_size_bytes("512MB"), 512 * 1024 * 1024)
        self.assertEqual(parse_size_bytes("512M"), 512 * 1024 * 1024)
        self.assertEqual(parse_size_bytes("1GB"), 1024 * 1024 * 1024)
        self.assertEqual(parse_size_bytes("1G"), 1024 * 1024 * 1024)
        self.assertEqual(parse_size_bytes("2GB"), 2 * 1024 * 1024 * 1024)
        self.assertEqual(parse_size_bytes("1.5GB"), int(1.5 * 1024 * 1024 * 1024))

        with self.assertRaises(ValueError):
            parse_size_bytes("invalid_size")

    def test_default_values(self):
        # Clean any env overrides for default check
        os.environ.pop("SPOOLER_READ_CACHE_SIZE", None)
        os.environ.pop("SPOOLER_READ_CACHE_BYTES", None)
        os.environ.pop("SPOOLER_WRITE_BUFFER_SIZE", None)
        os.environ.pop("SPOOLER_MAX_RAM_BYTES", None)

        config = SpoolerConfig()
        self.assertEqual(config.read_cache_max_bytes, 1024 * 1024 * 1024)  # 1 GB
        self.assertEqual(config.max_ram_bytes, 512 * 1024 * 1024)          # 512 MB
        self.assertEqual(config.max_queue_memory_bytes, 512 * 1024 * 1024)

    def test_string_size_arguments(self):
        config = SpoolerConfig(read_cache_max_bytes="2GB", max_ram_bytes="1GB")
        self.assertEqual(config.read_cache_max_bytes, 2 * 1024 * 1024 * 1024)
        self.assertEqual(config.max_ram_bytes, 1024 * 1024 * 1024)

    def test_aliases(self):
        config = SpoolerConfig(read_cache_size="256MB", write_buffer_size="128MB")
        self.assertEqual(config.read_cache_max_bytes, 256 * 1024 * 1024)
        self.assertEqual(config.max_ram_bytes, 128 * 1024 * 1024)

    def test_environment_variable_overrides(self):
        os.environ["SPOOLER_READ_CACHE_SIZE"] = "3GB"
        os.environ["SPOOLER_WRITE_BUFFER_SIZE"] = "768MB"

        config = SpoolerConfig()
        self.assertEqual(config.read_cache_max_bytes, 3 * 1024 * 1024 * 1024)
        self.assertEqual(config.max_ram_bytes, 768 * 1024 * 1024)


if __name__ == "__main__":
    unittest.main()
