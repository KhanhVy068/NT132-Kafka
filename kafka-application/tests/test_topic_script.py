import os
from pathlib import Path
import shutil
import subprocess
import unittest

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "kafka-infrastructure" / "topics" / "init_topics.sh"
BASH = (os.getenv("GIT_BASH") or
        (r"C:\Program Files\Git\bin\bash.exe" if os.name == "nt"
         else shutil.which("bash")))


@unittest.skipUnless(BASH and Path(BASH).exists(), "Bash unavailable")
class TopicScriptTests(unittest.TestCase):
    def test_exact_partition_and_replication_fields(self):
        cases = [
            ("Topic: x PartitionCount: 2 ReplicationFactor: 1 Configs:", 0),
            ("Topic: x\tPartitionCount: 2\tReplicationFactor: 1\tConfigs:", 0),
            ("Topic: x PartitionCount: 20 ReplicationFactor: 1", 1),
            ("Topic: x PartitionCount: 2 ReplicationFactor: 10", 1),
            ("Topic: x PartitionCount: 1 ReplicationFactor: 1", 1),
            ("Topic: x PartitionCount: 2 ReplicationFactor: 2", 1),
            ("Topic: x PartitionCount: 2", 1),
            ("Topic: x PartitionCount: 2x ReplicationFactor: 1", 1),
            ("", 1),
        ]
        for description, expected in cases:
            with self.subTest(description=description):
                result = subprocess.run(
                    [BASH, "-c", 'source "$1"; verify_description "$2" 2 1',
                     "topic-test", SCRIPT.as_posix(), description],
                    capture_output=True, text=True, timeout=15,
                )
                self.assertEqual(result.returncode, expected, result.stderr)

    def test_bash_syntax(self):
        subprocess.run([BASH, "-n", str(SCRIPT)], check=True, timeout=15)
