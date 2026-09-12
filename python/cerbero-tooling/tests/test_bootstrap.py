import unittest

from cerbero_tooling import ARCHITECTURE_BASELINE, project_identity


class BootstrapTests(unittest.TestCase):
    def test_project_identity(self) -> None:
        self.assertEqual(project_identity(), "CERBERO")
        self.assertEqual(ARCHITECTURE_BASELINE, "v1.0")


if __name__ == "__main__":
    unittest.main()
