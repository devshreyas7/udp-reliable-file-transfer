"""Controller routing unit tests (Demo 1 & Demo 2) without requiring Ryu runtime."""
import ast
from pathlib import Path
import unittest


tree = ast.parse((Path(__file__).parent / "controller.py").read_text())
fn_nodes = [node for node in tree.body if isinstance(node, ast.FunctionDef)
            and node.name in ("chosen_path", "path_ports")]
scope = {"PRIMARY": ("s1-s2", "s2-s4"), "BACKUP": ("s1-s3", "s3-s4")}
exec(compile(ast.Module(body=fn_nodes, type_ignores=[]), "controller.py", "exec"), scope)


class RoutesTest(unittest.TestCase):
    def test_primary(self):
        self.assertEqual(scope["chosen_path"](set()), (1, 2, 4))

    def test_failover(self):
        self.assertEqual(scope["chosen_path"]({"s1-s2"}), (1, 3, 4))
        self.assertEqual(scope["chosen_path"]({"s2-s4"}), (1, 3, 4))

    def test_both_paths_down(self):
        self.assertIsNone(scope["chosen_path"]({"s1-s2", "s1-s3"}))

    def test_static_baseline_mode_does_not_reroute(self):
        """Demo 2: Verifies static baseline mode stays on primary and fails when primary breaks."""
        self.assertEqual(scope["chosen_path"](set(), static_mode=True), (1, 2, 4))
        self.assertIsNone(scope["chosen_path"]({"s1-s2"}, static_mode=True))

    def test_ports(self):
        self.assertEqual(scope["path_ports"]((1, 2, 4)),
                         [(1, 1, 2), (2, 1, 2), (4, 1, 3)])
        self.assertEqual(scope["path_ports"]((1, 3, 4)),
                         [(1, 1, 3), (3, 1, 2), (4, 2, 3)])


if __name__ == "__main__":
    unittest.main(verbosity=2)
