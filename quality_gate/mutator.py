"""Isolated AST-level fault injection for mutation testing.

Each mutant is a *copy* of the production package written into a throwaway
directory under the system temp area; the files inside ``ferry_observer`` are
never modified, and no mutant bytecode is written into the working tree. The
faults are injected by rewriting the syntax tree -- never by matching source
text -- so the check cannot be faked by renaming an identifier.

Three contract faults are available:

``duplicate_batch_advances_seq``
    Empty/duplicate-only batches incorrectly consume a knowledge sequence
    (the sequence assignment becomes unconditional).

``filter_time_before_revision``
    The report filters revisions by event time *before* choosing the highest
    revision, resurrecting a corrected old value when the correction moved
    beyond ``T``.

``redelivery_overwrites_first_source``
    Re-delivering an accepted revision rewrites its first-source ownership.
"""
import ast
import shutil
import sys
import tempfile
from pathlib import Path

MUTATIONS = (
    "duplicate_batch_advances_seq",
    "filter_time_before_revision",
    "redelivery_overwrites_first_source",
)


def _package_root():
    import ferry_observer

    return Path(ferry_observer.__file__).resolve().parent


# ---------------------------------------------------------------- AST helpers
class _ForceSequenceAdvance(ast.NodeTransformer):
    """Make knowledge_seq assignment unconditional inside ``ingest``."""

    def __init__(self):
        self._in_ingest = False

    def visit_FunctionDef(self, node):
        if node.name != "ingest":
            return node
        self._in_ingest = True
        self.generic_visit(node)
        self._in_ingest = False
        return node

    def visit_IfExp(self, node):
        self.generic_visit(node)
        if self._in_ingest and isinstance(node.test, ast.Name) and node.test.id == "new_records":
            # next_seq = _current_seq(db) + 1  (drop the "else None")
            return node.body
        return node

    def visit_If(self, node):
        self.generic_visit(node)
        if self._in_ingest and isinstance(node.test, ast.Name) and node.test.id == "new_records":
            return ast.If(test=ast.Constant(True), body=node.body, orelse=node.orelse)
        return node


class _FilterTimeBeforeRevision(ast.NodeTransformer):
    """Apply the event-time cutoff before revision selection."""

    def visit_FunctionDef(self, node):
        if node.name != "performance":
            return node
        node = self.generic_visit(node)
        # After the (wrong) event-time filter some events have no surviving
        # revision, so the evidence loop's winner lookup must skip them. The
        # semantic resurrection bug remains and is still caught by report
        # comparison; this guard only keeps the mutant executable.
        class _WinnerGuard(ast.NodeTransformer):
            def visit_Assign(self, inner):
                if not (
                    isinstance(inner.targets, list)
                    and len(inner.targets) == 1
                    and isinstance(inner.targets[0], ast.Name)
                    and inner.targets[0].id == "winner"
                    and isinstance(inner.value, ast.Subscript)
                    and isinstance(inner.value.value, ast.Name)
                    and inner.value.value.id == "winners"
                ):
                    return inner
                guarded = ast.parse(
                    "winner = winners.get(rec['event_id'])\n"
                    "if winner is None:\n    continue"
                ).body
                return [
                    ast.fix_missing_locations(guarded[0]),
                    ast.fix_missing_locations(guarded[1]),
                ]

        return ast.fix_missing_locations(_WinnerGuard().visit(node))

    def visit_Assign(self, node):
        value = node.value
        if not (
            isinstance(value, ast.Call)
            and isinstance(value.func, ast.Name)
            and value.func.id == "_projection"
        ):
            return node
        # winners = _projection([r for r in revisions
        #                         if r["action"] != "upsert"
        #                         or instant(r["occurred_at"]) <= cutoff])
        comprehension = ast.ListComp(
            elt=ast.Name(id="r", ctx=ast.Load()),
            generators=[
                ast.comprehension(
                    target=ast.Name(id="r", ctx=ast.Store()),
                    iter=ast.Name(id="revisions", ctx=ast.Load()),
                    ifs=[
                        ast.BoolOp(
                            op=ast.Or(),
                            values=[
                                ast.Compare(
                                    left=ast.Subscript(
                                        value=ast.Name(id="r", ctx=ast.Load()),
                                        slice=ast.Constant("action"),
                                        ctx=ast.Load(),
                                    ),
                                    ops=[ast.NotEq()],
                                    comparators=[ast.Constant("upsert")],
                                ),
                                ast.Compare(
                                    left=ast.Call(
                                        func=ast.Name(id="instant", ctx=ast.Load()),
                                        args=[
                                            ast.Subscript(
                                                value=ast.Name(id="r", ctx=ast.Load()),
                                                slice=ast.Constant("occurred_at"),
                                                ctx=ast.Load(),
                                            )
                                        ],
                                        keywords=[],
                                    ),
                                    ops=[ast.LtE()],
                                    comparators=[ast.Name(id="cutoff", ctx=ast.Load())],
                                ),
                            ],
                        )
                    ],
                    is_async=0,
                )
            ],
        )
        value.args = [comprehension]
        value.keywords = []
        return ast.fix_missing_locations(node)


class _RedeliveryOverwritesSource(ast.NodeTransformer):
    """Rewrite first-source ownership when a revision is redelivered."""

    def __init__(self):
        self._in_ingest = False

    def visit_FunctionDef(self, node):
        if node.name != "ingest":
            return node
        self._in_ingest = True
        self.generic_visit(node)
        self._in_ingest = False
        return node

    def visit_If(self, node):
        if not self._in_ingest:
            return node
        # The ledger-hit branch: `if prior is not None:`.
        is_ledger_hit = (
            isinstance(node.test, ast.Compare)
            and isinstance(node.test.left, ast.Name)
            and node.test.left.id == "prior"
            and len(node.test.ops) == 1
            and isinstance(node.test.ops[0], ast.IsNot)
            and isinstance(node.test.comparators[0], ast.Constant)
            and node.test.comparators[0].value is None
        )
        if not is_ledger_hit:
            return self.generic_visit(node)
        overwrite = ast.parse(
            'db.execute(\n'
            '    "UPDATE revisions SET source_sha256=?"\n'
            '    " WHERE event_id=? AND revision=?",\n'
            '    (digest, rec["event_id"], rec["revision"]),\n'
            ')'
        ).body[0]
        # Inject before the existing duplicate accounting / continue.
        node.body.insert(0, ast.fix_missing_locations(overwrite))
        return ast.fix_missing_locations(node)


_TRANSFORMS = {
    "duplicate_batch_advances_seq": ("storage.py", _ForceSequenceAdvance),
    "filter_time_before_revision": ("analysis.py", _FilterTimeBeforeRevision),
    "redelivery_overwrites_first_source": ("storage.py", _RedeliveryOverwritesSource),
}


def mutate_source(module_name, source, mutation):
    """Return rewritten source code for one module and one mutation."""
    filename, transformer_cls = _TRANSFORMS[mutation]
    if module_name != filename:
        return source
    tree = ast.parse(source, filename=module_name)
    transformed = ast.fix_missing_locations(transformer_cls().visit(tree))
    return ast.unparse(transformed) + "\n"


class MutantPackage:
    """A loaded, mutated copy of the production package."""

    def __init__(self, mutation):
        if mutation not in MUTATIONS:
            raise ValueError(f"unknown mutation {mutation!r}")
        self.mutation = mutation
        self.tempdir = tempfile.mkdtemp(prefix=f"qg-mutant-{mutation}-")
        self.alias = f"ferry_qg_mutant_{mutation}"
        self.path = Path(self.tempdir) / self.alias
        shutil.copytree(_package_root(), self.path)
        filename, _transformer = _TRANSFORMS[mutation]
        target = self.path / filename
        target.write_text(
            mutate_source(filename, target.read_text(), mutation),
            encoding="utf-8",
        )
        if self.alias in sys.modules:
            del sys.modules[self.alias]
        sys.path.insert(0, self.tempdir)
        try:
            self.module = __import__(self.alias)
        finally:
            sys.path.remove(self.tempdir)

    def production_api(self):
        """Return ``(ingest, performance, write_report)`` from the mutant."""
        storage = __import__(f"{self.alias}.storage", fromlist=["ingest"])
        analysis = __import__(
            f"{self.alias}.analysis", fromlist=["performance", "write_report"]
        )
        return storage.ingest, analysis.performance, analysis.write_report

    def cleanup(self):
        for name in list(sys.modules):
            if name == self.alias or name.startswith(self.alias + "."):
                del sys.modules[name]
        shutil.rmtree(self.tempdir, ignore_errors=True)
