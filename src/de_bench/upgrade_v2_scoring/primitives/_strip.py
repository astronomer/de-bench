"""Source-stripping helper used by `_has_deprecated_context_var`.

Lives in its own module so the AST + tokenize imports stay lazy at
the metric hot path: `upgrades.py` imports this only when scanning
a file, not at registry-import time.
"""

from __future__ import annotations


def strip_comments_and_docstrings(source: str) -> str | None:
    """Return `source` with `#` comments and *real* docstrings
    (Module/ClassDef/FunctionDef/AsyncFunctionDef first-statement
    string expressions) replaced with whitespace, preserving line
    offsets. Regular string literals — including dict values,
    lambdas, annotations, `if` block strings, and any other
    expression-context strings — are kept intact, because the
    only thing the downstream regex cares about is whether a
    deprecated context-var name appears in *executable code*, and
    `ctx['execution_date']` is the canonical deprecated usage.

    Codex adversarial review (2026-04-26) flagged the prior
    implementation: it tracked the last non-trivia token and
    treated *any* STRING after `:` as a docstring. That mis-
    classified `KEYS = {"bad": "execution_date"}` as a docstring
    drop, hiding a real dynamic deprecated-var access via
    `context[KEYS["bad"]]`. AST-based detection only nominates
    docstrings that actually live in the four AST node positions
    Python recognises as docstring slots, so dict values / lambda
    bodies / annotation strings stay in the scanned source.

    Returns None on parse / tokenise failure so the caller can
    fall back to the raw text scan — preferable to silently
    treating an unparseable file as clean.
    """
    import ast
    import io
    import tokenize

    try:
        tree = ast.parse(source)
    except (SyntaxError, ValueError):
        return None

    # Identify docstring nodes by their (lineno, col_offset). A
    # docstring is the first statement of a Module / ClassDef /
    # FunctionDef / AsyncFunctionDef body, where that statement is
    # an `ast.Expr` whose `.value` is an `ast.Constant` of `str`.
    docstring_positions: set[tuple[int, int]] = set()
    docstring_owners = (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)
    for node in ast.walk(tree):
        if not isinstance(node, docstring_owners):
            continue
        body = getattr(node, "body", None)
        if not body:
            continue
        first = body[0]
        if not isinstance(first, ast.Expr):
            continue
        if not isinstance(first.value, ast.Constant):
            continue
        if not isinstance(first.value.value, str):
            continue
        docstring_positions.add((first.value.lineno, first.value.col_offset))

    try:
        tokens = list(tokenize.generate_tokens(io.StringIO(source).readline))
    except (tokenize.TokenizeError, IndentationError, SyntaxError):
        return None

    out: list[str] = []
    for tok in tokens:
        ttype, tstr, start, _end, _line = tok
        if ttype == tokenize.COMMENT:
            out.append("\n" * tstr.count("\n"))
            continue
        if ttype == tokenize.STRING and start in docstring_positions:
            # AST flagged this exact STRING token as a real
            # docstring — strip while preserving line count.
            out.append("\n" * tstr.count("\n"))
            continue
        out.append(tstr)
    return "".join(out)
