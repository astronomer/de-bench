Does the DAG in `dags/load_orders.py` work with the provider
versions pinned in `requirements.txt`? Read both files, decide
whether the imports and operator usages match what the pinned
provider releases actually expose, and write your findings to
`output.json` in this shape:

    {"removed_symbols": ["..."], "removed_kwargs": {"OperatorName": ["kwarg"]}}

Or, if you prefer a flat list, name kwargs with the operator they
belong to so the operator identity isn't lost:

    {"flags": ["SymbolName", "OperatorName.kwarg_name"]}

Either shape works. Empty arrays are fine if nothing is broken.
