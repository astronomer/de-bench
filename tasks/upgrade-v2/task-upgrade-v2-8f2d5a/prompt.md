Read `requirements.txt`. Decide whether the pinned `apache-airflow` and
`apache-airflow-providers-cncf-kubernetes` versions install cleanly together.
If they do not, propose a single fix. Write your finding to `output.json` in
this shape:

    { "incompatible": <true|false>,
      "resolution": "airflow_bump" | "provider_downgrade" | null,
      "notes": "<one-sentence rationale>" }

Set `resolution: null` when the combination already installs.
