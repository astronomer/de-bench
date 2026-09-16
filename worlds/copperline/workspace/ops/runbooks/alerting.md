**How a DAG notifies on failure.** Review date 2025-06-30. Owner: platform.

Every scheduled DAG tells somebody when it fails. This is how to wire that up.

## The parameters

Set them per task, or in `default_args` for the whole DAG:

```python
default_args = {
    "email": ["data-platform@copperline.example"],
    "email_on_failure": True,
    "email_on_retry": False,
}
```

`email` takes a list. `email_on_failure` sends on the final failure, after retries are exhausted. `email_on_retry` sends on every retry and is off for everything except two intake DAGs where a retry means the source is down.

## Who to address it to

| Team | Address |
|---|---|
| data platform | `data-platform@copperline.example` |
| finance analytics | `finance-eng@copperline.example` |
| commerce | `commerce-data@copperline.example` |
| supply chain | `supply-data@copperline.example` |
| growth | `growth-data@copperline.example` |
| customer | `customer-data@copperline.example` |

Address it to the team that owns the DAG, not to the platform team. The platform team is not on call for another team's pipeline.

## Paging

Email is not paging. A DAG whose failure needs somebody out of bed is wired to the pager separately, through the alerting config, and that is a different subject from this file — see `contracts/alerting.md`.

Only four DAGs page. Everything else emails and is picked up in the morning.

## SLA misses

An SLA miss is not a failure. The DAG succeeded, late. There is a separate notification for it and most teams have it switched off, because the marts that matter are watched by the freshness alerts instead.

## Testing it

Trigger the DAG with a task that raises, and check the mail arrived. Do this in a personal namespace, not in the shared deployment.

## Notes

- The mail is sent by the deployment's own SMTP configuration, which is managed by the platform team.
- Failures in a mapped task send once per failed map index, which is loud on the DAGs that map over locations.
- Nothing here covers on-call rotations. Those are in the pager tool.

## Follow-ups

- TODO: the addresses above are distribution lists and two of them have not been checked since they were created.
- Somebody should write down what the four paging DAGs are. It is currently folklore.
