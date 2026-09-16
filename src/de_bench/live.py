"""One-line summaries of an agent's event stream, for watching a trial run.

Each harness emits its own JSONL shape on stdout — pi has `message_end` wrapping
a message with content blocks, claude-code emits `assistant`/`user` with the same
block vocabulary, codex emits `item.completed` carrying a typed item, opencode
emits parts. This turns any of them into a readable line, or None for events not
worth a line.

Nothing here is load-bearing: the raw stream is still what gets stored, and a
shape this does not recognise costs a log line, not a trial.
"""

from __future__ import annotations

import json

# pi emits one of these per token; claude-code repeats the whole message per
# block. Streaming them would bury the events worth reading.
_SKIP = {"message_update", "message_start", "session", "agent_start", "step_start", "item.started"}
_TURN = {"turn_start", "turn.started"}


def _clip(text: str, width: int = 160) -> str:
    flat = " ".join(str(text).split())
    return flat if len(flat) <= width else flat[: width - 1] + "…"


def _block(block: dict, role: str = "assistant") -> str | None:
    """One content block, in the vocabulary pi and claude-code share.

    Role matters: pi and otto deliver tool output as user-role text, so labelling
    every text block "say" makes a file listing look like the agent talking.
    """
    kind = block.get("type")
    if kind == "text":
        text = block.get("text") or ""
        if not text.strip():
            return None
        return f"{'say ' if role == 'assistant' else 'got '} {_clip(text)}"
    if kind == "toolResult":  # pi nests its result under the block
        return f"got   {_clip(block.get('text') or block.get('result') or '', 120)}"
    if kind == "thinking":
        thought = block.get("thinking") or ""
        return f"think {_clip(thought)}" if thought.strip() else None
    # `tool_use` is claude-code's spelling, `toolCall` pi's. pi settles its
    # arguments through the update events this skips, so its calls show a name
    # and nothing else — coarser, but it does not invent detail it never saw.
    if kind in ("tool_use", "toolCall"):
        args = block.get("input") or block.get("arguments") or {}
        name = block.get("name") or "?"
        if not args:
            return f"tool  {name}"
        for key in ("command", "file_path", "path", "pattern", "query", "dag_id"):
            if key in args:
                return f"tool  {name}: {_clip(args[key], 120)}"
        return f"tool  {name}: {_clip(json.dumps(args), 120)}"
    if kind == "tool_result":
        content = block.get("content")
        if isinstance(content, list):
            content = " ".join(str(c.get("text", "")) for c in content if isinstance(c, dict))
        return f"got   {_clip(content or '', 120)}"
    return None


def summarize(line: str) -> list[str]:
    """Zero or more display lines for one raw event."""
    line = line.strip()
    if not line:
        return []
    try:
        event = json.loads(line)
    except (ValueError, TypeError):
        return []
    if not isinstance(event, dict):
        return []

    kind = event.get("type") or event.get("role")
    if kind in _TURN:
        return ["—— turn ——"]
    if kind in _SKIP:
        return []

    # codex: a completed item carrying its own type.
    item = event.get("item")
    if isinstance(item, dict):
        itype = item.get("type")
        if itype == "agent_message":
            return [f"say  {_clip(item.get('text') or '')}"]
        if itype == "command_execution":
            return [f"tool  bash: {_clip(item.get('command') or '', 120)}"]
        if itype in ("file_change", "patch_apply"):
            return [f"edit  {_clip(json.dumps(item.get('changes') or item.get('files') or ''), 120)}"]
        if itype == "reasoning":
            return [f"think {_clip(item.get('text') or '')}"]
        return [f"{itype}"] if itype else []

    # pi and claude-code: a message whose content is a list of blocks.
    message = event.get("message")
    if isinstance(message, dict):
        role = message.get("role") or kind or "assistant"
        content = message.get("content")
        if isinstance(content, str):
            label = "say " if role == "assistant" else "got "
            return [f"{label} {_clip(content)}"] if content.strip() else []
        if isinstance(content, list):
            return [out for b in content if isinstance(b, dict) and (out := _block(b, role))]
        return []

    # opencode: a part with its own type.
    part = event.get("part")
    if isinstance(part, dict):
        ptype = part.get("type")
        if ptype == "text" and (part.get("text") or "").strip():
            return [f"say  {_clip(part['text'])}"]
        if ptype == "tool":
            return [f"tool  {_clip(part.get('tool') or part.get('name') or '')}"]
        return []

    if kind == "result":
        return [f"result {_clip(event.get('result') or event.get('subtype') or '')}"]
    return []
