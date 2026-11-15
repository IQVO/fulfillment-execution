#!/usr/bin/env python3
"""Breaking-change gate for apis/asyncapi.yaml (producer-parity wave).

Compares a base and a revision AsyncAPI 2.x document and fails (exit 1) on
exactly the payload changes that break consumers pinned to this spec:

  1. a message removed from components.messages (a rename is remove+add);
  2. a required data field of a surviving message removed (rename ditto);
  3. a data field narrowed: `null` dropped from its type union, `number`
     narrowed to `integer`, a `type` list losing a member, an `enum` added
     or shrunk, or a min/max/minLength/maxLength/minItems bound added or
     tightened.

Additive changes (a new message, a new optional field, a widened type) pass:
the fleet CloudEvents rule ships those inside v1, and a breaking payload
change as a new `.v2` type with a new dataschema URN (apis/asyncapi.yaml
header, ADR-0032) — which this gate sees as a message added, not changed.

Usage: asyncapi_breaking.py BASE REVISION   exit 0 = no breaking change,
                                           1 = breaking changes (listed),
                                           2 = unreadable input.
       asyncapi_breaking.py --self-test    embedded fixtures, exit 0 = pass.
Dependencies: PyYAML for files (pinned in .github/requirements/chart-tests.txt);
the self-test builds its fixtures as dicts, so it runs on a bare python3.
"""
import sys

BOUNDS = ("minimum", "maximum", "minLength", "maxLength", "minItems")


def resolve(node, doc):
    """Follow one $ref level into components (./components/schemas/Name)."""
    ref = node.get("$ref") if isinstance(node, dict) else None
    if not ref:
        return node
    cur = doc
    for part in ref.lstrip("#/").split("/"):
        cur = cur.get(part) if isinstance(cur, dict) else None
        if cur is None:
            return {}
    return cur


def data_schema(message, doc):
    """The message-specific `data` payload schema of a message.

    A message payload is allOf [generic CloudEvent, message block]; the
    generic CloudEvent component also has a bare `data` property, so the
    LAST candidate that actually shapes data (required or properties) wins.
    """
    payload = resolve(message.get("payload", {}), doc)
    candidates = []
    if isinstance(payload, dict):
        candidates.append(payload.get("properties", {}).get("data"))
        for part in payload.get("allOf", []):
            candidates.append(resolve(part, doc).get("properties", {}).get("data"))
    best = {}
    for cand in candidates:
        cand = resolve(cand, doc) if cand is not None else {}
        if cand.get("required") or cand.get("properties"):
            best = cand
    return best


def merged(schema, doc):
    """required + properties of a schema, merging its local allOf parts."""
    req, props = set(schema.get("required", [])), dict(schema.get("properties", {}))
    for part in schema.get("allOf", []) if isinstance(schema, dict) else []:
        part = resolve(part, doc)
        req |= set(part.get("required", []))
        props.update(part.get("properties", {}))
    return req, props


def types(schema):
    """Value types a schema accepts, with `number` covering `integer`.

    Used as covers-sets: narrowing is when the revision's accepted values no
    longer include everything the base accepted (number -> integer, or `null`
    dropped from a union), so base types must each stay accepted.
    """
    t = schema.get("type")
    declared = set(t) if isinstance(t, list) else {t} if t else set()
    covers = set(declared)
    if "number" in declared:
        covers.add("integer")  # a number accepts every integer
    return covers


def narrowing(base, rev):
    """Why `rev` rejects values `base` accepted, or None if it does not."""
    tb, tr = types(base), types(rev)
    if tb and tr and not tr >= tb:
        return f"type {sorted(tb - tr)} no longer accepted"
    eb, er = base.get("enum"), rev.get("enum")
    if er is not None and (eb is None or not set(er) >= set(eb)):
        lo = f"enum {sorted(eb)} -> {sorted(er)}" if eb is not None else "enum added"
        return lo
    for b in BOUNDS:
        bv, rv = base.get(b), rev.get(b)
        tightening = rv is not None and (bv is None or rv > bv) if b in ("minimum", "minLength", "minItems") \
            else rv is not None and (bv is None or rv < bv)
        if tightening:
            return f"{b} {bv} -> {rv}"
    return None


def breaking(base_doc, rev_doc):
    """List of human-readable breaking changes between two AsyncAPI docs."""
    out = []
    base_msgs = base_doc.get("components", {}).get("messages", {})
    rev_msgs = rev_doc.get("components", {}).get("messages", {})
    for name in base_msgs:
        if name not in rev_msgs:
            out.append(f"message removed: {name}")
            continue
        breq, bprops = merged(data_schema(base_msgs[name], base_doc), base_doc)
        rreq, rprops = merged(data_schema(rev_msgs[name], rev_doc), rev_doc)
        for field in breq:
            if field not in rprops:
                out.append(f"{name}: required data field removed: {field}")
        for field, bs in bprops.items():
            why = narrowing(bs, rprops.get(field, {}))
            if why:
                out.append(f"{name}: data field `{field}` narrowed ({why})")
    return out


def self_test():
    def msg(req=(), props=None):
        props = props or {}
        return {"payload": {"properties": {"data": {"required": list(req), "properties": props}}}}

    base = {"components": {"messages": {
        "A": msg(["order_ref"], {"order_ref": {"type": ["string", "null"]}, "cpt": {"type": "string", "enum": ["a", "b"]}, "qty": {"type": "number"}, "note": {"type": "string"}}),
        "B": msg(["x"], {"x": {"type": "string"}})}}}
    ok = {"components": {"messages": {  # additive only: passes
        "A": msg(["order_ref"], {"order_ref": {"type": ["string", "null"]}, "cpt": {"type": "string", "enum": ["a", "b", "c"]}, "qty": {"type": "number"}, "note": {"type": "string"}, "extra": {"type": "string"}}),
        "B": msg(["x"], {"x": {"type": "string"}}),
        "C": msg()}}}
    bad = {"components": {"messages": {
        "A": msg(["order_ref"], {"cpt": {"type": "string", "enum": ["a"]}, "qty": {"type": "integer"}, "note": {"type": "string", "minLength": 3}, "order_renamed": {"type": "string"}})}}}
    assert breaking(base, ok) == [], breaking(base, ok)
    got = sorted(breaking(base, bad))
    want = sorted([
        "message removed: B",
        "A: required data field removed: order_ref",
        "A: data field `cpt` narrowed (enum ['a', 'b'] -> ['a'])",
        "A: data field `note` narrowed (minLength None -> 3)",
    ])
    assert "qty" in " ".join(got), got  # number -> integer is caught too
    for w in want:
        assert w in got, (w, got)
    print("asyncapi_breaking self-test: PASS")


def main(argv):
    if argv == ["--self-test"]:
        self_test()
        return 0
    if len(argv) != 2:
        print(__doc__)
        return 2
    try:
        import yaml  # deferred: --self-test and the dict API need no PyYAML
    except ImportError:
        print("asyncapi_breaking: file mode needs PyYAML (pinned in .github/requirements/chart-tests.txt).")
        return 2
    try:
        base_doc = yaml.safe_load(open(argv[0]))
        rev_doc = yaml.safe_load(open(argv[1]))
    except OSError as e:
        print(f"asyncapi_breaking: cannot read input: {e}")
        return 2
    except yaml.YAMLError as e:
        print(f"asyncapi_breaking: cannot parse input: {e}")
        return 2
    changes = breaking(base_doc, rev_doc)
    for c in changes:
        print(f"asyncapi_breaking: BREAKING: {c}")
    if changes:
        print(f"asyncapi_breaking: {len(changes)} breaking change(s) vs {argv[0]}.")
        return 1
    print("asyncapi_breaking: no breaking changes.")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
