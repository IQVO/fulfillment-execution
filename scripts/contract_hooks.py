"""Schemathesis hooks for scripts/contract-test.sh.

Every POST of this service sits behind the Idempotency-Key middleware
(ADR-0028), which is enforced in every deployment now that Postgres is the
only runtime. The OpenAPI document gives the parameter no per-request value,
so Schemathesis would send the SAME key with DIFFERENT bodies and rightly
get 409 `idempotency-key-reused`. A real client sends a fresh UUID per
logical submit; this hook does the same for every POSITIVE case that carries
a header. A request that deliberately omits the header (negative testing of
the required parameter) is left alone and must still be answered 400.

A deliberately INVALID key (empty, over 128 characters, non-printable or
non-ASCII — negative testing of the parameter's schema) is left alone too:
laundering it into a valid UUID would make the API accept a schema-violating
request, and the fuzzer would (and did) report that as a contract failure.
The validity rule mirrors the middleware's: 1 to 128 printable ASCII
characters.

One negative class is untestable over real HTTP: a key that differs from a
valid key only by LEADING or TRAILING spaces/tabs. RFC 7230 optional
whitespace around a header value is stripped by every compliant server (Go's
net/http included) before the application sees it, so the middleware
observes — and rightly accepts — the trimmed value. A negative case whose
only violation is that edge whitespace therefore gets an observably invalid
substitute key, keeping the middleware's rejection path under test instead
of reporting a false "API accepted schema-violating request".
"""

import uuid

import schemathesis

_OWS = " \t"


def _valid_key(key: str) -> bool:
    return 0 < len(key) <= 128 and all(0x20 <= ord(c) <= 0x7E for c in key)


def _is_positive(case) -> bool:
    return case.meta is not None and case.meta.generation.mode.value == "positive"


@schemathesis.hook
def before_call(ctx, case, **kwargs):
    if not case.headers or "Idempotency-Key" not in case.headers:
        return None
    key = case.headers["Idempotency-Key"]
    if _is_positive(case) or _valid_key(key):
        case.headers["Idempotency-Key"] = str(uuid.uuid4())
    elif key != key.strip(_OWS) and _valid_key(key.strip(_OWS)):
        # Edge-whitespace violation: invisible to any HTTP server (see the
        # module docstring). Substitute a violation the server CAN observe:
        # non-ASCII passes the transport's header validation but fails the
        # middleware's printable-ASCII rule (a control byte like \x7f would
        # be rejected by the transport itself with a bare text/plain 400).
        case.headers["Idempotency-Key"] = "é"
    return None
