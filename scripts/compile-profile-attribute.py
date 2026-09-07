#!/usr/bin/env python3
"""Attribute rustc's trait-solving time to the functions that cause it.

Input: the chrome_profiler.json that measureme's `crox` writes from a
`-Zself-profile` run made with `-Zself-profile-events=default,args` (see
scripts/compile-profile.sh). Every `evaluate_obligation` event is charged to
the innermost enclosing per-item query (typeck_root, mir_borrowck,
check_coroutine_obligations, optimized_mir, items_of_instance, ...), whose
`arg0` names the function. Output: total, split by root query, top functions,
and a per-module roll-up.

Why this view: `summarize` alone says "evaluate_obligation: 58 s" and nothing
else. The functions it lands on are the ones whose futures are large and get
awaited inside `Send` futures (`async_trait` methods, `tokio::spawn`), and
each of those is a place where one `Box::pin` moves the proof to the
definition and out of every caller.
"""
import collections
import json
import sys

ROOTS = {
    "typeck_root",
    "mir_borrowck",
    "check_coroutine_obligations",
    "optimized_mir",
    "items_of_instance",
    "check_well_formed",
    "mir_built",
    "coherence_checking",
    "generate_crate_metadata",
}


def main(path: str, top: int = 30) -> None:
    with open(path) as fh:
        events = json.load(fh)
    if isinstance(events, dict):
        events = events["traceEvents"]
    events = [e for e in events if e.get("ph") == "X"]

    by_tid = collections.defaultdict(list)
    for e in events:
        by_tid[e["tid"]].append(e)

    per_fn = collections.Counter()
    per_root = collections.Counter()
    for lst in by_tid.values():
        lst.sort(key=lambda e: (e["ts"], -e["dur"]))
        stack = []
        for e in lst:
            while stack and e["ts"] >= stack[-1]["ts"] + stack[-1]["dur"]:
                stack.pop()
            if e["name"] == "evaluate_obligation" and not any(
                s["name"] == "evaluate_obligation" for s in stack
            ):
                root = None
                for s in reversed(stack):
                    if s["name"] in ROOTS:
                        root = s
                        break
                secs = e["dur"] / 1e6
                if root is None:
                    per_fn["<no root>"] += secs
                    per_root["<no root>"] += secs
                else:
                    name = ((root.get("args") or {}).get("arg0") or "").strip()
                    if name.startswith("(Instance {"):
                        # items_of_instance keys: pull the path out of the DefId text.
                        start = name.find("~ ")
                        end = name.find(")", start)
                        name = name[start + 2 : end] if start >= 0 else name
                    per_fn[name[:110]] += secs
                    per_root[root["name"]] += secs
            stack.append(e)

    total = sum(per_fn.values())
    print(f"evaluate_obligation total: {total:.1f}s")
    print("by root query: " + ", ".join(f"{k} {v:.1f}s" for k, v in per_root.most_common()))
    print(f"\ntop {top} functions:")
    for name, secs in per_fn.most_common(top):
        print(f"{secs:6.1f}s  {name}")

    per_module = collections.Counter()
    for name, secs in per_fn.items():
        parts = name.split("::")
        per_module[parts[1] if len(parts) > 1 else name] += secs
    print("\nby module:")
    for mod, secs in per_module.most_common(15):
        if secs >= 0.1:
            print(f"{secs:6.1f}s  {mod}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit("usage: compile-profile-attribute.py chrome_profiler.json [top]")
    main(sys.argv[1], int(sys.argv[2]) if len(sys.argv) > 2 else 30)
