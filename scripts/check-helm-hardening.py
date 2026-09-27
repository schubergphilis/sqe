#!/usr/bin/env python3
"""Fail when the rendered Helm chart drops a pod hardening control.

Reads `helm template` output on stdin and checks every pod template against
the Pod Security "restricted" profile fields this chart relies on, plus the
NetworkPolicies the production overlay turns on:

  pod:        runAsNonRoot, seccompProfile RuntimeDefault, no host
              namespaces, no hostPath volumes
  container:  allowPrivilegeEscalation false, readOnlyRootFilesystem,
              capabilities drop ALL, not privileged
  policy:     a NetworkPolicy selects every component that has a Deployment

Usage: helm template ... | python3 scripts/check-helm-hardening.py
"""
import sys

import yaml

POD_KINDS = {"Deployment", "StatefulSet", "DaemonSet", "Job"}


def check(docs):
    errors, components, covered = [], set(), set()
    for doc in docs:
        if not doc:
            continue
        kind, name = doc.get("kind"), doc.get("metadata", {}).get("name")
        if kind == "NetworkPolicy":
            labels = doc["spec"]["podSelector"].get("matchLabels", {})
            covered.add(labels.get("app.kubernetes.io/component"))
        if kind not in POD_KINDS:
            continue
        spec = doc["spec"]["template"]["spec"]
        components.add(doc["spec"]["template"]["metadata"]["labels"].get("app.kubernetes.io/component"))
        where = f"{kind}/{name}"
        psc = spec.get("securityContext", {})
        if psc.get("runAsNonRoot") is not True:
            errors.append(f"{where}: runAsNonRoot is not true")
        if psc.get("seccompProfile", {}).get("type") != "RuntimeDefault":
            errors.append(f"{where}: seccompProfile is not RuntimeDefault")
        for host in ("hostNetwork", "hostPID", "hostIPC"):
            if spec.get(host):
                errors.append(f"{where}: {host} is set")
        for vol in spec.get("volumes", []) or []:
            if "hostPath" in vol:
                errors.append(f"{where}: hostPath volume {vol.get('name')}")
        for c in (spec.get("initContainers") or []) + spec.get("containers", []):
            csc, cw = c.get("securityContext", {}), f"{where}/{c['name']}"
            if csc.get("allowPrivilegeEscalation") is not False:
                errors.append(f"{cw}: allowPrivilegeEscalation is not false")
            if csc.get("readOnlyRootFilesystem") is not True:
                errors.append(f"{cw}: readOnlyRootFilesystem is not true")
            if "ALL" not in (csc.get("capabilities", {}).get("drop") or []):
                errors.append(f"{cw}: capabilities do not drop ALL")
            if csc.get("privileged"):
                errors.append(f"{cw}: privileged")
    for comp in sorted(c for c in components - covered if c):
        errors.append(f"component {comp}: no NetworkPolicy selects it")
    return errors, len(components)


def main():
    errors, pods = check(list(yaml.safe_load_all(sys.stdin)))
    for e in errors:
        print(e)
    if pods == 0:
        print("no pod templates in the input; did helm template fail?")
        return 1
    print(f"==> {pods} pod templates checked, {len(errors)} problems")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
