# SQE asset register

Internal. The assets the SQE team itself owns, with named owners. The public book (`architecture/system-context.md`) describes a deployment's parts by role; this page names people. Runtime assets of a customer deployment belong in that customer's CMDB, not here.

Classification comes from `.asset`: criticality **high**, data classification **internal**, supplier Schuberg Philis, owning team VPF Data & AI.

## Owners

| Role | Who |
|---|---|
| Accountable owner and contact (`.asset` `contact`) | jjverhoeks |
| GitLab Owners (can change settings, members, protected branches) | jjverhoeks, owit, rherrero, xdoerga |
| GitLab Maintainers (can merge to `main`, push `v*` tags) | sgaurav, oomorisiagbon, plus the group Renovate bot |

Membership is read from the project (`members/all`, access level >= 40) and reviewed with the periodic access review.

## Assets

| Asset | Where | Owner | Notes |
|---|---|---|---|
| Source repository (canonical, CI) | Internal GitLab project `sqlengine` | Accountable owner | Protected `main`, protected `v*` tags |
| Public mirror | `github.com/schubergphilis/sqe` | Accountable owner | Pushed by hand; private vulnerability reporting enabled |
| Container images | GitLab registry (`:sha`, `:vX.Y.Z`, `:stable`); release mirror in Harbor | Accountable owner | Scanned on main and weekly (`:stable`) |
| Helm chart | `deploy/helm/sqe` | Accountable owner | Version = SQE version; lint and hardening gate in CI |
| Documentation site | `docs/site` (published as the book, blog, ebook) | Accountable owner | `make leak-scan` in every pipeline |
| Marketing site | `getsqe/www` repository | Accountable owner | Separate repo; `security.txt` pending publish |
| CI/CD pipeline and schedules | `.gitlab-ci.yml`; schedules "Weekly security rescan + supply-chain watch" and "Monthly vulnerability review" | Accountable owner | Schedules are owned by the user that created them |
| CI and release credentials | See `ci-credentials.md` | Group maintainers | Rotation log there |

## Review

Review this register with the monthly vulnerability review: owners still correct, no asset missing, schedules still owned by an active member.
