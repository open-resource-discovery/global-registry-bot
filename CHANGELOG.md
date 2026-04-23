# CHANGELOG

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).
This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html) rules.

## [unreleased]

- Improved namespace approval flow:
  - subcontext requests approved by parent owners now go directly to PR
  - system namespace requests require an extra contact-owner gate if requester is not in contacts
  - parent chain validation now checks the full chain down to root

- Improved issue form parsing:
  - extra UI-only fields are allowed in issue templates
  - non-schema fields are filtered before validation and YAML generation
  - fields like `justification` stay visible in GitHub issues without affecting registry data

## [[0.1.1](https://github.com/open-resource-discovery/global-registry-bot/releases/tag/v0.1.1)] - 2026-04-21

## Summary

- improve auto-update and merge flow for approved registry PRs
- keep approved PRs in sync with default branch
- make CI-triggered auto-merge more reliable

## Changes

- auto-update approved PR branches when default branch changes
- use mergeability state (`behind`) instead of requiring pre-update green checks
- add delayed retry for branch updates after push events
- improve merge flow with mergeability polling
- trigger auto-merge on successful `check_suite` and `check_run` events
- prevent duplicate approval handling and respect existing approvals

## Result

- approved PRs no longer get stuck outdated
- smoother CI-driven merge behavior
- more stable and predictable bot automation

## [[0.1.0](https://github.com/open-resource-discovery/global-registry-bot/releases/tag/v/0.1.0)] - 2026-04-14

## Added

- Direct PR auto-approval via `onApproval` hook
- Use last commit author as `requestAuthorId`
- Merge `approvers` and `approversPool` for approval logic
- Auto-add `Approved` label on successful auto-approval
- Multi-file validation support with aggregated PR comment
- Machine-readable validation output

## Improved

- Consistent validation feedback between CI and bot comments
- Safer approval logic

## [[0.0.5](https://github.com/open-resource-discovery/global-registry-bot/releases/tag/rel/0.0.5)] - 2026-04-10

## Added

- onApproval hook support for request and direct PR flows
- automatic PR approval for direct PRs when onApproval returns approved
- machine-readable metadata in PR validation comments

## Changed

- unified error structure (`field` + `message`) across hooks and validation
- approval flow now prioritizes onApproval over default reviewer assignment
- improved consistency between issue and PR validation outputs

## Behavior

- approved → auto-approve + merge flow continues
- rejected → PR/issue closed with structured feedback
- no decision → fallback to existing manual review flow

## Notes

- fully backward compatible if onApproval is not configured
- no changes to existing validation, routing, or CI logic

## [[0.0.4](https://github.com/open-resource-discovery/global-registry-bot/releases/tag/rel/0.0.4)] - 2026-03-31

- accept approval only for explicit approval commands
- support configured approval keyword only as an exact command
- stop matching approval keywords inside normal review sentences
- require the issue to already be in real review state
- do not auto-repair missing review labels during approval
- revalidate the request before creating a PR
- run parent-chain checks again before PR creation

## [[0.0.3](https://github.com/open-resource-discovery/global-registry-bot/releases/tag/rel/0.0.3)] - 2026-03-11

## Added

- Safe validation support for fork PRs.
- CI detected-issue comments for fork PRs.
- Parent-owner approval for sub-namespace requests.

## Changed

- Aligned CI validation with issue validation.
- Extended filename vs identifier validation to all `data/*` resources.
- Improved bot comment deduplication and collapsing.

## Fixed

- Missing validation comments on fork PRs.
- False positives from inconsistent validation input.
- Repeated routing-label lock comments.

## [[0.0.2](https://github.com/open-resource-discovery/global-registry-bot/releases/tag/rel/0.0.2)] - 2026-03-10

- Fixed inconsistency between issue validation and CI validation so both now use the same normalized hook input.
- Fixed false-positive CI validation errors for custom hook checks such as Product ID comparison.
- Updated the CI hook validation flow to match the same behavior as `validateRequestIssue`.
- Added and extended unit tests for `validation/run.ts` to keep bot and CI validation aligned.

## [0.0.1]

- Open-source readiness pushed forward (OSS compliance files + repo cleanup/standards).
- Approval/review flow tightened: reviewers come from config, and human vs bot PR behavior is enforced consistently.
- Validation logic expanded: stronger namespace/parent-chain checks and additional governance gates for sub-namespace requests.
- CI feedback improved: full validation runs in CI and validation problems are surfaced directly on the PR.
- Issue workflow hardened: label/state guardrails to prevent manual “drift” and reduce noisy comment behavior.
- Parent namespace owner approval introduced for sub-namespace requests before forwarding to CPA review.
