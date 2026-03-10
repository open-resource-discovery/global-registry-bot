# CHANGELOG

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).
This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html) rules.

## [unreleased]

- Open-source readiness pushed forward (OSS compliance files + repo cleanup/standards).
- Approval/review flow tightened: reviewers come from config, and human vs bot PR behavior is enforced consistently.
- Validation logic expanded: stronger namespace/parent-chain checks and additional governance gates for sub-namespace requests.
- CI feedback improved: full validation runs in CI and validation problems are surfaced directly on the PR.
- Issue workflow hardened: label/state guardrails to prevent manual “drift” and reduce noisy comment behavior.
- Parent namespace owner approval introduced for sub-namespace requests before forwarding to CPA review.
