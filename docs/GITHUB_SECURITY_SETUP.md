# GitHub Security Setup (Recommended)

This project is now connected to:
- `https://github.com/hasaena/shopee_ads`

Apply these GitHub-side settings in repository **Settings**.

## 1) Branch protection

Create rules for:
- `main`
- `release/phase1-lock`

Enable:
- Require pull request before merging
- Require at least 1 approval
- Dismiss stale approvals when new commits are pushed
- Require status checks to pass before merging (after CI is enabled)
- Restrict who can push directly

## 2) Repository security

Enable in **Security** tab:
- Secret scanning
- Push protection for secrets
- Dependabot alerts
- Dependabot security updates

## 3) Access control

- Keep repository private unless intentionally public
- Review collaborators and remove unused access
- Prefer least-privilege roles

## 4) Account-level safety

For GitHub account:
- Enable 2FA
- Review authorized OAuth apps/tokens periodically
- Revoke old/unused personal access tokens

## 5) Operational policy

- Never commit `.env`, token exports, DB, runtime artifacts
- Use `scripts/git_commit_safe.ps1` for local commits
- Keep Shopee auth/token changes and report/UI changes in separate commits
