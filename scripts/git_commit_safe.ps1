param(
    [Parameter(Mandatory = $true)]
    [string]$Message,
    [switch]$AddAll
)

$ErrorActionPreference = "Stop"

if (-not (Get-Command git -ErrorAction SilentlyContinue)) {
    throw "git command not found."
}
if (-not (Get-Command python -ErrorAction SilentlyContinue)) {
    throw "python command not found."
}

if ($AddAll) {
    git add -A
}

python scripts/git_secret_guard.py
if ($LASTEXITCODE -ne 0) {
    throw "Secret guard failed. Commit blocked."
}

python scripts/baseline_guard.py
if ($LASTEXITCODE -ne 0) {
    throw "Baseline guard failed. Split risky mixed commit."
}

# --no-verify avoids shell hook failures on some Windows Git environments.
git commit --no-verify -m $Message
