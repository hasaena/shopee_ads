param(
    [Parameter(Mandatory = $true)]
    [string]$RemoteUrl,
    [string]$Branch = "main"
)

$ErrorActionPreference = "Stop"

if (-not (Get-Command git -ErrorAction SilentlyContinue)) {
    throw "git command not found."
}

$repoRoot = (Get-Location).Path

if (-not (Test-Path (Join-Path $repoRoot ".git"))) {
    throw "This directory is not a git repository: $repoRoot"
}

$existing = git remote get-url origin 2>$null
if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($existing)) {
    git remote add origin $RemoteUrl
} else {
    git remote set-url origin $RemoteUrl
}

git branch -M $Branch
git push -u origin $Branch

Write-Host "Remote connected and pushed: origin/$Branch"
