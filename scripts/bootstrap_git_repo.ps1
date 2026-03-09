param(
  [string]$DefaultBranch = "main",
  [switch]$EnableHooks
)

$ErrorActionPreference = "Stop"
$repoPath = (Resolve-Path ".").Path
$git = @("git", "-c", "safe.directory=$repoPath")

if (-not (Test-Path ".git")) {
  & $git[0] $git[1] $git[2] init
}

if ($EnableHooks) {
  & $git[0] $git[1] $git[2] config core.hooksPath .githooks
} else {
  & $git[0] $git[1] $git[2] config --unset core.hooksPath 2>$null
}

$current = & $git[0] $git[1] $git[2] branch --show-current 2>$null
if (-not $current) {
  & $git[0] $git[1] $git[2] checkout -b $DefaultBranch
}

Write-Host "Git repository bootstrap complete."
if ($EnableHooks) {
  Write-Host "hooksPath=.githooks configured."
} else {
  Write-Host "hooksPath disabled (use scripts/git_commit_safe.ps1 on Windows)."
}
