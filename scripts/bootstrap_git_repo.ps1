param(
  [string]$DefaultBranch = "main"
)

$ErrorActionPreference = "Stop"
$repoPath = (Resolve-Path ".").Path
$git = @("git", "-c", "safe.directory=$repoPath")

if (-not (Test-Path ".git")) {
  & $git[0] $git[1] $git[2] init
}

& $git[0] $git[1] $git[2] config core.hooksPath .githooks

$current = & $git[0] $git[1] $git[2] branch --show-current 2>$null
if (-not $current) {
  & $git[0] $git[1] $git[2] checkout -b $DefaultBranch
}

Write-Host "Git repository bootstrap complete."
Write-Host "hooksPath=.githooks configured."
