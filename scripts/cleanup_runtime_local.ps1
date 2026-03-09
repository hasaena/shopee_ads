param(
    [switch]$Apply
)

$targets = @(
    ".pytest_cache",
    "artifacts",
    "reports"
)

$singleFiles = @(
    "dotori.db",
    "shopee_tokens_export.json"
)

Write-Host "cleanup_runtime_local_start=1 apply=$($Apply.IsPresent)"

foreach ($t in $targets) {
    if (Test-Path $t) {
        Write-Host "target_dir=$t exists=1"
        if ($Apply) {
            Remove-Item -Recurse -Force $t
            Write-Host "removed_dir=$t"
        }
    }
}

foreach ($f in $singleFiles) {
    if (Test-Path $f) {
        Write-Host "target_file=$f exists=1"
        if ($Apply) {
            Remove-Item -Force $f
            Write-Host "removed_file=$f"
        }
    }
}

Write-Host "cleanup_runtime_local_done=1"
if (-not $Apply) {
    Write-Host "dry_run=1 hint='Use -Apply to delete listed runtime files.'"
}
