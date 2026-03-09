$repo = "D:/python/myproject/dotori_shopee_automation"

Write-Host "repo=$repo"
Write-Host "remote:"
git -C $repo remote -v

Write-Host ""
Write-Host "identity:"
git -C $repo config --get user.name
git -C $repo config --get user.email

Write-Host ""
Write-Host "safety_config:"
$keys = @(
    "pull.ff",
    "fetch.prune",
    "fetch.fsckobjects",
    "transfer.fsckobjects",
    "push.default"
)
foreach ($k in $keys) {
    $v = git -C $repo config --get $k
    Write-Host "$k=$v"
}

Write-Host ""
Write-Host "branch_status:"
git -C $repo branch -vv
