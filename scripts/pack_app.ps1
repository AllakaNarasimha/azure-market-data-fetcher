Param(
    [switch]$DryRun,
    [string]$Destination = "app_clean.zip"
)

$root = Get-Location
$ignoreFile = Join-Path $root ".funcignore"
if (!(Test-Path $ignoreFile)) {
    Write-Error ".funcignore not found in repository root"
    exit 1
}

$patterns = Get-Content $ignoreFile | ForEach-Object { $_.Trim() } | Where-Object { $_ -and -not ($_.StartsWith('#')) }

# Gather all files recursively
$allFiles = Get-ChildItem -File -Recurse -Force

$include = @()
foreach ($f in $allFiles) {
    $rel = $f.FullName.Substring($root.Path.Length+1) -replace '\\','/'
    $skip = $false

    foreach ($p in $patterns) {
        $pp = $p.Trim()
        if ($pp -eq '') { continue }

        # Normalize trailing slashes for directory patterns
        if ($pp.EndsWith('/')) { $pp = $pp.TrimEnd('/') }

        # If pattern contains wildcard characters, use -like directly
        if ($pp -like '*[*?]*' -or $pp.StartsWith('*')) {
            if ($rel -like $pp) { $skip = $true; break }
        } else {
            # treat as prefix (directory) or exact file
            if ($rel -eq $pp -or $rel -like "$pp/*" -or $rel -like "*/$pp/*" -or $rel -like "*/*$pp") { $skip = $true; break }
        }
    }

    if (-not $skip) { $include += $f.FullName }
}

if ($DryRun) {
    Write-Host "Files that would be included ($($include.Count)):`n"
    $include | ForEach-Object { Write-Host $_ }
    exit 0
}

if (Test-Path $Destination) { Remove-Item $Destination -Force }

if ($include.Count -eq 0) {
    Write-Error "No files to include in archive. Check .funcignore patterns."
    exit 1
}

# Compress-Archive accepts an array of paths
Compress-Archive -LiteralPath $include -DestinationPath $Destination -Force
Write-Host "Created $Destination with $($include.Count) files."
