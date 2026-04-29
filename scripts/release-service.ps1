param(
  [string]$Version = ''
)

$root = 'C:\Users\AdminWC\Dift App project\Project-Production-Ready'
$releaseScript = Join-Path $root 'scripts\release-microservices.ps1'

$params = @{
  GroupDirs = @('dift_backend_insentive')
  OnlyServices = @('promotion-service')
  IncludeExisting = $true
}
if ($Version -and $Version.Trim().Length -gt 0) {
  $params.ForceVersion = $Version.Trim()
}

& powershell -ExecutionPolicy Bypass -File $releaseScript @params
