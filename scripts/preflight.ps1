[CmdletBinding()]
param(
    [switch]$Online,
    [switch]$InspectImages
)

# Read-only diagnostics. Never read .env or dump Docker credentials/config.
$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot

function Invoke-ReadOnlyNative {
    param([string]$File, [string[]]$Arguments)
    $savedPreference = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try {
        $nativeOutput = & $File @Arguments 2>&1 | Out-String
        $nativeExit = $LASTEXITCODE
        return [ordered]@{
            exit_code = $nativeExit
            output = $nativeOutput.Replace([string][char]0, '').Trim()
        }
    }
    catch {
        return [ordered]@{ exit_code = -1; output = $_.Exception.Message }
    }
    finally { $ErrorActionPreference = $savedPreference }
}

$report = [ordered]@{
    checked_at_utc = [DateTime]::UtcNow.ToString('o')
    scope = 'read-only preparation; no container workload executed'
    powershell = $PSVersionTable.PSVersion.ToString()
    process_architecture = $env:PROCESSOR_ARCHITECTURE
    host = $null
    disks = @()
    docker = $null
    warnings = @()
}

try {
    $osInfo = Get-CimInstance Win32_OperatingSystem
    $sysInfo = Get-CimInstance Win32_ComputerSystem
    $cpuInfo = Get-CimInstance Win32_Processor
    $report.host = [ordered]@{
        os = $osInfo.Caption
        os_version = $osInfo.Version
        architecture = $osInfo.OSArchitecture
        cpu = $cpuInfo.Name
        logical_cpus = $sysInfo.NumberOfLogicalProcessors
        ram_gib = [math]::Round($sysInfo.TotalPhysicalMemory / 1GB, 2)
        free_ram_gib = [math]::Round($osInfo.FreePhysicalMemory / 1MB, 2)
    }
    $report.disks = @(Get-CimInstance Win32_LogicalDisk -Filter 'DriveType=3' | ForEach-Object {
        [ordered]@{
            drive = $_.DeviceID
            free_gib = [math]::Round($_.FreeSpace / 1GB, 2)
            size_gib = [math]::Round($_.Size / 1GB, 2)
        }
    })
}
catch { $report.warnings += 'CIM unavailable: ' + $_.Exception.Message }

$report.docker_version = Invoke-ReadOnlyNative -File docker -Arguments @('version')
$report.compose_version = Invoke-ReadOnlyNative -File docker -Arguments @('compose', 'version')
$report.docker_context = Invoke-ReadOnlyNative -File docker -Arguments @('context', 'show')
# Query selected fields only; do not serialize all docker info (may contain proxy credentials).
$dockerResult = Invoke-ReadOnlyNative -File docker -Arguments @('info', '--format', '{{.OSType}}|{{.Architecture}}|{{.NCPU}}|{{.MemTotal}}|{{.ServerVersion}}')
if ($dockerResult.exit_code -eq 0) {
    try {
        $values = ($dockerResult.output -split "`r?`n" | Select-Object -Last 1) -split '\|'
        $report.docker = [ordered]@{
            os = $values[0]
            architecture = $values[1]
            cpus = [int]$values[2]
            ram_gib = [math]::Round([double]$values[3] / 1GB, 2)
            server_version = $values[4]
        }
        if ($report.docker.ram_gib -lt 7.5 -or $report.docker.cpus -lt 4) {
            $report.warnings += 'Below planned Docker budget: configure approximately 8 GiB and 4 CPUs before day 1.'
        }
        if ($report.docker.os -ne 'linux') { $report.warnings += 'Switch Docker Desktop to Linux containers.' }
    }
    catch { $report.warnings += 'Unable to parse Docker resource output.' }
}
else {
    $report.warnings += 'Docker access failed; a sandbox permission error does not prove the daemon is stopped.'
    $report.docker_error = $dockerResult
}
$report.wsl_status = Invoke-ReadOnlyNative -File wsl -Arguments @('--status')
$report.wsl_distros = Invoke-ReadOnlyNative -File wsl -Arguments @('--list', '--verbose')
$report.docker_disk_usage = Invoke-ReadOnlyNative -File docker -Arguments @('system', 'df')

try {
    $reservedPorts = @(8080, 8081, 8082, 3000)
    $report.listen_ports = @(Get-NetTCPConnection -State Listen -ErrorAction Stop |
        Where-Object { $_.LocalPort -in $reservedPorts } |
        Select-Object LocalAddress, LocalPort -Unique)
}
catch { $report.warnings += 'Port inspection unavailable: ' + $_.Exception.Message }

if ($Online) {
    $report.network = [ordered]@{}
    foreach ($endpoint in @('https://registry-1.docker.io/v2/', 'https://pypi.org/simple/')) {
        $report.network[$endpoint] = Invoke-ReadOnlyNative -File curl.exe -Arguments @(
            '--silent', '--show-error', '--head', '--max-time', '20', $endpoint
        )
    }
}
if ($InspectImages) {
    $stack = Get-Content -LiteralPath (Join-Path $projectRoot 'config/stack.json') -Raw -Encoding UTF8 | ConvertFrom-Json
    $report.image_checks = [ordered]@{}
    foreach ($entry in $stack.images.PSObject.Properties) {
        $imageRef = $entry.Value.tag + '@' + $entry.Value.digest
        $report.image_checks[$entry.Name] = Invoke-ReadOnlyNative -File docker -Arguments @(
            'buildx', 'imagetools', 'inspect', $imageRef
        )
    }
}
$report | ConvertTo-Json -Depth 10
