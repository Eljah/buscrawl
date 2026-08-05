#!/usr/bin/env pwsh
# Проверка доступности удаленного сервера 192.168.0.120
# Логин: eljah
# Пароль: Tatarstan1920

$server = "192.168.0.120"
$username = "eljah"
$password = "Tatarstan1920"

Write-Host "=========================================="
Write-Host "Проверка доступности сервера $server"
Write-Host "=========================================="
Write-Host ""

# Функция для проверки доступности порта
function Test-Port {
    param(
        [string]$ComputerName,
        [int]$Port,
        [int]$Timeout = 3000
    )
    
    try {
        $tcpClient = New-Object System.Net.Sockets.TcpClient($ComputerName, $Port)
        $tcpClient.Close()
        return $true
    } catch {
        return $false
    }
}

# Шаг 1: Проверка доступности порта 22
Write-Host "[1/2] Проверка доступности порта 22 (SSH)..."

$portAvailable = Test-Port -ComputerName $server -Port 22 -Timeout 5000

if ($portAvailable) {
    Write-Host "  OK: Port 22 available (SSH server is running)"
} else {
    Write-Host "  FAIL: Cannot connect to port 22"
    Write-Host ""
    Write-Host "Possible reasons:"
    Write-Host "  - Server $server is not reachable over network"
    Write-Host "  - SSH server is not running on the server"
    Write-Host "  - Firewall blocks port 22"
    Write-Host "  - SSH server is configured on a different port"
    exit 1
}

# Шаг 2: Проверка аутентификации
Write-Host ""
Write-Host "[2/2] Checking SSH authentication..."

# Проверяем наличие различных SSH-клиентов
$sshClient = $null
$sshClientName = ""

# 1. Проверяем стандартный SSH (OpenSSH)
if (Get-Command ssh -ErrorAction SilentlyContinue) {
    $sshClient = "ssh"
    $sshClientName = "OpenSSH"
}
# 2. Проверяем plink (PuTTY)
elseif (Get-Command plink -ErrorAction SilentlyContinue) {
    $sshClient = "plink"
    $sshClientName = "PuTTY plink"
}

if ($null -eq $sshClient) {
    Write-Host "  WARN: SSH client not found"
    Write-Host ""
    Write-Host "Install one of the following SSH clients:"
    Write-Host "  1. OpenSSH Client (built-in in Windows 10/11)"
    Write-Host "     Settings -> Apps -> Optional features -> Add a feature -> OpenSSH Client"
    Write-Host ""
    Write-Host "  2. PuTTY (includes plink)"
    Write-Host "     Download from https://www.chiark.greenend.org.uk/~sgtatham/putty/latest.html"
    Write-Host ""
    Write-Host "After installation, run the check again."
    Write-Host ""
    Write-Host "However, port 22 is accessible - good sign!"
    Write-Host "=========================================="
    exit 0
}

Write-Host "  Using: $sshClientName"

# Выполняем проверку аутентификации
if ($sshClient -eq "ssh") {
    # OpenSSH - используем стандартный SSH
    Write-Host "  Connecting to $username@$server..."
    Write-Host "  (first connection may require key confirmation)"
    Write-Host ""
    
    # Используем Process для выполнения SSH
    $psi = New-Object System.Diagnostics.ProcessStartInfo
    $psi.FileName = "ssh"
    $psi.Arguments = "-o StrictHostKeyChecking=no -o ConnectTimeout=10 $username@$server echo AuthSuccessful"
    $psi.RedirectStandardOutput = $true
    $psi.RedirectStandardError = $true
    $psi.UseShellExecute = $false
    $psi.CreateNoWindow = $true
    
    $process = New-Object System.Diagnostics.Process
    $process.StartInfo = $psi
    $process.Start() | Out-Null
    
    $success = $process.WaitForExit(10000)
    
    $output = $process.StandardOutput.ReadToEnd()
    $error = $process.StandardError.ReadToEnd()
    $exitCode = $process.ExitCode
    
    if ($exitCode -eq 0 -or $output.Contains("AuthSuccessful")) {
        Write-Host "  OK: Authentication successful"
        Write-Host ""
        Write-Host "=========================================="
        Write-Host "RESULT: Server fully accessible!"
        Write-Host "=========================================="
        exit 0
    } else {
        Write-Host "  FAIL: Authentication failed"
        if ($error) {
            Write-Host "  $error"
        } elseif ($output) {
            Write-Host "  $output"
        }
        Write-Host ""
        Write-Host "Possible reasons:"
        Write-Host "  - Incorrect username or password"
        Write-Host "  - SSH server not configured for this user"
        exit 1
    }
} elseif ($sshClient -eq "plink") {
    # PuTTY plink
    Write-Host "  Connecting to $username@$server..."
    
    $psi = New-Object System.Diagnostics.ProcessStartInfo
    $psi.FileName = "plink"
    $psi.Arguments = "-ssh -l $username -pw $password $server echo AuthSuccessful"
    $psi.RedirectStandardOutput = $true
    $psi.RedirectStandardError = $true
    $psi.UseShellExecute = $false
    $psi.CreateNoWindow = $true
    
    $process = New-Object System.Diagnostics.Process
    $process.StartInfo = $psi
    $process.Start() | Out-Null
    
    $success = $process.WaitForExit(10000)
    
    $output = $process.StandardOutput.ReadToEnd()
    $error = $process.StandardError.ReadToEnd()
    $exitCode = $process.ExitCode
    
    if ($exitCode -eq 0 -or $output.Contains("AuthSuccessful")) {
        Write-Host "  OK: Authentication successful"
        Write-Host ""
        Write-Host "=========================================="
        Write-Host "RESULT: Server fully accessible!"
        Write-Host "=========================================="
        exit 0
    } else {
        Write-Host "  FAIL: Authentication failed"
        if ($error) {
            Write-Host "  $error"
        } elseif ($output) {
            Write-Host "  $output"
        }
        Write-Host ""
        Write-Host "Possible reasons:"
        Write-Host "  - Incorrect username or password"
        Write-Host "  - SSH server not configured for this user"
        exit 1
    }
}
