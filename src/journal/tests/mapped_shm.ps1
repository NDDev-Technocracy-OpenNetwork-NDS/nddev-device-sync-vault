# Isolated Windows acceptance: retain a real mapping of the owned SQLite SHM.
# No application FFI, injected DLL, owner database or clipboard is involved.
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'
$stream = $null
$mapping = $null
$view = $null
$stage = 'open'
try {
    $sharing = [System.IO.FileShare]::ReadWrite -bor [System.IO.FileShare]::Delete
    $stream = [System.IO.File]::Open($env:NDS_FIXTURE_SHM, [System.IO.FileMode]::Open,
        [System.IO.FileAccess]::ReadWrite, $sharing)
    $stage = 'map'
    $mapping = [System.IO.MemoryMappedFiles.MemoryMappedFile]::CreateFromFile(
        $stream, [System.Management.Automation.Language.NullString]::Value, 0,
        [System.IO.MemoryMappedFiles.MemoryMappedFileAccess]::ReadWrite,
        [System.IO.HandleInheritability]::None, $true)
    $stage = 'view'
    $view = $mapping.CreateViewAccessor(0, 0,
        [System.IO.MemoryMappedFiles.MemoryMappedFileAccess]::ReadWrite)
    $stage = 'ready'
    [Console]::Out.Write("NDS_SHM_READY`n")
    [Console]::Out.Flush()
    $stage = 'control'
    if ([Console]::In.Read() -ne 82) { throw 'unexpected fixture control' }
} catch {
    $exception = $_.Exception
    for ($depth = 0; $null -ne $exception -and $depth -lt 4; $depth++) {
        [Console]::Error.WriteLine('mapped-SHM fixture: stage=' + $stage +
            ' type=' + $exception.GetType().FullName + ' hresult=' + $exception.HResult)
        $exception = $exception.InnerException
    }
    exit 1
} finally {
    if ($null -ne $view) { $view.Dispose() }
    if ($null -ne $mapping) { $mapping.Dispose() }
    if ($null -ne $stream) { $stream.Dispose() }
}
