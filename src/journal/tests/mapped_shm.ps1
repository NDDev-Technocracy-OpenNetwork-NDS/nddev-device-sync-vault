# Isolated Windows acceptance: retain a real mapping of the owned SQLite SHM.
# No application FFI, injected DLL, owner database or clipboard is involved.
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'
$stream = $null
$mapping = $null
$view = $null
try {
    $sharing = [System.IO.FileShare]::ReadWrite -bor [System.IO.FileShare]::Delete
    $stream = [System.IO.File]::Open($env:NDS_FIXTURE_SHM, [System.IO.FileMode]::Open,
        [System.IO.FileAccess]::ReadWrite, $sharing)
    $mapping = [System.IO.MemoryMappedFiles.MemoryMappedFile]::CreateFromFile(
        $stream, $null, 0, [System.IO.MemoryMappedFiles.MemoryMappedFileAccess]::ReadWrite,
        [System.IO.HandleInheritability]::None, $true)
    $view = $mapping.CreateViewAccessor(0, 0,
        [System.IO.MemoryMappedFiles.MemoryMappedFileAccess]::ReadWrite)
    [Console]::Out.Write("NDS_SHM_READY`n")
    [Console]::Out.Flush()
    if ([Console]::In.Read() -ne 82) { throw 'unexpected fixture control' }
} catch {
    [Console]::Error.WriteLine('mapped-SHM fixture failed: ' + $_.Exception.GetType().FullName)
    exit 1
} finally {
    if ($null -ne $view) { $view.Dispose() }
    if ($null -ne $mapping) { $mapping.Dispose() }
    if ($null -ne $stream) { $stream.Dispose() }
}
