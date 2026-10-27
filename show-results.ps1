#Requires -Version 5.1
<#
    Shows the MapReduce outputs from HDFS as tables (incl. intermediate steps).
    Requires that run-demo.ps1 has run once (it copies /tmp/mrdemo-env.sh into the container).
    All settings in the config block, no parameters.
#>

# ============================== CONFIG ==============================
$Container = 'namenode'
$OutBase   = '/demo/out'
$EnvRemote = '/tmp/mrdemo-env.sh'
$Culture   = [System.Globalization.CultureInfo]::GetCultureInfo('de-CH')   # 1'451.29

# Which outputs to show: HDFS sub-path, title, columns (Name + number format, '' = text)
$Outputs = @(
    @{ Path = 'a/a1_avg_by_land';           Title = 'a) A1 - GROUP BY land, AVG (unsortiert, 2 Reducer)'
       Cols = @(@{N='Land';F=''}, @{N='Durchschnitt';F='N2'}) }
    @{ Path = 'a/a2_sorted';                Title = 'a) A2 - ORDER BY 2  (Endresultat)'
       Cols = @(@{N='Land';F=''}, @{N='Durchschnitt';F='N2'}) }
    @{ Path = 'b/b1_join';                  Title = 'b) B1 - JOIN ON land -> (Bezeichnung, Summe, Anzahl)'
       Cols = @(@{N='Bezeichnung';F=''}, @{N='Summe';F='N2'}, @{N='Anzahl';F='N0'}) }
    @{ Path = 'b/b2_avg_by_bezeichnung';    Title = 'b) B2 - GROUP BY bezeichnung, AVG (unsortiert)'
       Cols = @(@{N='Bezeichnung';F=''}, @{N='Durchschnitt';F='N2'}) }
    @{ Path = 'b/b3_sorted';                Title = 'b) B3 - ORDER BY 2  (Endresultat, Reduce-Side Join)'
       Cols = @(@{N='Bezeichnung';F=''}, @{N='Durchschnitt';F='N2'}) }
    @{ Path = 'b-mapjoin/m1_mapjoin_avg';   Title = 'b) M1 - Map-Side Join + GROUP BY bezeichnung, AVG'
       Cols = @(@{N='Bezeichnung';F=''}, @{N='Durchschnitt';F='N2'}) }
    @{ Path = 'b-mapjoin/m2_sorted';        Title = 'b) M2 - ORDER BY 2  (Endresultat, Map-Side Join)'
       Cols = @(@{N='Bezeichnung';F=''}, @{N='Durchschnitt';F='N2'}) }
)
# ====================================================================

$ErrorActionPreference = 'Stop'
$inv = [System.Globalization.CultureInfo]::InvariantCulture

function Get-MrTable([string]$HdfsPath, [object[]]$Cols) {
    $lines = @(& docker exec $Container bash -c ". $EnvRemote && hdfs dfs -cat $HdfsPath/part-* 2>/dev/null")
    if ($LASTEXITCODE -ne 0) { return $null }

    $rows = New-Object System.Collections.Generic.List[object]
    foreach ($line in $lines) {
        if (-not $line.Trim()) { continue }
        $fields = $line -split "`t"
        $obj = [ordered]@{}
        for ($i = 0; $i -lt $Cols.Count; $i++) {
            $v = if ($i -lt $fields.Count) { $fields[$i].Trim() } else { '' }
            if ($Cols[$i].F) { $v = [double]::Parse($v, [System.Globalization.NumberStyles]::Float, $inv) }
            $obj[$Cols[$i].N] = $v
        }
        $rows.Add([pscustomobject]$obj)
    }
    return , $rows
}

function Get-DisplayProps([object[]]$Cols) {
    $props = New-Object System.Collections.Generic.List[object]
    foreach ($c in $Cols) {
        $name = $c.N; $fmt = $c.F; $cul = $Culture
        if ($fmt) {
            $props.Add(@{ Name = $name; Alignment = 'Right'
                          Expression = { $_.$name.ToString($fmt, $cul) }.GetNewClosure() })
        } else {
            $props.Add($name)
        }
    }
    return , $props.ToArray()
}

foreach ($o in $Outputs) {
    $path = "$OutBase/$($o.Path)"
    Write-Host "`n=== $($o.Title)" -ForegroundColor Cyan
    Write-Host "    $path" -ForegroundColor DarkGray

    $table = Get-MrTable -HdfsPath $path -Cols $o.Cols
    if ($null -eq $table -or $table.Count -eq 0) {
        Write-Warning "No output at $path (variant not run yet?)"
        continue
    }
    $props = Get-DisplayProps -Cols $o.Cols
    $table | Format-Table -Property $props -AutoSize | Out-Host
}