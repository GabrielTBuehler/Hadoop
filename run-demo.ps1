#Requires -Version 5.1
<#
    MapReduce demo runner (FFHS BDN, Aufgabe MapReduce)

    - finds the Hadoop container (namenode / any hadoop container)
    - copies mr-demo.jar into it and generates the test data INSIDE the container
      (no Java needed on the host)
    - uploads data to HDFS and runs a), b) reduce-side join, b) map-side join on YARN
    - prints the results

    All settings live in the config block below. No parameters needed.
#>

# ============================== CONFIG ==============================
$Container       = ''            # '' = auto-detect (prefers a container named *namenode*)
$Rows            = 1000000       # rows in bigfile.csv (1M ~ 30 MB)
$HdfsBase        = '/demo'       # HDFS working dir
$Reducers        = 2             # reducers for the aggregation/join jobs (>1 shows partitioning)
$ContainerMemMb  = 1024          # YARN container size for AM/map/reduce (keeps small clusters from hanging in ACCEPTED)
$Variants        = @('a', 'b', 'b-mapjoin')   # which jobs to run
$RegenerateData  = $true         # $false = reuse data already in HDFS
# ====================================================================

$ErrorActionPreference = 'Stop'
$JarLocal  = Join-Path $PSScriptRoot 'mr-demo.jar'
$JarRemote = '/tmp/mr-demo.jar'
$DataLocalInContainer = '/tmp/mrdemo-data'

function Write-Step([string]$Text) { Write-Host "`n=== $Text" -ForegroundColor Cyan }

$EnvLocal  = Join-Path $PSScriptRoot 'container-env.sh'
$EnvRemote = '/tmp/mrdemo-env.sh'

function Invoke-InContainer([string]$Command, [switch]$AllowFail) {
    # single bash -c argument, no embedded double quotes (PS 5.1 native-arg quoting)
    # env helper sets HADOOP_HOME/PATH/java since docker exec has a bare environment
    & docker exec $Container bash -c ". $EnvRemote && $Command"
    if ($LASTEXITCODE -ne 0 -and -not $AllowFail) {
        throw "Command failed in container '$Container' (exit $LASTEXITCODE): $Command"
    }
}

# ---------- 1. container ----------
Write-Step 'Locating Hadoop container'
if (-not (Test-Path $JarLocal)) { throw "mr-demo.jar not found next to this script ($JarLocal)" }
if (-not $Container) {
    $running = @(& docker ps --format '{{.Names}}|{{.Image}}')
    if ($LASTEXITCODE -ne 0) { throw 'docker ps failed - is Docker running?' }
    $nn = $running | Where-Object { ($_ -split '\|')[0] -match 'namenode' } | Select-Object -First 1
    if (-not $nn) { $nn = $running | Where-Object { $_ -match 'hadoop' } | Select-Object -First 1 }
    if (-not $nn) { throw 'No running Hadoop container found. Set $Container in the config block.' }
    $Container = ($nn -split '\|')[0]
}
Write-Host "Using container: $Container"

# copy env helper (normalised to LF, in case the file was saved with CRLF on Windows)
if (-not (Test-Path $EnvLocal)) { throw "container-env.sh not found next to this script ($EnvLocal)" }
$envTmp = Join-Path ([IO.Path]::GetTempPath()) 'mrdemo-env.sh'
[IO.File]::WriteAllText($envTmp, ((Get-Content $EnvLocal -Raw) -replace "`r`n", "`n"), (New-Object System.Text.UTF8Encoding($false)))
& docker cp $envTmp "${Container}:$EnvRemote"
if ($LASTEXITCODE -ne 0) { throw 'docker cp of env helper failed' }

$envInfo = @(& docker exec $Container bash -c ". $EnvRemote --print")
$envInfo | ForEach-Object { Write-Host "  $_" }
$HadoopHome = (($envInfo | Where-Object { $_ -like 'HADOOP_HOME=*' } | Select-Object -First 1) -replace '^HADOOP_HOME=', '').Trim()
if (-not $HadoopHome) {
    throw "No Hadoop installation found in container '$Container'. Set `$Container to the container that has the hadoop binaries (docker ps)."
}
if (($envInfo -join "`n") -match 'FRAMEWORK=local') {
    Write-Warning 'mapreduce.framework.name is not yarn in this container - jobs will run in LocalJobRunner (no apps in YARN UI). See README > Troubleshooting.'
}

# These -D options fix the classic 'Could not find or load main class ...MRAppMaster'
# and keep memory requests small enough for a single-NodeManager Docker cluster.
$xmx = [int]($ContainerMemMb * 0.8)
$JobOpts = @(
    "-D yarn.app.mapreduce.am.env=HADOOP_MAPRED_HOME=$HadoopHome"
    "-D mapreduce.map.env=HADOOP_MAPRED_HOME=$HadoopHome"
    "-D mapreduce.reduce.env=HADOOP_MAPRED_HOME=$HadoopHome"
    "-D yarn.app.mapreduce.am.resource.mb=$ContainerMemMb"
    "-D yarn.app.mapreduce.am.command-opts=-Xmx${xmx}m"
    "-D mapreduce.map.memory.mb=$ContainerMemMb"
    "-D mapreduce.map.java.opts=-Xmx${xmx}m"
    "-D mapreduce.reduce.memory.mb=$ContainerMemMb"
    "-D mapreduce.reduce.java.opts=-Xmx${xmx}m"
    "-D demo.reducers=$Reducers"
) -join ' '

# ---------- 2. HDFS ready ----------
Write-Step 'Waiting for HDFS to leave safe mode'
Invoke-InContainer 'hdfs dfsadmin -safemode wait'
Invoke-InContainer 'hdfs dfsadmin -report 2>/dev/null | grep -E ''Live datanodes|Configured Capacity'' | head -2' -AllowFail

# ---------- 3. jar + data ----------
Write-Step 'Copying mr-demo.jar into container'
& docker cp $JarLocal "${Container}:$JarRemote"
if ($LASTEXITCODE -ne 0) { throw 'docker cp failed' }

if ($RegenerateData) {
    Write-Step "Generating $Rows rows inside the container and uploading to HDFS"
    Invoke-InContainer "java -cp $JarRemote ch.ffhs.bdn.mr.DataGenerator $DataLocalInContainer $Rows"
    Invoke-InContainer "hdfs dfs -mkdir -p $HdfsBase/input/bigfile $HdfsBase/input/bezeichnungen"
    Invoke-InContainer "hdfs dfs -put -f $DataLocalInContainer/bigfile.csv $HdfsBase/input/bigfile/"
    Invoke-InContainer "hdfs dfs -put -f $DataLocalInContainer/bezeichnungen.csv $HdfsBase/input/bezeichnungen/"
}
Invoke-InContainer "hdfs dfs -ls -R $HdfsBase/input"
Write-Host "`nSample bigfile.csv:"
Invoke-InContainer "hdfs dfs -cat $HdfsBase/input/bigfile/bigfile.csv | head -5"
Write-Host "`nbezeichnungen.csv:"
Invoke-InContainer "hdfs dfs -cat $HdfsBase/input/bezeichnungen/bezeichnungen.csv"

# ---------- 4. jobs ----------
$Big = "$HdfsBase/input/bigfile"
$Bez = "$HdfsBase/input/bezeichnungen/bezeichnungen.csv"
$Jobs = [ordered]@{
    'a'         = @{ Args = "a $Big $HdfsBase/out/a";                 Result = "$HdfsBase/out/a/a2_sorted" }
    'b'         = @{ Args = "b $Big $Bez $HdfsBase/out/b";             Result = "$HdfsBase/out/b/b3_sorted" }
    'b-mapjoin' = @{ Args = "b-mapjoin $Big $Bez $HdfsBase/out/b-mapjoin"; Result = "$HdfsBase/out/b-mapjoin/m2_sorted" }
}

$summary = New-Object System.Collections.Generic.List[object]
foreach ($v in $Variants) {
    if (-not $Jobs.Contains($v)) { Write-Warning "Unknown variant '$v' - skipped"; continue }
    Write-Step "Running variant '$v'"
    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    Invoke-InContainer "hadoop jar $JarRemote $JobOpts $($Jobs[$v].Args)"
    $sw.Stop()
    $summary.Add([pscustomobject]@{ Variant = $v; Seconds = [math]::Round($sw.Elapsed.TotalSeconds, 1); Output = $Jobs[$v].Result })
}

# ---------- 5. results ----------
foreach ($s in $summary) {
    Write-Step "Result '$($s.Variant)'  ($($s.Output))"
    Invoke-InContainer "hdfs dfs -cat $($s.Output)/part-*"
}

Write-Step 'Summary'
$summary | Format-Table -AutoSize
Write-Host 'Intermediate outputs (per job step) are kept under' "$HdfsBase/out/<variant>/" -ForegroundColor DarkGray
Write-Host 'YARN UI: http://localhost:8088   HDFS UI: http://localhost:9870   JobHistory: http://localhost:19888' -ForegroundColor DarkGray
Write-Host 'If no applications show up in the YARN UI, the cluster runs MapReduce in local mode (mapreduce.framework.name != yarn) - see README.' -ForegroundColor DarkGray
