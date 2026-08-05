$text = Get-Content -Raw -Encoding UTF8 tmp_navi_bundle.js
$patterns = @(
  'https?://[^"''``<> ]+',
  'wss?://[^"''``<> ]+',
  '\b[\w./-]+\.(?:php|json|ashx|asmx|svc|aspx)\b(?:\?[^"''``<> ]*)?',
  '\b(?:ajax|api|transport|vehicle|route|monitor|gps|markers|marsh|rasp|stop)[A-Za-z0-9_./?=-]{0,140}'
)
$seen = [ordered]@{}
foreach ($p in $patterns) {
  [regex]::Matches($text, $p, 'IgnoreCase') | ForEach-Object {
    $v = $_.Value.Trim('\')
    if (-not $seen.Contains($v)) { $seen[$v] = $true }
  }
}
$seen.Keys | Select-Object -First 700
"COUNT $($seen.Count)"
