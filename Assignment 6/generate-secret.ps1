$envFile = "D:\Kubernetes\.env"
$secretName = "my-env-secret"
$outputYaml = "D:\Kubernetes\secret-from-env.yaml"

$yaml = @"
apiVersion: v1
kind: Secret
metadata:
  name: $secretName
type: Opaque
data:
"@

# Read each line and convert to base64
Get-Content $envFile | ForEach-Object {
    if ($_ -match "^\s*#|^\s*$") { return }  # Skip comments/empty lines
    $parts = $_ -split '=', 2
    $key = $parts[0].Trim()
    $value = $parts[1].Trim()
    $encoded = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($value))
    $yaml += "  ${key}: $encoded`n"
}

# Save to file
$yaml | Out-File -Encoding ascii $outputYaml

Write-Host "✅ Secret YAML generated at: $outputYaml" -ForegroundColor Green