<#
    setup_env.ps1
    --------------------------------------------
    DEEP+ environment setup helper.
    Creates or updates a .env file with the API key
    and Neon PostgreSQL database URL for the local FastAPI app.
#>

$envFile = ".\.env"

Write-Host "⚙️  Setting up DEEP+ environment file..."

# --- Ask for confirmation before overwriting ---
if (Test-Path $envFile) {
    $response = Read-Host "'.env' already exists. Overwrite? (y/n)"
    if ($response -ne "y") {
        Write-Host "❌  Aborted."
        exit
    }
}

# --- Define your variables here ---
$API_KEY = "KFA2025SuperKey"
$DATABASE_URL = "postgresql://neondb_owner:npg_VdfNpB5c9OKU@ep-red-lab-agqbzdxa-pooler.c-2.eu-central-1.aws.neon.tech/neondb?sslmode=require"

# --- Write them to .env file ---
@"
API_KEY=$API_KEY
DATABASE_URL=$DATABASE_URL
"@ | Out-File -FilePath $envFile -Encoding utf8

Write-Host "✅  .env file created successfully."
Write-Host "📄  Contents:"
Get-Content $envFile

Write-Host "`nTo verify inside Python:"
Write-Host "python -c `"import os; from dotenv import load_dotenv; load_dotenv(); print(os.getenv('API_KEY'))`""
