# MSF Referrals Data Converter
# Converts old CSV format to new JSON format

param(
    [Parameter(Mandatory=$true)]
    [string]$InputFile,
    
    [Parameter(Mandatory=$false)]
    [string]$OutputFile = "referrals.json"
)

# Function to parse date in DD-MMM-YY or D-MMM-YY format
function Parse-Date {
    param([string]$dateStr)
    
    if ([string]::IsNullOrWhiteSpace($dateStr)) {
        return $null
    }
    
    try {
        # Handle formats like "13-Oct-83", "17-Sep-24", "1-Jan-25"
        $date = [DateTime]::ParseExact($dateStr, @('d-MMM-yy', 'dd-MMM-yy'), $null)
        
        # Adjust 2-digit year: 00-49 = 2000s, 50-99 = 1900s
        if ($date.Year -lt 100) {
            if ($date.Year -lt 50) {
                $date = $date.AddYears(2000)
            } else {
                $date = $date.AddYears(1900)
            }
        }
        
        return $date.ToString("yyyy-MM-dd")
    } catch {
        try {
            # Fallback to general parse
            $date = [DateTime]::Parse($dateStr)
            return $date.ToString("yyyy-MM-dd")
        } catch {
            return $null
        }
    }
}

# Function to clean phone numbers
function Clean-Phone {
    param([string]$phone)
    if ([string]::IsNullOrWhiteSpace($phone)) { return "" }
    return $phone.Trim()
}

# Function to clean email
function Clean-Email {
    param([string]$email)
    if ([string]::IsNullOrWhiteSpace($email)) { return "" }
    return $email.Trim().ToLower()
}

Write-Host "Converting $InputFile to $OutputFile..." -ForegroundColor Cyan

# Import the CSV file
$oldData = Import-Csv -Path $InputFile

Write-Host "Loaded $($oldData.Count) rows from input file" -ForegroundColor Green

# Initialize the new JSON structure
$newData = @{
    referrals = @()
    nextId = 1
    selectOptions = @{
        requestedLocations = @('Any', 'Downtown', 'Mississauga', 'Vaughan')
        requestedPhysicians = @('First Available', 'Dr. Bacal', 'Dr. Greenblatt', 'Dr. Jones', 'Dr. Liu', 'Dr. Michaeli', 'Dr. Pereira', 'Dr. Russo', 'Dr. Shapiro')
        servicesRequested = @('Infertility', 'EEF', 'ONC', 'SB', 'RPL', 'Donor', 'ARA', 'PGD', 'Gyne', 'Other')
        referralType = @('New', 'Previous', 'Partner')
        lastAttemptModes = @('Phone', 'E-Mail')
        physicianAdmins = @('CJ Admin', 'EG Admin', 'HS Admin', 'JM Admin', 'KL Admin', 'MR Admin', 'NP Admin', 'VB Admin', 'NursePrac Admin', 'Fellow Admin')
        genderAtBirth = @('Female', 'Male', 'Other')
    }
}

$rowCount = 0
# Convert each row
foreach ($row in $oldData) {
    $rowCount++
    
    # Skip rows with no last name (empty rows)
    if ([string]::IsNullOrWhiteSpace($row.'LAST NAME')) {
        Write-Host "Skipping empty row $rowCount" -ForegroundColor Yellow
        continue
    }
    
    # Determine referral status based on "Referral Complete" field
    # Old states: Pending, Complete, Cancelled, Deferred
    # New states: New, Pending, Info Received, Completed, Deferred
    
    $oldStatus = $row.'Referral Complete'
    $hasFirstAttempt = ![string]::IsNullOrWhiteSpace($row.'1st Attempt to reach Patient/Referring MD')
    $hasCompleteInfo = ![string]::IsNullOrWhiteSpace($row.'Date Complete Information received')
    
    $status = "New"  # Default
    
    if ($oldStatus -eq "Complete") {
        $status = "Completed"
    } elseif ($oldStatus -in @("Cancelled", "Deferred")) {
        $status = "Deferred"
    } elseif ($oldStatus -eq "Pending") {
        # Pending in old system maps to:
        # - "New" if no 1st attempt yet
        # - "Pending" if has 1st attempt
        # - "Info Received" if has complete info date
        if ($hasCompleteInfo) {
            $status = "Info Received"
        } elseif ($hasFirstAttempt) {
            $status = "Pending"
        } else {
            $status = "New"
        }
    }
    
    # Determine referral type from "New or Returning" column
    # Maps: "New" -> "New", "Prev Pt" -> "Previous", "Partner" -> "Partner"
    $referralType = "New"  # Default
    $newOrReturning = $row.'New or Returning'
    
    if (![string]::IsNullOrWhiteSpace($newOrReturning)) {
        $normalized = $newOrReturning.Trim()
        
        # Check for "Prev Pt", "Previous", "Returning", "Return"
        if ($normalized -match "(?i)^Prev\s*Pt$|^Previous$|^Returning$|^Return") {
            $referralType = "Previous"
        }
        # Check for "Partner"
        elseif ($normalized -match "(?i)^Partner$") {
            $referralType = "Partner"
        }
        # Check for "New"
        elseif ($normalized -match "(?i)^New$") {
            $referralType = "New"
        }
        # If none match, default to "New" (already set)
    }
    
    # Build attempt history from the 3 attempts
    $attemptHistory = @()
    
    # 1st attempt
    if ($hasFirstAttempt) {
        $contactMode1 = "Phone"  # Default
        if (![string]::IsNullOrWhiteSpace($row.'Email')) {
            # "Email" column is the contact type for 1st attempt
            # Map "Phone Call" -> "Phone", "Email" -> "E-Mail"
            if ($row.'Email' -match "(?i)email|e-mail") {
                $contactMode1 = "E-Mail"
            }
        }
        
        $attempt1 = @{
            date = Parse-Date $row.'1st Attempt to reach Patient/Referring MD'
            time = ""
            mode = $contactMode1
            comment = if (![string]::IsNullOrWhiteSpace($row.'Comments')) { $row.'Comments' } else { "" }
        }
        $attemptHistory += $attempt1
    }
    
    # 2nd attempt
    if (![string]::IsNullOrWhiteSpace($row.'2nd Attempt to reach Patient/Referring MD')) {
        $contactMode2 = "Phone"  # Default
        if (![string]::IsNullOrWhiteSpace($row.'Type of Contact')) {
            if ($row.'Type of Contact' -match "(?i)email|e-mail") {
                $contactMode2 = "E-Mail"
            }
        }
        
        $attempt2 = @{
            date = Parse-Date $row.'2nd Attempt to reach Patient/Referring MD'
            time = ""
            mode = $contactMode2
            comment = if (![string]::IsNullOrWhiteSpace($row.'Comments2')) { $row.'Comments2' } else { "" }
        }
        $attemptHistory += $attempt2
    }
    
    # 3rd attempt
    if (![string]::IsNullOrWhiteSpace($row.'3rd Attempt to reach Patient/Referring MD')) {
        $contactMode3 = "Phone"  # Default
        if (![string]::IsNullOrWhiteSpace($row.'Type of Contact3')) {
            if ($row.'Type of Contact3' -match "(?i)email|e-mail") {
                $contactMode3 = "E-Mail"
            }
        }
        
        $attempt3 = @{
            date = Parse-Date $row.'3rd Attempt to reach Patient/Referring MD'
            time = ""
            mode = $contactMode3
            comment = if (![string]::IsNullOrWhiteSpace($row.'Comments4')) { $row.'Comments4' } else { "" }
        }
        $attemptHistory += $attempt3
    }
    
    # Get the last attempt for lastAttemptDate fields
    $lastAttempt = if ($attemptHistory.Count -gt 0) { $attemptHistory[-1] } else { $null }
    
    # Create the new referral object
    $newReferral = @{
        referralID = $newData.nextId
        addedToDBDate = (Get-Date).ToString("yyyy-MM-ddTHH:mm:ss")
        
        # Referral Information
        referralDate = Parse-Date $row.'Date Referral Received'
        receivedDate = Parse-Date $row.'Date Referral Received'
        fileName = ""
        referringPhysicianName = if (![string]::IsNullOrWhiteSpace($row.'Referring MD/NP')) { $row.'Referring MD/NP'.Trim() } else { "" }
        referringPhysicianBilling = ""
        referringPhysicianFax = ""
        referringPhysicianPhone = ""
        referringPhysicianEmail = ""
        
        # Service Information
        requestedLocation = "Any"
        requestedPhysician = if (![string]::IsNullOrWhiteSpace($row.'Requested Physician')) { $row.'Requested Physician'.Trim() } else { "First Available" }
        urgent = $false
        serviceRequested = if (![string]::IsNullOrWhiteSpace($row.'Service Requested')) { $row.'Service Requested'.Trim() } else { "" }
        subServiceRequested = if (![string]::IsNullOrWhiteSpace($row.'Sub Service Requested')) { $row.'Sub Service Requested'.Trim() } else { "" }
        referralType = $referralType
        
        # Patient Information
        patientPID = if (![string]::IsNullOrWhiteSpace($row.'PID')) { $row.'PID'.Trim() } else { "" }
        patientMRN = ""
        patientFirstName = if (![string]::IsNullOrWhiteSpace($row.'FIRST NAME')) { $row.'FIRST NAME'.Trim() } else { "" }
        patientMiddleName = ""
        patientLastName = if (![string]::IsNullOrWhiteSpace($row.'LAST NAME')) { $row.'LAST NAME'.Trim() } else { "" }
        patientDOB = Parse-Date $row.'DOB'
        patientPhone = Clean-Phone $row.'Phone'
        patientEmail = Clean-Email $row.'E-Mail'  # This is the NEW patient email column (mostly empty)
        patientAddress = ""
        patientHC = ""
        patientGenderAtBirth = ""
        
        # Partner Information (empty by default)
        partnerPID = ""
        partnerMRN = ""
        partnerFirstName = ""
        partnerMiddleName = ""
        partnerLastName = ""
        partnerDOB = $null
        partnerPhone = ""
        partnerEmail = ""
        partnerAddress = ""
        partnerHC = ""
        partnerGenderAtBirth = ""
        
        # Referral Handling
        referralStatus = $status
        lastAttemptDate = if ($lastAttempt) { $lastAttempt.date } else { $null }
        lastAttemptTime = if ($lastAttempt) { $lastAttempt.time } else { "" }
        lastAttemptMode = if ($lastAttempt) { $lastAttempt.mode } else { $null }
        lastAttemptComment = if ($lastAttempt) { $lastAttempt.comment } else { $null }
        attemptHistory = $attemptHistory
        faxedBackDate = $null
        completeInfoReceivedDate = Parse-Date $row.'Date Complete Information received'
        taskedToPhysicianAdmin = if (![string]::IsNullOrWhiteSpace($row.'Tasked To')) { $row.'Tasked To'.Trim() } else { $null }
        referralCompleteDate = if ($oldStatus -eq "Complete") { Parse-Date $row.'Date Complete Information received' } else { $null }
        notes = if (![string]::IsNullOrWhiteSpace($row.'Notes')) { $row.'Notes'.Trim() } else { $null }
        notesDate = Parse-Date $row.'Date'
        notesHistory = @()
    }
    
    $newData.referrals += $newReferral
    $newData.nextId++
    
    if ($rowCount % 100 -eq 0) {
        Write-Host "Processed $rowCount rows..." -ForegroundColor Gray
    }
}

# Convert to JSON
Write-Host "`nConverting to JSON format..." -ForegroundColor Cyan
$jsonOutput = $newData | ConvertTo-Json -Depth 10 -Compress:$false

# Ensure DB folder exists - resolve to full path
if (![System.IO.Path]::IsPathRooted($OutputFile)) {
    $OutputFile = Join-Path (Get-Location) $OutputFile
}

$outputDir = Split-Path -Parent $OutputFile
if ($outputDir -and !(Test-Path $outputDir)) {
    New-Item -ItemType Directory -Path $outputDir -Force | Out-Null
}

# Save to file WITHOUT BOM (critical for Python compatibility)
Write-Host "Saving to $OutputFile (UTF-8 without BOM)..." -ForegroundColor Cyan
$utf8NoBom = New-Object System.Text.UTF8Encoding $false
[System.IO.File]::WriteAllText($OutputFile, $jsonOutput, $utf8NoBom)

# Verify encoding
$bytes = [System.IO.File]::ReadAllBytes($OutputFile)
$firstBytes = $bytes[0..3] | ForEach-Object { $_.ToString("X2") }
if ($firstBytes[0] -eq "EF" -and $firstBytes[1] -eq "BB" -and $firstBytes[2] -eq "BF") {
    Write-Host "WARNING: File still has UTF-8 BOM!" -ForegroundColor Red
} else {
    Write-Host "File encoding verified: UTF-8 without BOM [OK]" -ForegroundColor Green
}

Write-Host "`nConversion complete!" -ForegroundColor Green
Write-Host "Converted $($newData.referrals.Count) referrals from $rowCount total rows" -ForegroundColor Green
Write-Host "Output saved to: $OutputFile" -ForegroundColor Cyan
Write-Host "`nNext ID will be: $($newData.nextId)" -ForegroundColor Yellow

# Show status breakdown
$statusCount = $newData.referrals | Group-Object -Property referralStatus | Select-Object Name, Count
Write-Host "`nStatus breakdown:" -ForegroundColor Cyan
$statusCount | Format-Table -AutoSize

# Show referral type breakdown
$typeCount = $newData.referrals | Group-Object -Property referralType | Select-Object Name, Count
Write-Host "Referral type breakdown:" -ForegroundColor Cyan
$typeCount | Format-Table -AutoSize
