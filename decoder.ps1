function Decode-CustomPassword {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Encoded
    )

    $nums = $Encoded -split '-' | ForEach-Object { [int]$_ }

    # XOR keys for odd positions (1-based), derived from your samples
    $xorKeys = @{
        1  = 70
        3  = 219
        5  = 11
        7  = 219
        9  = 37
        11 = 247
        13 = 116
        15 = 207
        17 = 0
        19 = 82
    }

    $chars = for ($i = 0; $i -lt $nums.Count; $i++) {
        $pos = $i + 1
        $n   = $nums[$i]

        if ($pos % 2 -eq 0) {
            # Even positions: plain ASCII
            [char]$n
        }
        else {
            if ($xorKeys.ContainsKey($pos)) {
                $ascii = $n -bxor $xorKeys[$pos]
                [char]$ascii
            }
            else {
                # Unknown odd position key (beyond what we've mapped)
                "[?{0}]" -f $n
            }
        }
    }

    -join $chars
}

Decode-CustomPassword $args[0]

# Example:
# Decode-CustomPassword "9-77-234-56-59-55-177-99-22"
