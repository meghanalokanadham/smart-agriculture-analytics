Add-Type -AssemblyName System.Drawing
Add-Type -AssemblyName System.Windows.Forms

$logPath = "C:\Users\megha\.gemini\antigravity\scratch\smart-agriculture-analytics\docs\screenshots\terminal_execution_log.txt"
$outPng = "C:\Users\megha\.gemini\antigravity\scratch\smart-agriculture-analytics\docs\screenshots\terminal_output_screenshot.png"

$lines = Get-Content $logPath
$font = New-Object System.Drawing.Font("Consolas", 11, [System.Drawing.FontStyle]::Regular)
$brushBg = New-Object System.Drawing.SolidBrush([System.Drawing.Color]::FromArgb(15, 23, 42))
$brushHeader = New-Object System.Drawing.SolidBrush([System.Drawing.Color]::FromArgb(52, 211, 153))
$brushText = New-Object System.Drawing.SolidBrush([System.Drawing.Color]::FromArgb(226, 232, 240))
$brushMuted = New-Object System.Drawing.SolidBrush([System.Drawing.Color]::FromArgb(148, 163, 184))

$width = 1100
$height = 28 * ($lines.Count + 4)
$bmp = New-Object System.Drawing.Bitmap($width, $height)
$gfx = [System.Drawing.Graphics]::FromImage($bmp)
$gfx.TextRenderingHint = [System.Drawing.Text.TextRenderingHint]::AntiAliasGridFit

# Background
$gfx.FillRectangle($brushBg, 0, 0, $width, $height)

# Header Bar (macOS / Linux terminal style dots)
$brushDot1 = New-Object System.Drawing.SolidBrush([System.Drawing.Color]::FromArgb(239, 68, 68))
$brushDot2 = New-Object System.Drawing.SolidBrush([System.Drawing.Color]::FromArgb(245, 158, 11))
$brushDot3 = New-Object System.Drawing.SolidBrush([System.Drawing.Color]::FromArgb(16, 185, 129))
$gfx.FillEllipse($brushDot1, 16, 14, 12, 12)
$gfx.FillEllipse($brushDot2, 34, 14, 12, 12)
$gfx.FillEllipse($brushDot3, 52, 14, 12, 12)

$titleFont = New-Object System.Drawing.Font("Segoe UI", 10, [System.Drawing.FontStyle]::Bold)
$gfx.DrawString("bash: bigdata-cluster-worker-node-01 ~ python src/analytics/master_pipeline.py", $titleFont, $brushMuted, 80, 12)

$y = 48
foreach ($line in $lines) {
    $curBrush = $brushText
    if ($line.StartsWith("===") -or $line.StartsWith("---") -or $line.StartsWith("[STEP")) {
        $curBrush = $brushHeader
    } elseif ($line.StartsWith("+") -or $line.StartsWith("|")) {
        $curBrush = $brushMuted
    }
    $gfx.DrawString($line, $font, $curBrush, 18, $y)
    $y += 24
}

$bmp.Save($outPng, [System.Drawing.Imaging.ImageFormat]::Png)
$gfx.Dispose()
$bmp.Dispose()
Write-Output "Generated: $outPng"
