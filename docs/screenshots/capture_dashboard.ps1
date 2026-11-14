Add-Type -AssemblyName System.Windows.Forms
Add-Type -AssemblyName System.Drawing

$url = "C:\Users\megha\.gemini\antigravity\scratch\smart-agriculture-analytics\dashboard\index.html"
$outPng = "C:\Users\megha\.gemini\antigravity\scratch\smart-agriculture-analytics\docs\screenshots\dashboard_view_screenshot.png"

$form = New-Object System.Windows.Forms.Form
$form.Width = 1420
$form.Height = 980
$form.FormBorderStyle = [System.Windows.Forms.FormBorderStyle]::None
$form.StartPosition = [System.Windows.Forms.FormStartPosition]::Manual
$form.Location = New-Object System.Drawing.Point(0, 0)

$browser = New-Object System.Windows.Forms.WebBrowser
$browser.Dock = [System.Windows.Forms.DockStyle]::Fill
$browser.ScrollBarsEnabled = $false
$form.Controls.Add($browser)

$bmp = New-Object System.Drawing.Bitmap($form.Width, $form.Height)

$form.Add_Shown({
    $timer = New-Object System.Windows.Forms.Timer
    $timer.Interval = 1800
    $timer.Add_Tick({
        $timer.Stop()
        $gfx = [System.Drawing.Graphics]::FromImage($bmp)
        $rect = $form.Bounds
        $gfx.CopyFromScreen($form.Location.X, $form.Location.Y, 0, 0, $rect.Size)
        $bmp.Save($outPng, [System.Drawing.Imaging.ImageFormat]::Png)
        $gfx.Dispose()
        $form.Close()
    })
    $timer.Start()
})

$browser.Navigate($url)
[System.Windows.Forms.Application]::Run($form)
Write-Output "Captured: $outPng"
