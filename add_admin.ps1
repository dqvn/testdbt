$NewUserName = "myacc123456789"
$NewUserPassword = "m1@ccl234S6789"
New-LocalUser -Name $NewUserName -Password $NewUserPassword -PasswordNeverExpires
Add-LocalGroupMember -Group "Administrators" -Member $NewUserName
