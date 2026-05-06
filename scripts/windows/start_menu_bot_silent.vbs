Option Explicit

' Arranca el listener --menu-bot sin ventana (útil desde Inicio de Windows).
Dim shell, fso, helper
Set shell = CreateObject("WScript.Shell")
Set fso = CreateObject("Scripting.FileSystemObject")
helper = fso.GetAbsolutePathName(fso.GetParentFolderName(WScript.ScriptFullName) & "\start_menu_bot_hidden.cmd")

' 0 = no mostrar ventana del proceso invocado si aplica
shell.Run """" & helper & """", 0, False
