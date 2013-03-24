Option explicit
Dim vers, searchPath, cmd, cp, javaClass, args, wsh

vers = "0.3.14"
searchPath = "'%\\fs-c-" & vers & "\\%'"
cmd = "java"
cp = "-cp "
javaClass = "de.pc2.dedup.fschunk.trace.Main"
args = ""


' Remove last character of string str
Function RemoveLastChar(str)
	Dim length
	length = Len(str)
	RemoveLastChar = Mid(str, 1, length - 1)
End Function


' Use WMI to get the classpath
Function GetCpFromWMI
	Dim strComputer, wmiService, jarFiles, jarFile, p
	strComputer = "."
	Set wmiService = GetObject("winmgmts:\\" & strComputer & "\root\cimv2")
	Set jarFiles = wmiService.ExecQuery _
		("Select * from CIM_DataFile Where Path like " & searchPath & " and Extension = 'jar'")
	For Each jarFile in jarFiles
		p = p & jarFile.Drive & jarFile.Path & jarFile.FileName & "." & jarFile.Extension & ";"
	Next
	GetCpFromWMI = RemoveLastChar(p)
End Function


Function GetArguments
	Dim wsArgs, i, a
	Set wsArgs = WScript.Arguments
	For i = 0 to wsArgs.Count - 1
	   a = a & wsArgs(i) & " "
	Next

	' remove last whitespace
	If a <> "" Then
		a = RemoveLastChar(a)
	End If
	GetArguments = a
End Function

'################################ main ##################################

args = GetArguments
cp = cp & GetCpFromWMI

' create command
cmd = cmd & " " & cp & " " & javaClass & " " & args
Set wsh = WScript.CreateObject("WScript.Shell")
Wscript.Echo "Executing: " & cmd
' run the command see: http://msdn.microsoft.com/en-us/library/d5fk67ky%28v=VS.85%29.aspx
wsh.Run "cmd /K " & cmd, 1, False
