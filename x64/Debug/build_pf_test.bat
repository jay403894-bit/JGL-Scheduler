@echo off
call "C:\Program Files\Microsoft Visual Studio\18\Community\VC\Auxiliary\Build\vcvars64.bat" >nul 2>&1
cd /d "C:\Users\jay40\source\repos\T_Threads\T_Threads\x64\Debug"
cl /nologo /std:c++17 /EHsc /MDd /Zi /I "C:\Users\jay40\source\repos\T_Threads\T_Threads" "C:\Users\jay40\source\repos\T_Threads\T_Threads\pf_test.cpp" T_Threads.lib /Fe:pf_test.exe /link /SUBSYSTEM:CONSOLE
echo CL_EXIT=%ERRORLEVEL%
