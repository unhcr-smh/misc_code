@echo off
CLS
echo %cd%

ECHO 1.nameplate
ECHO 2.controller
ECHO.

:getChoice
set "fn=123"
set "choice=0"
set /p "choice=Type your choice, 1 or 2 ...  "

if not %choice% EQU 1 ( 
    if not %choice% EQU 2 ( 
        echo Please enter a valid option
        goto getChoice
        )
    )


if %choice% EQU 1 (
  set "fn=nameplate"  
  GOTO execute
)

if %choice% EQU 2 (
  set "fn=controller"  
  GOTO execute
)

:execute
echo %fn%
rem GOTO end

rem You must set this to the appropriate token for your account:

set header=Authorization: Token fbe39af96d7aceba9450a013ac3a69c033a13809

 

rem Necessary for variable assignment inside loop; see https://ss64.com/nt/delayedexpansion.html

setlocal EnableDelayedExpansion


md output_%fn%
echo TEST "!ERRORLEVEL!"

set /a count = 1
for /f "tokens=*" %%A in (urls_%fn%.txt) do (

    set url=%%A

    rem https://superuser.com/a/1497078

    set url_after_question_mark=!url:*?=!

        if exist !cd!/output_!fn!/!url_after_question_mark! (
            echo file exists !cd!/output_!fn!/!url_after_question_mark!
        ) else (
            echo file doesn't exist
        

            echo !count!  Now downloading !url!
            set /a count += 1

            curl --header "%header%" --output-dir "output_!fn!" --output "!url_after_question_mark!" "!url!"

            rem Bizarre syntax to echo a blank line

            echo(

            )
        )   

:end