@setlocal
@REM This is for NuoDB development only -- do not include in the release!
set JARDIR=C:\dev\NuoDB\jar
set DEVDIR=..\nuodb-jruby
jruby -I %DEVDIR%\activerecord-jdbcnuodb-adapter\lib -I %DEVDIR%\jdbc-nuodb\lib -I %JARDIR% sample.rb
