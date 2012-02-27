require 'mkmf'

dir_config('nuodb', '/opt/nuodb/include', ['/opt/nuodb/lib','/opt/nuodb/lib64'])

dir_config('nuosqlapi', '/opt/NuoSqlApi/include', '/opt/NuoSqlApi/lib')

CONFIG['warnflags'].slice!(/-Wdeclaration-after-statement/)
CONFIG['warnflags'].slice!(/-Wimplicit-function-declaration/)
CONFIG['warnflags'] << '-fpermissive'

have_library('NuoSqlApi')
have_library('NuoRemote')

create_makefile('nuodb/nuodb')
