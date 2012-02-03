#include <ruby.h>

extern "C"
VALUE Init_nuodb(void)
{
    rb_require("dbi");
    
    VALUE nuoModule = rb_define_module("NuoDB");
}