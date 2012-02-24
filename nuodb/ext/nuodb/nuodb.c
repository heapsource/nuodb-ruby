#include <ruby.h>
#ifndef RSTRING_PTR
#define RSTRING_PTR(str) RSTRING(str)->ptr
#endif
#ifndef RSTRING_LEN
#define RSTRING_LEN(str) RSTRING(str)->len
#endif
#ifndef RARRAY_PTR
#define RARRAY_PTR(ary) RARRAY(ary)->ptr
#endif
#ifndef HAVE_RB_STR_SET_LEN
#define rb_str_set_len(str, length) (RSTRING_LEN(str) = (length))
#endif

#define NILorSTRING(obj)	(NIL_P(obj)? NULL: StringValuePtr(obj))
#define NILorINT(obj)		(NIL_P(obj)? 0: NUM2INT(obj))

#define GetMysqlStruct(obj)	(Check_Type(obj, T_DATA), (struct mysql*)DATA_PTR(obj))
#define GetHandler(obj)		(Check_Type(obj, T_DATA), &(((struct mysql*)DATA_PTR(obj))->handler))
#define GetMysqlRes(obj)	(Check_Type(obj, T_DATA), ((struct mysql_res*)DATA_PTR(obj))->res)
#define GetMysqlStmt(obj)	(Check_Type(obj, T_DATA), ((struct mysql_stmt*)DATA_PTR(obj))->stmt)

void Init_nuodb(void)
{
  rb_raise(rb_eRuntimeError, "YOU ARE HERE!");
}
