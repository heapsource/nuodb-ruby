/*
 * NuoDB Adapter
 */

// TODO it would be better to do this in extconf.rb
#if !defined(WIN32) && !defined(_WIN32)
#define HAVE_STDINT_H 1
#endif

#include "nuodb/sqlapi/SqlResultSet.h"
#include "nuodb/sqlapi/SqlEnvironment.h"
#include "nuodb/sqlapi/SqlConnection.h"
#include "nuodb/sqlapi/SqlStatement.h"
#include "nuodb/sqlapi/SqlPreparedStatement.h"
#include "nuodb/sqlapi/SqlDatabaseMetaData.h"
#include "nuodb/sqlapi/SqlColumnMetaData.h"
#include "nuodb/sqlapi/SqlExceptions.h"

using nuodb::sqlapi::ErrorCodeException;
using nuodb::sqlapi::SqlColumnMetaData;
using nuodb::sqlapi::SqlConnection;
using nuodb::sqlapi::SqlDatabaseMetaData;
using nuodb::sqlapi::SqlEnvironment;
using nuodb::sqlapi::SqlOption;
using nuodb::sqlapi::SqlOptionArray;
using nuodb::sqlapi::SqlPreparedStatement;
using nuodb::sqlapi::SqlResultSet;
using nuodb::sqlapi::SqlStatement;
using nuodb::sqlapi::SqlType;
using nuodb::sqlapi::SQL_INTEGER;
using nuodb::sqlapi::SQL_DOUBLE;
using nuodb::sqlapi::SQL_STRING;
using nuodb::sqlapi::SQL_DATE;
using nuodb::sqlapi::SQL_TIME;
using nuodb::sqlapi::SQL_DATETIME;

#include <ruby.h>

#include <iostream> // TODO temporary

using std::cout; // TODO temporary

//------------------------------------------------------------------------------
// class building macros

#define WRAPPER_COMMON(WT, RT)					\
  private:							\
  static VALUE type;						\
  RT& ref;							\
public:								\
 WT(RT& arg) : ref(arg) {}					\
 static VALUE getRubyType() { return type; }			\
 static void release(WT* self) {				\
   cout << "DISABLED release " << #WT << " " << self << "\n";	\
   /* self->ref.release(); */					\
   delete self;							\
 }								\
 static RT& asRef(VALUE value) {				\
   Check_Type(value, T_DATA);					\
   return ((WT*) DATA_PTR(value))->ref;				\
 }

#define WRAPPER_DEFINITION(WT)			\
  VALUE WT::type = 0;

//------------------------------------------------------------------------------
// utility function macros

#define RETURN_WRAPPER(WT, func)					\
  WT* w = new WT(func);							\
  VALUE obj = Data_Wrap_Struct(WT::getRubyType(), 0, WT::release, w);	\
  rb_obj_call_init(obj, 0, 0);						\
  return obj

#define SYMBOL_OF(value)			\
  ID2SYM(rb_intern(#value))

#define INIT_TYPE(name)						\
  type = rb_define_class_under(module, name, rb_cObject)

#define DEFINE_SINGLE(func, count)					\
  rb_define_singleton_method(type, #func, RUBY_METHOD_FUNC(func), count)

#define DEFINE_METHOD(func, count)				\
  rb_define_method(type, #func, RUBY_METHOD_FUNC(func), count)

//------------------------------------------------------------------------------

class SqlDatabaseMetaDataWrapper
{
  WRAPPER_COMMON(SqlDatabaseMetaDataWrapper, SqlDatabaseMetaData)

  static void init(VALUE module)
  {
    INIT_TYPE("SqlDatabaseMetaData");
    DEFINE_METHOD(getDatabaseVersion, 0);
  }

  static VALUE getDatabaseVersion(VALUE self)
  {
    return rb_str_new2(asRef(self).getDatabaseVersion());
  }
};

WRAPPER_DEFINITION(SqlDatabaseMetaDataWrapper)

//------------------------------------------------------------------------------

class SqlColumnMetaDataWrapper
{
  WRAPPER_COMMON(SqlColumnMetaDataWrapper, SqlColumnMetaData)

  static void init(VALUE module)
  {
    INIT_TYPE("SqlColumnMetaData");
    DEFINE_METHOD(getColumnName, 0);
    DEFINE_METHOD(getType, 0);
  }

  static VALUE getColumnName(VALUE self)
  {
    return rb_str_new2(asRef(self).getColumnName());
  }

  static VALUE getType(VALUE self)
  {
    SqlType t = asRef(self).getType();
    switch(t)
      {
      case SQL_INTEGER:
	return SYMBOL_OF(SQL_INTEGER);
      case SQL_DOUBLE:
	return SYMBOL_OF(SQL_DOUBLE);
      case SQL_STRING:
	return SYMBOL_OF(SQL_STRING);
      case SQL_DATE:
	return SYMBOL_OF(SQL_DATE);
      case SQL_TIME:
	return SYMBOL_OF(SQL_TIME);
      case SQL_DATETIME:
	return SYMBOL_OF(SQL_DATETIME);
      default:
	rb_raise(rb_eRuntimeError, "invalid sql type: %d", t);
      }
  }
};

WRAPPER_DEFINITION(SqlColumnMetaDataWrapper)

//------------------------------------------------------------------------------

class SqlResultSetWrapper
{
  WRAPPER_COMMON(SqlResultSetWrapper, SqlResultSet)

  static void init(VALUE module)
  {
    INIT_TYPE("SqlResultSet");
    DEFINE_METHOD(next, 0);
    DEFINE_METHOD(getColumnCount, 0);
    DEFINE_METHOD(getMetaData, 1);
    DEFINE_METHOD(getInteger, 1);
    DEFINE_METHOD(getDouble, 1);
    DEFINE_METHOD(getString, 1);
    DEFINE_METHOD(getDate, 1);
  }

  static VALUE next(VALUE self)
  {
    return asRef(self).next() ? Qtrue: Qfalse;
  }

  static VALUE getColumnCount(VALUE self)
  {
    return UINT2NUM(asRef(self).getColumnCount());
  }

  static VALUE getMetaData(VALUE self, VALUE columnValue)
  {
    size_t column = NUM2UINT(columnValue);
    RETURN_WRAPPER(SqlColumnMetaDataWrapper, asRef(self).getMetaData(column));
  }

  static VALUE getInteger(VALUE self, VALUE columnValue)
  {
    size_t column = NUM2UINT(columnValue);
    return INT2NUM(asRef(self).getInteger(column));
  }

  static VALUE getDouble(VALUE self, VALUE columnValue)
  {
    size_t column = NUM2UINT(columnValue);
    return rb_float_new(asRef(self).getDouble(column));
  }

  static VALUE getString(VALUE self, VALUE columnValue)
  {
    size_t column = NUM2UINT(columnValue);
    return rb_str_new2(asRef(self).getString(column));
  }

  static VALUE getDate(VALUE self, VALUE columnValue)
  {
    // TODO SqlDate const * getDate(size_t column) const;
    size_t column = NUM2UINT(columnValue);
    return Qnil;
  }
};

WRAPPER_DEFINITION(SqlResultSetWrapper)

//------------------------------------------------------------------------------

class SqlStatementWrapper
{
  WRAPPER_COMMON(SqlStatementWrapper, SqlStatement)

  static void init(VALUE module)
  {
    INIT_TYPE("SqlStatement");
    DEFINE_METHOD(execute, 1);
    DEFINE_METHOD(executeQuery, 1);
  }

  static VALUE execute(VALUE self, VALUE sqlValue)
  {
    const char* sql = StringValuePtr(sqlValue);
    asRef(self).execute(sql);
    return Qnil;
  }

  static VALUE executeQuery(VALUE self, VALUE sqlValue)
  {
    const char* sql = StringValuePtr(sqlValue);
    RETURN_WRAPPER(SqlResultSetWrapper, asRef(self).executeQuery(sql));
  }
};

WRAPPER_DEFINITION(SqlStatementWrapper)

//------------------------------------------------------------------------------

class SqlPreparedStatementWrapper
{
  WRAPPER_COMMON(SqlPreparedStatementWrapper, SqlPreparedStatement)

  static void init(VALUE module)
  {
    INIT_TYPE("SqlPreparedStatement");
    DEFINE_METHOD(setInteger, 2);
    DEFINE_METHOD(setDouble, 2);
    DEFINE_METHOD(setString, 2);
    DEFINE_METHOD(execute, 0);
    DEFINE_METHOD(executeQuery, 0);
  }

  static VALUE setInteger(VALUE self, VALUE indexValue, VALUE valueValue)
  {
    size_t index = NUM2UINT(indexValue);
    int32_t value = NUM2INT(valueValue);
    asRef(self).setInteger(index, value);
    return Qnil;
  }

  static VALUE setDouble(VALUE self, VALUE indexValue, VALUE valueValue)
  {
    size_t index = NUM2UINT(indexValue);
    double value = NUM2DBL(valueValue);
    asRef(self).setDouble(index, value);
    return Qnil;
  }

  static VALUE setString(VALUE self, VALUE indexValue, VALUE valueValue)
  {
    size_t index = NUM2UINT(indexValue);
    char const* value = RSTRING_PTR(valueValue);
    asRef(self).setString(index, value);
    return Qnil;
  }

  static VALUE execute(VALUE self)
  {
    asRef(self).execute();
    return Qnil;
  }

  static VALUE executeQuery(VALUE self)
  {
    RETURN_WRAPPER(SqlResultSetWrapper, asRef(self).executeQuery());
  }
};

WRAPPER_DEFINITION(SqlPreparedStatementWrapper)

//------------------------------------------------------------------------------

class SqlConnectionWrapper
{
  WRAPPER_COMMON(SqlConnectionWrapper, SqlConnection)

  static void init(VALUE module)
  {
    INIT_TYPE("SqlConnection");
    DEFINE_METHOD(createStatement, 0);
    DEFINE_METHOD(createPreparedStatement, 1);
    DEFINE_METHOD(setAutoCommit, 1);
    DEFINE_METHOD(hasAutoCommit, 0);
    DEFINE_METHOD(commit, 0);
    DEFINE_METHOD(rollback, 0);
    DEFINE_METHOD(getMetaData, 0);
  }

  static VALUE createStatement(VALUE self)
  {
    RETURN_WRAPPER(SqlStatementWrapper, asRef(self).createStatement());
  }

  static VALUE createPreparedStatement(VALUE self, VALUE sqlValue)
  {
    const char* sql = StringValuePtr(sqlValue);
    RETURN_WRAPPER(SqlPreparedStatementWrapper, asRef(self).createPreparedStatement(sql));
  }

  static VALUE setAutoCommit(VALUE self, VALUE autoCommitValue)
  {
    bool autoCommit = NUM2INT(autoCommitValue);
    asRef(self).setAutoCommit(autoCommit);
    return Qnil;
  }

  static VALUE hasAutoCommit(VALUE self)
  {
    return INT2NUM(asRef(self).hasAutoCommit());
  }

  static VALUE commit(VALUE self)
  {
    asRef(self).commit();
    return Qnil;
  }

  static VALUE rollback(VALUE self)
  {
    asRef(self).rollback();
    return Qnil;
  }

  static VALUE getMetaData(VALUE self)
  {
    RETURN_WRAPPER(SqlDatabaseMetaDataWrapper, asRef(self).getMetaData());
  }
};

WRAPPER_DEFINITION(SqlConnectionWrapper)

//------------------------------------------------------------------------------

class SqlEnvironmentWrapper
{
  WRAPPER_COMMON(SqlEnvironmentWrapper, SqlEnvironment)

  static void init(VALUE module)
  {
    INIT_TYPE("SqlEnvironment");
    DEFINE_SINGLE(createSqlEnvironment, 0);
    DEFINE_METHOD(createSqlConnection, 3);
  }

  static VALUE createSqlEnvironment(VALUE klass)
  {
    try {
      SqlOptionArray optsArray;
      optsArray.count = 0;

      RETURN_WRAPPER(SqlEnvironmentWrapper, SqlEnvironment::createSqlEnvironment(&optsArray));
    } catch (ErrorCodeException & e) {
      rb_raise(rb_eRuntimeError, "failed to create SqlEnvironment: %s", e.what());
    }
  }

  static VALUE createSqlConnection(VALUE self, VALUE database, VALUE username, VALUE password)
  {
    try {
      SqlOption options[3];

      options[0].option = "database";
      options[0].extra = (void*) StringValuePtr(database);

      options[1].option = "user";
      options[1].extra = (void*) StringValuePtr(username);

      options[2].option = "password";
      options[2].extra = (void*) StringValuePtr(password);

      SqlOptionArray optsArray;
      optsArray.count = 3;
      optsArray.array = options;

      RETURN_WRAPPER(SqlConnectionWrapper, asRef(self).createSqlConnection(&optsArray));
    } catch (ErrorCodeException & e) {
      rb_raise(rb_eRuntimeError, "failed to create SqlConnection: %s", e.what());
    }
  }
};

WRAPPER_DEFINITION(SqlEnvironmentWrapper)

//------------------------------------------------------------------------------

extern "C"
void Init_nuodb(void)
{
  VALUE module = rb_define_module("Nuodb");
  SqlEnvironmentWrapper::init(module);
  SqlConnectionWrapper::init(module);
  SqlDatabaseMetaDataWrapper::init(module);
  SqlStatementWrapper::init(module);
  SqlPreparedStatementWrapper::init(module);
  SqlResultSetWrapper::init(module);
  SqlColumnMetaDataWrapper::init(module);
}

//------------------------------------------------------------------------------
