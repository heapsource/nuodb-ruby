/*
 * NuoDB Adapter
 */

#include "nuodb/sqlapi/SqlEnvironment.h"
#include "nuodb/sqlapi/SqlConnection.h"
#include "nuodb/sqlapi/SqlStatement.h"
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

#include <ruby.h>

#include <iostream> // TODO temporary

using std::cout; // TODO temporary

//------------------------------------------------------------------------------
// class building macros

#define WRAPPER_FIELDS(RT)			\
  private: static VALUE type;			\
  RT& ref;

#define WRAPPER_METHODS(WT, RT)					\
  public:							\
  WT(RT& arg) : ref(arg) {}					\
  static VALUE getType() { return type; }			\
  static void release(WT* self) {				\
    cout << "DISABLED release " << #WT << " " << self << "\n";	\
    /* self->ref.release(); */					\
    delete self;						\
  }								\
  static RT& asRef(VALUE value) {				\
    Check_Type(value, T_DATA);					\
    return ((WT*) DATA_PTR(value))->ref;			\
  }

#define WRAPPER_DEFINITION(WT)			\
  VALUE WT::type = 0;

//------------------------------------------------------------------------------
// utility function macros

#define RETURN_WRAPPER(WT, func)				      \
  WT* w = new WT(func);						      \
  VALUE obj = Data_Wrap_Struct(WT::getType(), 0, WT::release, w);     \
  rb_obj_call_init(obj, 0, 0);					      \
  return obj

#define INIT_TYPE(name)						\
  type = rb_define_class_under(module, name, rb_cObject)

#define DEFINE_SINGLE(name, func, count)				\
  rb_define_singleton_method(type, name, RUBY_METHOD_FUNC(func), count)

#define DEFINE_METHOD(name, func, count)			\
  rb_define_method(type, name, RUBY_METHOD_FUNC(func), count)

//------------------------------------------------------------------------------

class SqlDatabaseMetaDataWrapper
{
  WRAPPER_FIELDS(SqlDatabaseMetaData)
  WRAPPER_METHODS(SqlDatabaseMetaDataWrapper, SqlDatabaseMetaData)

  static void init(VALUE module)
  {
    INIT_TYPE("SqlDatabaseMetaData");
    DEFINE_METHOD("getDatabaseVersion", getDatabaseVersion, 0);
  }

  static VALUE getDatabaseVersion(VALUE self)
  {
    return rb_str_new2(asRef(self).getDatabaseVersion());
  }
};
WRAPPER_DEFINITION(SqlDatabaseMetaDataWrapper)

//------------------------------------------------------------------------------

class SqlStatementWrapper
{
  WRAPPER_FIELDS(SqlStatement)
  WRAPPER_METHODS(SqlStatementWrapper, SqlStatement)

  static void init(VALUE module)
  {
    INIT_TYPE("SqlStatement");
    // TODO void execute(char const * sql);
    // TODO SqlResultSet & executeQuery(char const * sql);
    // TODO void release();
  }
};
WRAPPER_DEFINITION(SqlStatementWrapper)

//------------------------------------------------------------------------------

class SqlPreparedStatementWrapper
{
  WRAPPER_FIELDS(SqlPreparedStatement)
  WRAPPER_METHODS(SqlPreparedStatementWrapper, SqlPreparedStatement)

  static void init(VALUE module)
  {
    INIT_TYPE("SqlPreparedStatement");
    // TODO void setInteger(size_t index, int32_t value);
    // TODO void setDouble(size_t index, double value);
    // TODO void setString(size_t index, char const * value);
    // TODO void execute();
    // TODO SqlResultSet & executeQuery();
    // TODO void release();
  }
};
WRAPPER_DEFINITION(SqlPreparedStatementWrapper)

//------------------------------------------------------------------------------

class SqlResultSetWrapper
{
  WRAPPER_FIELDS(SqlResultSet);
  WRAPPER_METHODS(SqlResultSetWrapper, SqlResultSet);

  static void init(VALUE module)
  {
    INIT_TYPE("SqlResultSet");
    // TODO bool next();
    // TODO size_t getColumnCount() const;
    // TODO SqlColumnMetaData & getMetaData(size_t column) const;
    // TODO int32_t getInteger(size_t column) const;
    // TODO double getDouble(size_t column) const;
    // TODO char const * getString(size_t column) const;
    // TODO SqlDate const * getDate(size_t column) const;
    // TODO void release();
  }
};
WRAPPER_DEFINITION(SqlResultSetWrapper)

//------------------------------------------------------------------------------

class SqlColumnMetaDataWrapper
{
  WRAPPER_FIELDS(SqlColumnMetaData);
  WRAPPER_METHODS(SqlColumnMetaDataWrapper, SqlColumnMetaData);

  static void init(VALUE module)
  {
    INIT_TYPE("SqlColumnMetaData");
    // TODO char const * getColumnName() const;
    // TODO SqlType getType() const;
    // TODO void release();
  }
};
WRAPPER_DEFINITION(SqlColumnMetaDataWrapper)

//------------------------------------------------------------------------------

class SqlConnectionWrapper
{
  WRAPPER_FIELDS(SqlConnection)
  WRAPPER_METHODS(SqlConnectionWrapper, SqlConnection)

  static void init(VALUE module)
  {
    INIT_TYPE("SqlConnection");
    DEFINE_METHOD("createStatement", createStatement, 0);
    DEFINE_METHOD("createPreparedStatement", createPreparedStatement, 1);
    // TODO void setAutoCommit(bool autoCommit = true);
    // TODO bool hasAutoCommit() const;
    // TODO void commit();
    // TODO void rollback();
    DEFINE_METHOD("getMetaData", getMetaData, 0);
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

  static VALUE getMetaData(VALUE self)
  {
    RETURN_WRAPPER(SqlDatabaseMetaDataWrapper, asRef(self).getMetaData());
  }
};
WRAPPER_DEFINITION(SqlConnectionWrapper)

//------------------------------------------------------------------------------

class SqlEnvironmentWrapper
{
  WRAPPER_FIELDS(SqlEnvironment)
  WRAPPER_METHODS(SqlEnvironmentWrapper, SqlEnvironment)

  static void init(VALUE module)
  {
    INIT_TYPE("SqlEnvironment");
    DEFINE_SINGLE("createSqlEnvironment", create, 0);
    DEFINE_METHOD("createSqlConnection", createSqlConnection, 3);
  }

  static VALUE create(VALUE klass)
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
