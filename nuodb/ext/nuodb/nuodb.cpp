/*
 * NuoDB Adapter
 */

#include "nuodb/sqlapi/SqlEnvironment.h"
#include "nuodb/sqlapi/SqlConnection.h"
#include "nuodb/sqlapi/SqlDatabaseMetaData.h"
#include "nuodb/sqlapi/SqlStatement.h"
#include "nuodb/sqlapi/SqlExceptions.h"

using nuodb::sqlapi::ErrorCodeException;
using nuodb::sqlapi::SqlConnection;
using nuodb::sqlapi::SqlDatabaseMetaData;
using nuodb::sqlapi::SqlEnvironment;
using nuodb::sqlapi::SqlOption;
using nuodb::sqlapi::SqlOptionArray;
using nuodb::sqlapi::SqlStatement;

#include <ruby.h>

#include <iostream> // TODO temporary

using std::cout; // TODO temporary

//-------------------------------------------------------------------------
// Type definitions

class ClassType
{
public:
  VALUE type;
};

#define DECLARE_CLASS_TYPE(classname) \
  class classname : public ClassType \
  { public: void init(VALUE module); }

DECLARE_CLASS_TYPE(SqlEnvironmentType);
DECLARE_CLASS_TYPE(SqlConnectionType);
DECLARE_CLASS_TYPE(SqlDatabaseMetaDataType);
DECLARE_CLASS_TYPE(SqlStatementType);
DECLARE_CLASS_TYPE(SqlPreparedStatementType);
DECLARE_CLASS_TYPE(SqlResultSetType);
DECLARE_CLASS_TYPE(SqlColumnMetaDataType);

class AllTypes
{
  VALUE module;
public:
  SqlEnvironmentType env;
  SqlConnectionType con;
  SqlDatabaseMetaDataType dbmeta;
  SqlStatementType stmt;
  SqlPreparedStatementType pstmt;
  SqlResultSetType rset;
  SqlColumnMetaDataType colmeta;

  void init();
};

static AllTypes types;

//-------------------------------------------------------------------------

#define WRAPPER_CTOR(WT, RT)			\
  WT(RT& arg) : ref(arg) {}

#define WRAPPER_RELEASE(WT)						\
  static void release(WT* self) {					\
    self->ref.release();						\
    delete self;							\
  }

#define WRAPPER_AS_REF(WT, RT)			\
  static RT& asRef(VALUE value) {		\
    Check_Type(value, T_DATA);			\
    return ((WT*) DATA_PTR(value))->ref;	\
  }

#define WRAPPER_METHODS(WT, RT)			\
  WRAPPER_CTOR(WT, RT)				\
  WRAPPER_RELEASE(WT)				\
  WRAPPER_AS_REF(WT, RT)

class SqlDatabaseMetaDataWrapper
{
  SqlDatabaseMetaData& ref;

public:
  WRAPPER_METHODS(SqlDatabaseMetaDataWrapper, SqlDatabaseMetaData)

  static VALUE getDatabaseVersion(VALUE self)
  {
    return rb_str_new2(asRef(self).getDatabaseVersion());
  }
};

class SqlStatementWrapper
{
  SqlStatement& ref;
public:
  WRAPPER_METHODS(SqlStatementWrapper, SqlStatement)
};

class SqlConnectionWrapper
{
  SqlConnection& ref;

public:
  WRAPPER_METHODS(SqlConnectionWrapper, SqlConnection)

  static VALUE createStatement(VALUE self)
  {
    SqlStatementWrapper* w = new SqlStatementWrapper(asRef(self).createStatement());
    VALUE obj = Data_Wrap_Struct(types.stmt.type, 0, SqlStatementWrapper::release, w);
    rb_obj_call_init(obj, 0, 0);
    return obj;
  }

  static VALUE getMetaData(VALUE self)
  {
    SqlDatabaseMetaDataWrapper* w = new SqlDatabaseMetaDataWrapper(asRef(self).getMetaData());
    VALUE obj = Data_Wrap_Struct(types.dbmeta.type, 0, SqlDatabaseMetaDataWrapper::release, w);
    rb_obj_call_init(obj, 0, 0);
    return obj;
  }
};

class SqlEnvironmentWrapper
{
  SqlEnvironment& ref;

public:
  WRAPPER_METHODS(SqlEnvironmentWrapper, SqlEnvironment)

  static VALUE create(VALUE klass)
  {
    try {
      SqlOptionArray optsArray;
      optsArray.count = 0;

      SqlEnvironmentWrapper* w = new SqlEnvironmentWrapper(SqlEnvironment::createSqlEnvironment(&optsArray));
      VALUE obj = Data_Wrap_Struct(types.env.type, 0, SqlEnvironmentWrapper::release, w);
      rb_obj_call_init(obj, 0, 0);
      return obj;
    } catch (ErrorCodeException & e) {
      rb_raise(rb_eRuntimeError, "failed to create SqlEnvironment: %s", e.what().c_str());
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

      SqlConnectionWrapper* con = new SqlConnectionWrapper(asRef(self).createSqlConnection(&optsArray));
      VALUE obj = Data_Wrap_Struct(types.con.type, 0, SqlConnectionWrapper::release, con);
      rb_obj_call_init(obj, 0, 0);
      return obj;
    } catch (ErrorCodeException & e) {
      rb_raise(rb_eRuntimeError, "failed to create SqlConnection: %s", e.what().c_str());
    }
  }
};

//-------------------------------------------------------------------------
// Initialization

#define DEF_CLASS(name) \
  type = rb_define_class_under(module, name, rb_cObject)

#define DEF_SINGLETON(name, func, count) \
  rb_define_singleton_method(type, name, RUBY_METHOD_FUNC(func), count)

#define DEF_METHOD(name, func, count) \
  rb_define_method(type, name, RUBY_METHOD_FUNC(func), count)

void SqlEnvironmentType::init(VALUE module)
{
  DEF_CLASS("SqlEnvironment");
  DEF_SINGLETON("createSqlEnvironment", SqlEnvironmentWrapper::create, 0);
  DEF_METHOD("createSqlConnection", SqlEnvironmentWrapper::createSqlConnection, 3);
}

void SqlConnectionType::init(VALUE module)
{
  DEF_CLASS("SqlConnection");
  DEF_METHOD("createStatement", SqlConnectionWrapper::createStatement, 0);
  // TODO SqlStatement & createStatement();
  // TODO SqlPreparedStatement & createPreparedStatement(char const * sql);
  // TODO void setAutoCommit(bool autoCommit = true);
  // TODO bool hasAutoCommit() const;
  // TODO void commit();
  // TODO void rollback();
  DEF_METHOD("getMetaData", SqlConnectionWrapper::getMetaData, 0);
}

void SqlDatabaseMetaDataType::init(VALUE module)
{
  DEF_CLASS("SqlDatabaseMetaData");
  DEF_METHOD("getDatabaseVersion", SqlDatabaseMetaDataWrapper::getDatabaseVersion, 0);
}

void SqlStatementType::init(VALUE module)
{
  DEF_CLASS("SqlStatement");
  // TODO void execute(char const * sql);
  // TODO SqlResultSet & executeQuery(char const * sql);
  // TODO void release();
}

void SqlPreparedStatementType::init(VALUE module)
{
  DEF_CLASS("SqlPreparedStatement");
  // TODO void setInteger(size_t index, int32_t value);
  // TODO void setDouble(size_t index, double value);
  // TODO void setString(size_t index, char const * value);
  // TODO void execute();
  // TODO SqlResultSet & executeQuery();
  // TODO void release();
}

void SqlResultSetType::init(VALUE module)
{
  DEF_CLASS("SqlResultSet");
  // TODO bool next();
  // TODO size_t getColumnCount() const;
  // TODO SqlColumnMetaData & getMetaData(size_t column) const;
  // TODO int32_t getInteger(size_t column) const;
  // TODO double getDouble(size_t column) const;
  // TODO char const * getString(size_t column) const;
  // TODO SqlDate const * getDate(size_t column) const;
  // TODO void release();
}

void SqlColumnMetaDataType::init(VALUE module)
{
  DEF_CLASS("SqlColumnMetaData");
  // TODO char const * getColumnName() const;
  // TODO SqlType getType() const;
  // TODO void release();
}

void AllTypes::init()
{
  module = rb_define_module("Nuodb");
  env.init(module);
  con.init(module);
  dbmeta.init(module);
  stmt.init(module);
  pstmt.init(module);
  rset.init(module);
  colmeta.init(module);
}

extern "C"
void Init_nuodb(void)
{
  types.init();
}

//-------------------------------------------------------------------------
