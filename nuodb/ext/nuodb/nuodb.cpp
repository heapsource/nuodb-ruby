/*
 * NuoDB Adapter
 */

#include "nuodb/sqlapi/SqlEnvironment.h"
#include "nuodb/sqlapi/SqlConnection.h"
#include "nuodb/sqlapi/SqlDatabaseMetaData.h"
#include "nuodb/sqlapi/SqlStatement.h"
#include "nuodb/sqlapi/SqlExceptions.h"

using nuodb::sqlapi::SqlOption;
using nuodb::sqlapi::SqlOptionArray;
using nuodb::sqlapi::SqlEnvironment;
using nuodb::sqlapi::SqlConnection;
using nuodb::sqlapi::SqlDatabaseMetaData;
using nuodb::sqlapi::ErrorCodeException;

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

class SqlEnvironmentType : public ClassType
{
public:
  void init(VALUE module);
};

class SqlConnectionType : public ClassType
{
public:
  void init(VALUE module);
};

class SqlDatabaseMetaDataType : public ClassType
{
public:
  void init(VALUE module);
};

class AllTypes
{
  VALUE module;
public:
  SqlEnvironmentType env;
  SqlConnectionType con;
  SqlDatabaseMetaDataType meta;

  void init();
};

static AllTypes types;

//-------------------------------------------------------------------------

#define WRAPPER_CTOR(WT, RT) \
  WT(RT& arg) : ref(arg) {}

#define WRAPPER_FREE(WT) \
  static void free(WT* self) { \
    self->ref.release(); \
    delete self; \
  }

#define WRAPPER_AS_REF(WT, RT) \
  static RT& asRef(VALUE value) { \
    Check_Type(value, T_DATA); \
    return ((WT*) DATA_PTR(value))->ref; \
  }

#define WRAPPER_METHODS(WT, RT) \
  WRAPPER_CTOR(WT, RT) \
  WRAPPER_FREE(WT) \
  WRAPPER_AS_REF(WT, RT)

class MetaDataWrapper
{
  SqlDatabaseMetaData& ref;

public:
  WRAPPER_METHODS(MetaDataWrapper, SqlDatabaseMetaData)

  static VALUE getDatabaseVersion(VALUE self)
  {
    return rb_str_new2(asRef(self).getDatabaseVersion());
  }
};

class ConWrapper
{
  SqlConnection& ref;

public:
  WRAPPER_METHODS(ConWrapper, SqlConnection)

  static VALUE getMetaData(VALUE self)
  {
    MetaDataWrapper* w = new MetaDataWrapper(asRef(self).getMetaData());
    VALUE obj = Data_Wrap_Struct(types.meta.type, 0, MetaDataWrapper::free, w);
    rb_obj_call_init(obj, 0, 0);
    return obj;
  }
};

class EnvWrapper
{
  SqlEnvironment& ref;

public:
  WRAPPER_METHODS(EnvWrapper, SqlEnvironment)

  static VALUE create(VALUE klass)
  {
    try {
      SqlOptionArray optsArray;
      optsArray.count = 0;

      EnvWrapper* w = new EnvWrapper(SqlEnvironment::createSqlEnvironment(&optsArray));
      VALUE obj = Data_Wrap_Struct(types.env.type, 0, EnvWrapper::free, w);
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

      ConWrapper* con = new ConWrapper(asRef(self).createSqlConnection(&optsArray));
      VALUE obj = Data_Wrap_Struct(types.con.type, 0, ConWrapper::free, con);
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
  DEF_SINGLETON("createSqlEnvironment", EnvWrapper::create, 0);
  DEF_METHOD("createSqlConnection", EnvWrapper::createSqlConnection, 3);
}

void SqlConnectionType::init(VALUE module)
{
  DEF_CLASS("SqlConnection");
  DEF_METHOD("getMetaData", ConWrapper::getMetaData, 0);
}

void SqlDatabaseMetaDataType::init(VALUE module)
{
  DEF_CLASS("SqlDatabaseMetaData");
  DEF_METHOD("getDatabaseVersion", MetaDataWrapper::getDatabaseVersion, 0);
}

void AllTypes::init()
{
  module = rb_define_module("Nuodb");
  env.init(module);
  con.init(module);
  meta.init(module);
}

extern "C"
void Init_nuodb(void)
{
  types.init();
}

//-------------------------------------------------------------------------
