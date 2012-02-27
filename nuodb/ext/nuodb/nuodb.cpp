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

using std::cout;

//-------------------------------------------------------------------------

class ClassDef
{
public:
  VALUE type;

protected:
  ClassDef(VALUE module, const char* name)
    : type(rb_define_class_under(module, name, rb_cObject))
  {
  }

  void singleton_method(char const* name, VALUE(*func)(...), int count)
  {
    rb_define_singleton_method(type, name, func, count);
  }

  void method(char const* name, VALUE(*func)(...), int count)
  {
    rb_define_method(type, name, func, count);
  }
};

class SqlEnvironmentDef : public ClassDef
{
public:
  SqlEnvironmentDef(VALUE module);
};

class SqlConnectionDef : public ClassDef
{
public:
  SqlConnectionDef(VALUE module);
};

class SqlDatabaseMetaDataDef : public ClassDef
{
public:
  SqlDatabaseMetaDataDef(VALUE module);
};

class AllTypes
{
  VALUE module;
public:
  SqlEnvironmentDef env;
  SqlConnectionDef con;
  SqlDatabaseMetaDataDef meta;

  AllTypes()
    : module(rb_define_module("Nuodb")),
      env(module),
      con(module),
      meta(module)
  {
  }
};

static AllTypes* types;

//-------------------------------------------------------------------------

class MetaDataWrapper
{
  SqlDatabaseMetaData& ref;

public:
  MetaDataWrapper(SqlDatabaseMetaData& arg) : ref(arg)
  {
  }

  static void free(MetaDataWrapper* self)
  {
    self->ref.release();
    delete self;
  }

  static VALUE getDatabaseVersion(VALUE selfArg)
  {
    Check_Type(selfArg, T_DATA);
    MetaDataWrapper* self = (MetaDataWrapper*) DATA_PTR(selfArg);

    return rb_str_new2(self->ref.getDatabaseVersion());
  }
};

class ConWrapper
{
  SqlConnection& ref;

public:
  ConWrapper(SqlConnection& arg) : ref(arg)
  {
  }

  static void free(ConWrapper* self)
  {
    self->ref.release();
    delete self;
  }

  static VALUE getMetaData(VALUE selfArg)
  {
    Check_Type(selfArg, T_DATA);
    ConWrapper* self = (ConWrapper*) DATA_PTR(selfArg);

    MetaDataWrapper* w = new MetaDataWrapper(self->ref.getMetaData());
    VALUE obj = Data_Wrap_Struct(types->meta.type, 0, MetaDataWrapper::free, w);
    rb_obj_call_init(obj, 0, 0);
    return obj;
  }
};

class EnvWrapper
{
  SqlEnvironment& ref;

public:
  EnvWrapper(SqlEnvironment& arg) : ref(arg)
  {
  }

  static VALUE create(VALUE klass, VALUE optsHash)
  {
    try {
      SqlOption options[3];

      options[0].option = "database";
      options[0].extra = (void*) "test@localhost";

      options[1].option = "user";
      options[1].extra = (void*) "cloud";

      options[2].option = "password";
      options[2].extra = (void*) "user";

      SqlOptionArray optsArray;
      optsArray.count = 3;
      optsArray.array = options;

      EnvWrapper* w = new EnvWrapper(SqlEnvironment::createSqlEnvironment(&optsArray));
      VALUE obj = Data_Wrap_Struct(types->env.type, 0, EnvWrapper::free, w);
      rb_obj_call_init(obj, 0, 0);
      return obj;
    } catch (ErrorCodeException & e) {
      rb_raise(rb_eRuntimeError, "failed to create SqlEnvironment: %s", e.what().c_str());
    }
  }

  static void free(EnvWrapper* self)
  {
    self->ref.release();
    delete self;
  }

  static VALUE createSqlConnection(VALUE env, VALUE optsHash)
  {
    try {
      Check_Type(env, T_DATA);
      EnvWrapper* wrapper = (EnvWrapper*) DATA_PTR(env);

      SqlOption options[3];

      options[0].option = "database";
      options[0].extra = (void*) "test@localhost";

      options[1].option = "user";
      options[1].extra = (void*) "cloud";

      options[2].option = "password";
      options[2].extra = (void*) "user";

      SqlOptionArray optsArray;
      optsArray.count = 3;
      optsArray.array = options;

      ConWrapper* w = new ConWrapper(wrapper->ref.createSqlConnection(&optsArray));
      VALUE obj = Data_Wrap_Struct(types->con.type, 0, ConWrapper::free, w);
      rb_obj_call_init(obj, 0, 0);
      return obj;
    } catch (ErrorCodeException & e) {
      rb_raise(rb_eRuntimeError, "failed to create SqlConnection: %s", e.what().c_str());
    }
  }
};

//-------------------------------------------------------------------------

SqlEnvironmentDef::SqlEnvironmentDef(VALUE module)
  : ClassDef(module, "SqlEnvironment")
{
  singleton_method("createSqlEnvironment", EnvWrapper::create, 1);
  method("createSqlConnection", EnvWrapper::createSqlConnection, 1);
}

SqlConnectionDef::SqlConnectionDef(VALUE module)
  : ClassDef(module, "SqlConnection")
{
  method("getMetaData", ConWrapper::getMetaData, 0);
}

SqlDatabaseMetaDataDef::SqlDatabaseMetaDataDef(VALUE module)
  : ClassDef(module, "SqlDatabaseMetaData")
{
  method("getDatabaseVersion", MetaDataWrapper::getDatabaseVersion, 0);
}

extern "C"
void Init_nuodb(void)
{
  types = new AllTypes();
}
