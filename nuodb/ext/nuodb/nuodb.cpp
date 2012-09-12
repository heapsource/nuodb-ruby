/****************************************************************************
* Copyright (c)2012, NuoDB, Inc.
* All rights reserved.
*
* Redistribution and use in source and binary forms, with or without
* modification, are permitted provided that the following conditions are met:
*
*   * Redistributions of source code must retain the above copyright
*       notice, this list of conditions and the following disclaimer.
*   * Redistributions in binary form must reproduce the above copyright
*       notice, this list of conditions and the following disclaimer in the
*       documentation and/or other materials provided with the distribution.
*   * Neither the name of NuoDB, Inc. nor the names of its contributors may
*       be used to endorse or promote products derived from this software
*       without specific prior written permission.
*
* THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
* ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
* WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
* DISCLAIMED. IN NO EVENT SHALL NUODB, INC. BE LIABLE FOR ANY DIRECT, INDIRECT,
* INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
* LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA,
* OR PROFITS; OR BUSINESS INTERRUPTION)HOWEVER CAUSED AND ON ANY THEORY OF
* LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
* OR OTHERWISE)ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
* ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
****************************************************************************/

/*
 * NuoDB Adapter
 */

#include <ruby.h>
#include <time.h>
#include <stdio.h>

extern "C" struct timeval rb_time_timeval(VALUE time);

//------------------------------------------------------------------------------
// class building macros

#define WRAPPER_COMMON(WT, RT)                                          \
    private:                                                            \
    static VALUE type;                                                  \
    RT* ptr;                                                            \
public:                                                                 \
    WT(RT* arg): ptr(arg) {}                                            \
    static VALUE getRubyType() { return type; }                         \
    static void release(WT* self)                                       \
{                                                                       \
    /*delete self->ptr;*/                                               \
    delete self;                                                        \
}                                                                       \
    static RT* asPtr(VALUE value)                                       \
{                                                                       \
    Check_Type(value, T_DATA);                                          \
    return ((WT*)DATA_PTR(value))->ptr;                                 \
}                                                                       \
    static VALUE wrap(RT* value)                                        \
{                                                                       \
    if (value == NULL) return Qnil;                                     \
    WT* w = new WT(value);                                              \
    VALUE obj = Data_Wrap_Struct(WT::getRubyType(), 0, WT::release, w); \
    rb_obj_call_init(obj, 0, 0);                                        \
    return obj;                                                         \
}

#define WRAPPER_DEFINITION(WT)                                          \
    VALUE WT::type = 0;

#define AS_QBOOL(value)((value)? Qtrue : Qfalse)

//------------------------------------------------------------------------------
// utility function macros

#define SYMBOL_OF(value)                                                \
    ID2SYM(rb_intern(#value))

#define INIT_TYPE(name)                                                 \
    type = rb_define_class_under(module, name, rb_cObject)

#define DEFINE_SINGLE(func, count)                                      \
    rb_define_singleton_method(type, #func, RUBY_METHOD_FUNC(func), count)

#define DEFINE_METHOD(func, count)                                      \
    rb_define_method(type, #func, RUBY_METHOD_FUNC(func), count)

//------------------------------------------------------------------------------
// exception mapper

static VALUE m_nuodb, c_nuodb_error;
static ID c_error_code_assignment;

static void rb_raise_nuodb_error(int code, const char * fmt, ...)
{
    va_list args;
    char text[BUFSIZ];

    va_start(args, fmt);
    vsnprintf(text, BUFSIZ, fmt, args);
    va_end(args);

    VALUE error = rb_exc_new2(c_nuodb_error, text);
    rb_funcall(error, c_error_code_assignment, 1, UINT2NUM(code));
    rb_exc_raise(error);
}

//------------------------------------------------------------------------------

#include "Connection.h"
#include "BigDecimal.h"
#include "Blob.h"
#include "Bytes.h"
#include "CallableStatement.h"
#include "Clob.h"
#include "DatabaseMetaData.h"
#include "DateClass.h"
#include "ParameterMetaData.h"
#include "PreparedStatement.h"
#include "Properties.h"
#include "ResultList.h"
#include "ResultSet.h"
#include "ResultSetMetaData.h"
#include "SQLException.h"
#include "Savepoint.h"
#include "Statement.h"
#include "TimeClass.h"
#include "Timestamp.h"
#include "SqlTimestamp.h"
#include "SqlDate.h"
#include "SqlTime.h"

using namespace NuoDB;

class WrapDatabaseMetaData
{
    WRAPPER_COMMON(WrapDatabaseMetaData, DatabaseMetaData)

    static void init(VALUE module);
    static VALUE getDatabaseVersion(VALUE self);
    static VALUE getIndexInfo(VALUE self, VALUE schemaValue, VALUE tableValue, VALUE uniqueValue, VALUE approxValue);
    static VALUE getColumns(VALUE self, VALUE schemaValue, VALUE tablePattern);
    static VALUE getTables(VALUE self, VALUE schemaValue, VALUE table);
};

WRAPPER_DEFINITION(WrapDatabaseMetaData);

//------------------------------------------------------------------------------

class WrapResultSetMetaData
{
    WRAPPER_COMMON(WrapResultSetMetaData, ResultSetMetaData);

    static void init(VALUE module);
    static VALUE getColumnCount(VALUE self);
    static VALUE getColumnName(VALUE self, VALUE columnValue);
    static VALUE getScale(VALUE self, VALUE columnValue);
    static VALUE getPrecision(VALUE self, VALUE columnValue);
    static VALUE isNullable(VALUE self, VALUE columnValue);
    static VALUE getColumnTypeName(VALUE self, VALUE columnValue);
    static VALUE getType(VALUE self, VALUE columnValue);
};

WRAPPER_DEFINITION(WrapResultSetMetaData);

//------------------------------------------------------------------------------

class WrapResultSet
{
    WRAPPER_COMMON(WrapResultSet, ResultSet)

    static void init(VALUE module);
    static VALUE next(VALUE self);
    static VALUE getColumnCount(VALUE self);
    static VALUE getMetaData(VALUE self);
    static VALUE getBoolean(VALUE self, VALUE columnValue);
    static VALUE getInteger(VALUE self, VALUE columnValue);
    static VALUE getDouble(VALUE self, VALUE columnValue);
    static VALUE getString(VALUE self, VALUE columnValue);
    static VALUE getDate(VALUE self, VALUE columnValue);
    static VALUE getTime(VALUE self, VALUE columnValue);
    static VALUE getTimestamp(VALUE self, VALUE columnValue);
    static VALUE getChar(VALUE self, VALUE columnValue);
};

WRAPPER_DEFINITION(WrapResultSet)

//------------------------------------------------------------------------------

class WrapStatement
{
    WRAPPER_COMMON(WrapStatement, Statement);

    static void init(VALUE module);
    static VALUE close(VALUE self);
    static VALUE execute(VALUE self, VALUE sqlValue);
    static VALUE executeQuery(VALUE self, VALUE sqlValue);
    static VALUE executeUpdate(VALUE self, VALUE sqlValue);
    static VALUE getResultSet(VALUE self);
    static VALUE getUpdateCount(VALUE self);
    static VALUE getGeneratedKeys(VALUE self);
};

WRAPPER_DEFINITION(WrapStatement);

//------------------------------------------------------------------------------

class WrapPreparedStatement
{
    WRAPPER_COMMON(WrapPreparedStatement, PreparedStatement);

    static void init(VALUE module);
    static VALUE close(VALUE self);
    static VALUE setBoolean(VALUE self, VALUE indexValue, VALUE valueValue);
    static VALUE setInteger(VALUE self, VALUE indexValue, VALUE valueValue);
    static VALUE setDouble(VALUE self, VALUE indexValue, VALUE valueValue);
    static VALUE setString(VALUE self, VALUE indexValue, VALUE valueValue);
    static VALUE setTime(VALUE self, VALUE indexValue, VALUE valueValue);
    static VALUE execute(VALUE self);
    static VALUE executeQuery(VALUE self);
    static VALUE executeUpdate(VALUE self);
    static VALUE getResultSet(VALUE self);
    static VALUE getUpdateCount(VALUE self);
    static VALUE getGeneratedKeys(VALUE self);
};

WRAPPER_DEFINITION(WrapPreparedStatement);

//------------------------------------------------------------------------------

class WrapConnection
{
    WRAPPER_COMMON(WrapConnection, Connection);

    static void init(VALUE module);
    static VALUE close(VALUE self);
    static VALUE ping(VALUE self);
    static VALUE createStatement(VALUE self);
    static VALUE createPreparedStatement(VALUE self, VALUE sqlValue);
    static VALUE setAutoCommit(VALUE self, VALUE autoCommitValue);
    static VALUE hasAutoCommit(VALUE self);
    static VALUE commit(VALUE self);
    static VALUE rollback(VALUE self);
    static VALUE getMetaData(VALUE self);
    static VALUE getSchema(VALUE self);
    static VALUE createSqlConnection(VALUE self,
        VALUE databaseValue,
        VALUE schemaValue,
        VALUE usernameValue,
        VALUE passwordValue);
};

WRAPPER_DEFINITION(WrapConnection);

//------------------------------------------------------------------------------

void WrapDatabaseMetaData::init(VALUE module)
{
    INIT_TYPE("DatabaseMetaData");
    DEFINE_METHOD(getDatabaseVersion, 0);
    DEFINE_METHOD(getIndexInfo, 4);
    DEFINE_METHOD(getColumns, 2);
    DEFINE_METHOD(getTables, 2);
}

VALUE WrapDatabaseMetaData::getDatabaseVersion(VALUE self)
{
    try
    {
        return rb_str_new2(asPtr(self)->getDatabaseProductVersion());
    }
    catch (SQLException & e)
    {
        rb_raise_nuodb_error(e.getSqlcode(), "Get database metadata database version failed: %s", e.getText());
    }
}

VALUE WrapDatabaseMetaData::getIndexInfo(VALUE self, VALUE schemaValue, VALUE tableValue, VALUE uniqueValue, VALUE approxValue)
{
    const char* schema = StringValuePtr(schemaValue);
    const char* table = StringValuePtr(tableValue);
    bool unique = !(RB_TYPE_P(uniqueValue, T_FALSE) || RB_TYPE_P(uniqueValue, T_NIL));
    bool approx = !(RB_TYPE_P(approxValue, T_FALSE) || RB_TYPE_P(approxValue, T_NIL));
    try
    {
        return WrapResultSet::wrap(asPtr(self)->getIndexInfo(NULL, schema, table, unique, approx));
    }
    catch (SQLException & e)
    {
        rb_raise_nuodb_error(e.getSqlcode(), "Get database metadata index info failed: %s", e.getText());
    }
}

VALUE WrapDatabaseMetaData::getColumns(VALUE self, VALUE schemaValue, VALUE tableValue)
{
    const char* schema = StringValuePtr(schemaValue);
    const char* table = StringValuePtr(tableValue);
    try
    {
        return WrapResultSet::wrap(asPtr(self)->getColumns(NULL, schema, table, NULL));
    }
    catch (SQLException & e)
    {
        rb_raise_nuodb_error(e.getSqlcode(), "Get columns failed: %s", e.getText());
    }
}

VALUE WrapDatabaseMetaData::getTables(VALUE self, VALUE schemaValue, VALUE tableValue)
{
    const char* schema = StringValuePtr(schemaValue);
    const char* table = StringValuePtr(tableValue);

    try
    {
        return WrapResultSet::wrap(asPtr(self)->getTables(NULL, schema, table, 0, NULL));
    }
    catch (SQLException & e)
    {
        rb_raise_nuodb_error(e.getSqlcode(), "Get tables failed: %s", e.getText());
    }
}

//------------------------------------------------------------------------------

void WrapResultSet::init(VALUE module)
{
    INIT_TYPE("ResultSet");
    DEFINE_METHOD(next, 0);
    DEFINE_METHOD(getMetaData, 0);
    DEFINE_METHOD(getBoolean, 1);
    DEFINE_METHOD(getInteger, 1);
    DEFINE_METHOD(getDouble, 1);
    DEFINE_METHOD(getString, 1);
    DEFINE_METHOD(getDate, 1);
    DEFINE_METHOD(getTime, 1);
    DEFINE_METHOD(getTimestamp, 1);
    DEFINE_METHOD(getChar, 1);
}

VALUE WrapResultSet::next(VALUE self)
{
    try
    {
        return AS_QBOOL(asPtr(self)->next());
    }
    catch (SQLException & e)
    {
        rb_raise_nuodb_error(e.getSqlcode(), "Failed to move the cursor forward one row: %s", e.getText());
    }
}

VALUE WrapResultSet::getMetaData(VALUE self)
{
    try
    {
        return WrapResultSetMetaData::wrap(asPtr(self)->getMetaData());
    }
    catch (SQLException & e)
    {
        rb_raise_nuodb_error(e.getSqlcode(), "Failed to retrieve number, types, and properties of the result set: %s", e.getText());
    }
}

VALUE WrapResultSet::getBoolean(VALUE self, VALUE columnValue)
{
    int column = NUM2UINT(columnValue);
    try
    {
        bool value = asPtr(self)->getBoolean(column);
        return AS_QBOOL(value);
    }
    catch (SQLException & e)
    {
        rb_raise_nuodb_error(e.getSqlcode(), "Failed to get boolean value for the designated column (%d): %s", column, e.getText());
    }
}

VALUE WrapResultSet::getInteger(VALUE self, VALUE columnValue)
{
    int column = NUM2UINT(columnValue);
    try
    {
        return INT2NUM(asPtr(self)->getInt(column));
    }
    catch (SQLException & e)
    {
        rb_raise_nuodb_error(e.getSqlcode(), "Failed to get integer value for the designated column (%d): %s", column, e.getText());
    }
}

VALUE WrapResultSet::getDouble(VALUE self, VALUE columnValue)
{
    int column = NUM2UINT(columnValue);
    try
    {
        return rb_float_new(asPtr(self)->getDouble(column));
    }
    catch (SQLException & e)
    {
        rb_raise_nuodb_error(e.getSqlcode(), "Failed to get double value for the designated column (%d): %s", column, e.getText());
    }
}

VALUE WrapResultSet::getString(VALUE self, VALUE columnValue)
{
    int column = NUM2UINT(columnValue);
    try
    {
        return rb_str_new2(asPtr(self)->getString(column));
    }
    catch (SQLException & e)
    {
        rb_raise_nuodb_error(e.getSqlcode(), "Failed to get string value for the designated column (%d): %s", column, e.getText());
    }
}
VALUE WrapResultSet::getDate(VALUE self, VALUE columnValue)
{
    int column = NUM2UINT(columnValue);
    try
    {
        return rb_str_new2(asPtr(self)->getString(column));
        //Date* date = asPtr(self)->getDate(column);
        //SqlDate timestamp(date->getMilliseconds());
        //struct tm utc;
        //timestamp.getDate(&utc);
        //char buffer[250];
        //::strftime(buffer,sizeof(buffer),"%Y%m%dT%I%M%S+0000",&utc);
        //return rb_str_new2(buffer);
    }
    catch (SQLException & e)
    {
        rb_raise_nuodb_error(e.getSqlcode(), "Failed to get date value for the designated column (%d): %s", column, e.getText());
    }
}

VALUE WrapResultSet::getTime(VALUE self, VALUE columnValue)
{
    int column = NUM2UINT(columnValue);
    try
    {
        return rb_str_new2(asPtr(self)->getString(column));
        //Time* time = asPtr(self)->getTime(column);
        //SqlTime timestamp(time->getMilliseconds());
        //struct tm utc;
        //timestamp.getTime(&utc);
        //char buffer[250];
        //::strftime(buffer,sizeof(buffer),"%Y%m%dT%I%M%S+0000",&utc);
        // Workaround until we fix the Date/Time support
        // Convert time to local time?
        //snprintf(buffer, sizeof(buffer), "%u:%u:%u", utc.tm_hour, utc.tm_min, utc.tm_sec);
        //return rb_str_new2(buffer);
    }
    catch (SQLException & e)
    {
        rb_raise_nuodb_error(e.getSqlcode(), "Failed to get time value for the designated column (%d): %s", column, e.getText());
    }
}

VALUE WrapResultSet::getTimestamp(VALUE self, VALUE columnValue)
{
    int column = NUM2UINT(columnValue);
    try
    {
        return rb_str_new2(asPtr(self)->getString(column));
        //Timestamp* ts = asPtr(self)->getTimestamp(column);
        //SqlTimestamp timestamp(ts->getMilliseconds());
        //struct tm utc;
        //timestamp.getTimestamp(&utc);
        //char buffer[250];
        //::strftime(buffer,sizeof(buffer),"%Y%m%dT%I%M%S+0000",&utc);
        //return rb_str_new2(buffer);
    }
    catch (SQLException & e)
    {
        rb_raise_nuodb_error(e.getSqlcode(), "Failed to get timestamp value for the designated column (%d): %s", column, e.getText());
    }
}

VALUE WrapResultSet::getChar(VALUE self, VALUE columnValue)
{
    int column = NUM2UINT(columnValue);
    try
    {
        return rb_str_new2(asPtr(self)->getString(column));
    }
    catch (SQLException & e)
    {
        rb_raise_nuodb_error(e.getSqlcode(), "Failed to get char value for the designated column (%d): %s", column, e.getText());
    }
}

//------------------------------------------------------------------------------

void WrapResultSetMetaData::init(VALUE module)
{
    INIT_TYPE("ResultMetaData");
    DEFINE_METHOD(getColumnCount, 0);
    DEFINE_METHOD(getColumnName, 1);
    DEFINE_METHOD(getType, 1);
    DEFINE_METHOD(getColumnTypeName, 1);
    DEFINE_METHOD(getScale, 1);
    DEFINE_METHOD(getPrecision, 1);
    DEFINE_METHOD(isNullable, 1);
}

VALUE WrapResultSetMetaData::getColumnCount(VALUE self)
{
    try
    {
        return UINT2NUM(asPtr(self)->getColumnCount());
    }
    catch (SQLException & e)
    {
        rb_raise_nuodb_error(e.getSqlcode(), "Failed to get column count from the result set metadata: %s", e.getText());
    }
}

VALUE WrapResultSetMetaData::getColumnName(VALUE self, VALUE columnValue)
{
    int column = NUM2UINT(columnValue);
    try
    {
        return rb_str_new2(asPtr(self)->getColumnName(column));
    }
    catch (SQLException & e)
    {
        rb_raise_nuodb_error(e.getSqlcode(), "Failed to get column name from the result set metadata: %s", e.getText());
    }
}

VALUE WrapResultSetMetaData::getScale(VALUE self, VALUE columnValue)
{
    int column = NUM2UINT(columnValue);
    try
    {
        return UINT2NUM(asPtr(self)->getScale(column));
    }
    catch (SQLException & e)
    {
        rb_raise_nuodb_error(e.getSqlcode(), "Failed to get scale from the result set metadata: %s", e.getText());
    }
}

VALUE WrapResultSetMetaData::getPrecision(VALUE self, VALUE columnValue)
{
    int column = NUM2UINT(columnValue);
    try
    {
        return UINT2NUM(asPtr(self)->getPrecision(column));
    }
    catch (SQLException & e)
    {
        rb_raise_nuodb_error(e.getSqlcode(), "Failed to get precision from the result set metadata: %s", e.getText());
    }
}

VALUE WrapResultSetMetaData::isNullable(VALUE self, VALUE columnValue)
{
    int column = NUM2UINT(columnValue);
    try
    {
        return AS_QBOOL(asPtr(self)->isNullable(column));
    }
    catch (SQLException & e)
    {
        rb_raise_nuodb_error(e.getSqlcode(), "Failed to determine nullability from the result set metadata: %s", e.getText());
    }
}

VALUE WrapResultSetMetaData::getColumnTypeName(VALUE self, VALUE columnValue)
{
    int column = NUM2UINT(columnValue);
    try
    {
        return rb_str_new2(asPtr(self)->getColumnTypeName(column));
    }
    catch (SQLException & e)
    {
        rb_raise_nuodb_error(e.getSqlcode(), "Failed to get column type name from the result set metadata for the designated column (%d): %s", column, e.getText());
    }
}

VALUE WrapResultSetMetaData::getType(VALUE self, VALUE columnValue)
{
    int column = NUM2UINT(columnValue);
    SqlType t = (SqlType)asPtr(self)->getColumnType(column);

    switch(t)
    {
    case NUOSQL_NULL:
        return SYMBOL_OF(SQL_NULL);
    case NUOSQL_BIT:
        return SYMBOL_OF(SQL_BIT);
    case NUOSQL_TINYINT:
        return SYMBOL_OF(SQL_TINYINT);
    case NUOSQL_SMALLINT:
        return SYMBOL_OF(SQL_SMALLINT);
    case NUOSQL_INTEGER:
        return SYMBOL_OF(SQL_INTEGER);
    case NUOSQL_BIGINT:
        return SYMBOL_OF(SQL_BIGINT);
    case NUOSQL_FLOAT:
        return SYMBOL_OF(SQL_FLOAT);
    case NUOSQL_DOUBLE:
        return SYMBOL_OF(SQL_DOUBLE);
    case NUOSQL_CHAR:
        return SYMBOL_OF(SQL_CHAR);
    case NUOSQL_VARCHAR:
        return SYMBOL_OF(SQL_STRING);
    case NUOSQL_LONGVARCHAR:
        return SYMBOL_OF(SQL_LONGVARCHAR);
    case NUOSQL_DATE:
        return SYMBOL_OF(SQL_DATE);
    case NUOSQL_TIME:
        return SYMBOL_OF(SQL_TIME);
    case NUOSQL_TIMESTAMP:
        return SYMBOL_OF(SQL_TIMESTAMP);
    case NUOSQL_BLOB:
        return SYMBOL_OF(SQL_BLOB);
    case NUOSQL_CLOB:
        return SYMBOL_OF(SQL_CLOB);
    case NUOSQL_NUMERIC:
        return SYMBOL_OF(SQL_NUMERIC);
    case NUOSQL_DECIMAL:
        return SYMBOL_OF(SQL_DECIMAL);
    case NUOSQL_BOOLEAN:
        return SYMBOL_OF(SQL_BOOLEAN);
    case NUOSQL_BINARY:
        return SYMBOL_OF(SQL_BINARY);
    case NUOSQL_LONGVARBINARY:
        return SYMBOL_OF(SQL_LONGVARBINARY);
    default:
        // raise FEATURE_NOT_YET_IMPLEMENTED for unsupported types
        rb_raise_nuodb_error(-2, "Invalid SQL type: %d", t);
    }
}

//------------------------------------------------------------------------------

void WrapStatement::init(VALUE module)
{
    INIT_TYPE("Statement");
    DEFINE_METHOD(close, 0);
    DEFINE_METHOD(execute, 1);
    DEFINE_METHOD(executeQuery, 1);
    DEFINE_METHOD(executeUpdate, 1);
    DEFINE_METHOD(getResultSet, 0);
    DEFINE_METHOD(getUpdateCount, 0);
    DEFINE_METHOD(getGeneratedKeys, 0);
}

VALUE WrapStatement::close(VALUE self)
{
    try
    {
        asPtr(self)->close();
        return Qnil;
    }
    catch (SQLException & e)
    {
        rb_raise_nuodb_error(e.getSqlcode(), "Failed to successfully close statement: %s", e.getText());
    }
}

VALUE WrapStatement::execute(VALUE self, VALUE sqlValue)
{
    const char* sql = StringValuePtr(sqlValue);
    try
    {
        return AS_QBOOL(asPtr(self)->execute(sql, NuoDB::RETURN_GENERATED_KEYS ));
    }
    catch (SQLException & e)
    {
        rb_raise_nuodb_error(e.getSqlcode(), "Failed to execute SQL statement (\"%s\"): %s", sql, e.getText());
    }
}

VALUE WrapStatement::executeQuery(VALUE self, VALUE sqlValue)
{
    const char* sql = StringValuePtr(sqlValue);
    try
    {
        return WrapResultSet::wrap(asPtr(self)->executeQuery(sql));
    }
    catch (SQLException & e)
    {
        rb_raise_nuodb_error(e.getSqlcode(), "Failed to execute SQL query statement (\"%s\"): %s", sql, e.getText());
    }
}

VALUE WrapStatement::executeUpdate(VALUE self, VALUE sqlValue)
{
    const char* sql = StringValuePtr(sqlValue);
    try
    {
        return INT2NUM(asPtr(self)->executeUpdate(sql));
    }
    catch (SQLException & e)
    {
        rb_raise_nuodb_error(e.getSqlcode(), "Failed to execute SQL update statement (\"%s\"): %s", sql, e.getText());
    }
}

VALUE WrapStatement::getResultSet(VALUE self)
{
    try
    {
        return WrapResultSet::wrap(asPtr(self)->getResultSet());
    }
    catch (SQLException & e)
    {
        rb_raise_nuodb_error(e.getSqlcode(), "Failed to get statement result set: %s", e.getText());
    }
}

VALUE WrapStatement::getUpdateCount(VALUE self)
{
    try
    {
        return INT2NUM(asPtr(self)->getUpdateCount());
    }
    catch (SQLException & e)
    {
        rb_raise_nuodb_error(e.getSqlcode(), "Failed to get update count for statement: %s", e.getText());
    }
}

VALUE WrapStatement::getGeneratedKeys(VALUE self)
{
    try
    {
        return WrapResultSet::wrap(asPtr(self)->getGeneratedKeys());
    }
    catch (SQLException & e)
    {
        rb_raise_nuodb_error(e.getSqlcode(), "Failed to get generated keys for statement: %s", e.getText());
    }
}

//------------------------------------------------------------------------------

void WrapPreparedStatement::init(VALUE module)
{
    INIT_TYPE("PreparedStatement");
    DEFINE_METHOD(close, 0);
    DEFINE_METHOD(setBoolean, 2);
    DEFINE_METHOD(setInteger, 2);
    DEFINE_METHOD(setDouble, 2);
    DEFINE_METHOD(setString, 2);
    DEFINE_METHOD(setTime, 2);
    DEFINE_METHOD(execute, 0);
    DEFINE_METHOD(executeQuery, 0);
    DEFINE_METHOD(executeUpdate, 0);
    DEFINE_METHOD(getResultSet, 0);
    DEFINE_METHOD(getUpdateCount, 0);
    DEFINE_METHOD(getGeneratedKeys, 0);
}

VALUE WrapPreparedStatement::close(VALUE self)
{
    try
    {
        asPtr(self)->close();
        return Qnil;
    }
    catch (SQLException & e)
    {
        rb_raise_nuodb_error(e.getSqlcode(), "Failed to successfully close prepared statement: %s", e.getText());
    }
}

VALUE WrapPreparedStatement::setTime(VALUE self, VALUE indexValue, VALUE valueValue)
{
    int32_t index = NUM2UINT(indexValue);
    struct timeval tv = rb_time_timeval(valueValue);

    SqlDate d( (((int64_t)tv.tv_sec)* 1000)+ (((int64_t)tv.tv_usec)/ 1000));
    try
    {
        asPtr(self)->setDate(index, &d);
        return Qnil;
    }
    catch (SQLException & e)
    {
        rb_raise_nuodb_error(e.getSqlcode(), "Failed to set prepared statement setTime(%d, %lld) failed: %s",
            index, d.getMilliseconds(), e.getText());
    }
}

VALUE WrapPreparedStatement::setBoolean(VALUE self, VALUE indexValue, VALUE valueValue)
{
    int32_t index = NUM2UINT(indexValue);
    bool value = valueValue ? true : false;
    try
    {
        asPtr(self)->setInt(index, value);
        return Qnil;
    }
    catch (SQLException & e)
    {
        rb_raise_nuodb_error(e.getSqlcode(), "Failed to set prepared statement boolean value (%d, %s): %s",
            index, (value ? "true" : "false"), e.getText());
    }
}

VALUE WrapPreparedStatement::setInteger(VALUE self, VALUE indexValue, VALUE valueValue)
{
    int32_t index = NUM2UINT(indexValue);
    int32_t value = NUM2INT(valueValue);
    try
    {
        asPtr(self)->setInt(index, value);
        return Qnil;
    }
    catch (SQLException & e)
    {
        rb_raise_nuodb_error(e.getSqlcode(), "Failed to set prepared statement integer value (%d, %d): %s", index, value, e.getText());
    }
}

VALUE WrapPreparedStatement::setDouble(VALUE self, VALUE indexValue, VALUE valueValue)
{
    int32_t index = NUM2UINT(indexValue);
    double value = NUM2DBL(valueValue);
    try
    {
        asPtr(self)->setDouble(index, value);
        return Qnil;
    }
    catch (SQLException & e)
    {
        rb_raise_nuodb_error(e.getSqlcode(), "Failed to set prepared statement double value (%d, %g): %s", index, value, e.getText());
    }
}

VALUE WrapPreparedStatement::setString(VALUE self, VALUE indexValue, VALUE valueValue)
{
    int32_t index = NUM2UINT(indexValue);
    char const* value = RSTRING_PTR(valueValue);
    try
    {
        asPtr(self)->setString(index, value);
        return Qnil;
    }
    catch (SQLException & e)
    {
        rb_raise_nuodb_error(e.getSqlcode(), "Failed to set prepared statement string value (%d, \"%s\"): %s", index, value, e.getText());
    }
}

VALUE WrapPreparedStatement::execute(VALUE self)
{
    try
    {
        return AS_QBOOL(asPtr(self)->execute());
    }
    catch (SQLException & e)
    {
        rb_raise_nuodb_error(e.getSqlcode(), "Failed to execute SQL prepared statement: %s", e.getText());
    }
}

VALUE WrapPreparedStatement::executeQuery(VALUE self)
{
    try
    {
        return WrapResultSet::wrap(asPtr(self)->executeQuery());
    }
    catch (SQLException & e)
    {
        rb_raise_nuodb_error(e.getSqlcode(), "Failed to execute SQL query prepared statement: %s", e.getText());
    }
}

VALUE WrapPreparedStatement::executeUpdate(VALUE self)
{
    try
    {
        return INT2NUM(asPtr(self)->executeUpdate());
    }
    catch (SQLException & e)
    {
        rb_raise_nuodb_error(e.getSqlcode(), "Failed to execute SQL update prepared statement: %s", e.getText());
    }
}

VALUE WrapPreparedStatement::getResultSet(VALUE self)
{
    try
    {
        return WrapResultSet::wrap(asPtr(self)->getResultSet());
    }
    catch (SQLException & e)
    {
        rb_raise_nuodb_error(e.getSqlcode(), "Failed to get the result set for the prepared statement: %s", e.getText());
    }
}

VALUE WrapPreparedStatement::getUpdateCount(VALUE self)
{
    try
    {
        return INT2NUM(asPtr(self)->getUpdateCount());
    }
    catch (SQLException & e)
    {
        rb_raise_nuodb_error(e.getSqlcode(), "Failed to get the update count for the prepared statement: %s", e.getText());
    }
}

VALUE WrapPreparedStatement::getGeneratedKeys(VALUE self)
{
    try
    {
        return WrapResultSet::wrap(asPtr(self)->getGeneratedKeys());
    }
    catch (SQLException & e)
    {
        rb_raise_nuodb_error(e.getSqlcode(), "Failed to get the generated keys for the prepared statement: %s", e.getText());
    }
}

//------------------------------------------------------------------------------

void WrapConnection::init(VALUE module)
{
    type = rb_define_class_under(module, "Connection", rb_cObject);

    // DBI

    // todo add .columns, or not? If we did: .columns(table_name)
    // todo and tie this into the SchemaCache on the Ruby side.
    rb_define_method(type, "commit", RUBY_METHOD_FUNC(commit), 0);
    rb_define_method(type, "disconnect", RUBY_METHOD_FUNC(close), 0);
    rb_define_method(type, "ping", RUBY_METHOD_FUNC(ping), 0);
    rb_define_method(type, "prepare", RUBY_METHOD_FUNC(createPreparedStatement), 1);
    rb_define_method(type, "rollback", RUBY_METHOD_FUNC(rollback), 0);
    // todo add .tables, definitely!

    // NUODB EXTENSIONS

    rb_define_method(type, "autocommit=", RUBY_METHOD_FUNC(setAutoCommit), 1);
    rb_define_method(type, "autocommit?", RUBY_METHOD_FUNC(hasAutoCommit), 0);

    // DEPRECATED, going away shortly...

    // todo use .new and .initialize instead...
    DEFINE_SINGLE(createSqlConnection, 4);
    DEFINE_METHOD(createStatement, 0);
    DEFINE_METHOD(createPreparedStatement, 1);
    DEFINE_METHOD(setAutoCommit, 1);
    DEFINE_METHOD(hasAutoCommit, 0);
    DEFINE_METHOD(getMetaData, 0);
    DEFINE_METHOD(close, 0);
    DEFINE_METHOD(getSchema, 0);
}

VALUE WrapConnection::getSchema(VALUE self)
{
    try
    {
        return rb_str_new2(asPtr(self)->getSchema());
    }
    catch (SQLException & e)
    {
        rb_raise_nuodb_error(e.getSqlcode(), "Failed to get schema for connection: %s", e.getText());
    }
}

VALUE WrapConnection::close(VALUE self)
{
    try
    {
        asPtr(self)->close();
        return Qnil;
    }
    catch (SQLException & e)
    {
        rb_raise_nuodb_error(e.getSqlcode(), "Failed to successfully disconnect connection: %s", e.getText());
    }
}

VALUE WrapConnection::ping(VALUE self)
{
    try
    {
        asPtr(self)->ping();
        return Qtrue;
    }
    catch (SQLException & e)
    {
        return Qfalse;
    }
}

VALUE WrapConnection::createStatement(VALUE self)
{
    try
    {
        return WrapStatement::wrap(asPtr(self)->createStatement());
    }
    catch (SQLException & e)
    {
        rb_raise_nuodb_error(e.getSqlcode(), "Failed to create statement: %s", e.getText());
    }
}

VALUE WrapConnection::createPreparedStatement(VALUE self, VALUE sqlValue)
{
    const char * sql = StringValuePtr(sqlValue);
    try
    {
        return WrapPreparedStatement::wrap( asPtr(self)->prepareStatement(sql, NuoDB::RETURN_GENERATED_KEYS));
    }
    catch (SQLException & e)
    {
        rb_raise_nuodb_error(e.getSqlcode(), "Failed to create prepared statement (%s): %s", sql, e.getText());
    }
}

VALUE WrapConnection::setAutoCommit(VALUE self, VALUE autoCommitValue)
{
    bool autoCommit = !(RB_TYPE_P(autoCommitValue, T_FALSE) || RB_TYPE_P(autoCommitValue, T_NIL));
    try
    {
        asPtr(self)->setAutoCommit(autoCommit);
        return Qnil;
    }
    catch (SQLException & e)
    {
        rb_raise_nuodb_error(e.getSqlcode(), "Failed to set auto-commit (%d) for connection: %s", autoCommit, e.getText());
    }
}

VALUE WrapConnection::hasAutoCommit(VALUE self)
{
    try
    {
        return AS_QBOOL(asPtr(self)->getAutoCommit());
    }
    catch (SQLException & e)
    {
        rb_raise_nuodb_error(e.getSqlcode(), "Failed to determine auto-commit state for connection: %s", e.getText());
    }
}

VALUE WrapConnection::commit(VALUE self)
{
    try
    {
        asPtr(self)->commit();
        return Qnil;
    }
    catch (SQLException & e)
    {
        rb_raise_nuodb_error(e.getSqlcode(), "Failed to commit transaction: %s", e.getText());
    }
}

VALUE WrapConnection::rollback(VALUE self)
{
    try
    {
        asPtr(self)->rollback();
        return Qnil;
    }
    catch (SQLException & e)
    {
        rb_raise_nuodb_error(e.getSqlcode(), "Failed to rollback transaction: %s", e.getText());
    }
}

VALUE WrapConnection::getMetaData(VALUE self)
{
    try
    {
        return WrapDatabaseMetaData::wrap( asPtr(self)->getMetaData());
    }
    catch (SQLException & e)
    {
        rb_raise_nuodb_error(e.getSqlcode(), "Failed to get database metadata: %s", e.getText());
    }
}

VALUE WrapConnection::createSqlConnection(VALUE self,
    VALUE databaseValue,
    VALUE schemaValue,
    VALUE usernameValue,
    VALUE passwordValue)
{
    char const* database = StringValuePtr(databaseValue);
    char const* schema = StringValuePtr(schemaValue);
    char const* username = StringValuePtr(usernameValue);
    char const* password = StringValuePtr(passwordValue);

    try
    {
        return WrapConnection::wrap( getDatabaseConnection(database,
            username,
            password,
            1,
            "schema",
            schema));
    }
    catch (SQLException & e)
    {
        rb_raise_nuodb_error(e.getSqlcode(),
            "Failed to create database connection (\"%s\", \"%s\", ********, \"%s\"): %s",
            database,
            username,
            schema,
            e.getText());
    }
}

//------------------------------------------------------------------------------

extern "C" void Init_nuodb(void)
{
    m_nuodb = rb_define_module("NuoDB");
    c_nuodb_error = rb_const_get(m_nuodb, rb_intern("Error"));
    c_error_code_assignment = rb_intern("error_code=");

    WrapConnection::init(m_nuodb);
    WrapDatabaseMetaData::init(m_nuodb);
    WrapStatement::init(m_nuodb);
    WrapPreparedStatement::init(m_nuodb);
    WrapResultSet::init(m_nuodb);
    WrapResultSetMetaData::init(m_nuodb);
}
