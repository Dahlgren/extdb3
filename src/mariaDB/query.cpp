/*
 * extDB3
 * © 2016 Declan Ireland <https://bitbucket.org/torndeco/extdb3>
 */

#include "query.h"

#include <iomanip>
#include <iostream>
#include <sstream>

#include <ma_errmsg.h>
//#include <errmsg.h>

#include "exceptions.h"


MariaDBQuery::MariaDBQuery()
{
  loc_date = std::locale(std::locale::classic(), new boost::posix_time::time_input_facet("%Y-%m-%d"));
  loc_datetime = std::locale(std::locale::classic(), new boost::posix_time::time_input_facet("%Y-%m-%d %H:%M:%S"));
  loc_time = std::locale(std::locale::classic(), new boost::posix_time::time_input_facet("%H:%M:%S"));
}


MariaDBQuery::~MariaDBQuery(void)
{
}


void MariaDBQuery::init(MariaDBConnector &connector)
{
  connector_ptr = &connector;
}


void MariaDBQuery::get(std::vector<std::vector<std::string>> &result_vec, bool check_dataType_string, bool check_dataType_null)
{
  result_vec.clear();
  MYSQL_RES *result = (mysql_store_result(connector_ptr->mysql_ptr));  // Returns NULL for Errors & No Result
  if (!result)
  {
    std::string error_msg(mysql_error(connector_ptr->mysql_ptr));
    if (!error_msg.empty())
    {
      mysql_free_result(result);
      throw MariaDBQueryException(connector_ptr->mysql_ptr);
    }
  } else {
    unsigned int num_fields = mysql_num_fields(result);
    if (num_fields > 0)
    {
      MYSQL_ROW row;
      MYSQL_FIELD *mysql_fields;
      mysql_fields = mysql_fetch_fields(result);
      while ((row = mysql_fetch_row(result)) != NULL)
      {
        std::vector<std::string> field_row;
        for (unsigned int i = 0; i < num_fields; i++)
        {
          if (!(row[i]))
          {
            if (check_dataType_null)
            {
              field_row.emplace_back("objNull");
            } else {
              field_row.emplace_back("\"\"");
            }
          } else {
            switch(mysql_fields[i].type)
            {
  			      case MYSQL_TYPE_VAR_STRING:
              case MYSQL_TYPE_TINY_BLOB:
              case MYSQL_TYPE_MEDIUM_BLOB:
              case MYSQL_TYPE_BLOB:
              {
  			        std::string tmp_str(row[i]);
                if (tmp_str.empty())
                {
                  if (check_dataType_null)
                  {
                    field_row.emplace_back("objNull");
                  } else {
                    field_row.emplace_back("\"\"");
                  }
                }
                if (check_dataType_string)
                {
                  field_row.emplace_back(('"' + std::move(tmp_str) + '"'));
                } else {
                  field_row.push_back(std::move(tmp_str));
                }
                break;
              }
              case MYSQL_TYPE_DATE:
              {
                try
                {
                  std::istringstream is(row[i]);
                  is.imbue(loc_date);
                  boost::posix_time::ptime ptime;
                  is >> ptime;

                  std::stringstream stream;
                  facet = new boost::posix_time::time_facet();
                  facet->format("[%Y,%m,%d]");
                  stream.imbue(std::locale(std::locale::classic(), facet));
                  stream << ptime;
                  std::string tmp_str = stream.str();
                  if (tmp_str != "not-a-date-time")
                  {
                    field_row.push_back(std::move(tmp_str));
                  } else {
                    field_row.emplace_back("[]");
                  }
                }
                catch(std::exception& e)
                {
                  field_row.emplace_back("[]");
                }
                break;
              }
              case MYSQL_TYPE_DATETIME:
              {
                try
                {
                  std::istringstream is(row[i]);
                  is.imbue(loc_datetime);
                  boost::posix_time::ptime ptime;
                  is >> ptime;

                  std::stringstream stream;
                  facet = new boost::posix_time::time_facet();
                  facet->format("[%Y,%m,%d,%H,%M,%S]");
                  stream.imbue(std::locale(std::locale::classic(), facet));
                  stream << ptime;
                  std::string tmp_str = stream.str();
                  if (tmp_str != "not-a-date-time")
                  {
                    field_row.push_back(std::move(tmp_str));
                  } else {
                    field_row.emplace_back("[]");
                  }
                }
                catch(std::exception& e)
                {
                  field_row.emplace_back("[]");
                }
                break;
              }
              case MYSQL_TYPE_TIME:
              {
                try
                {
                  std::istringstream is(row[i]);
                  is.imbue(loc_time);
                  boost::posix_time::ptime ptime;
                  is >> ptime;

                  std::stringstream stream;
                  facet = new boost::posix_time::time_facet();
                  facet->format("[%H,%M,%S]");
                  stream.imbue(std::locale(std::locale::classic(), facet));
                  stream << ptime;
                  std::string tmp_str = stream.str();
                  if (tmp_str != "not-a-date-time")
                  {
                    field_row.push_back(std::move(tmp_str));
                  } else {
                    field_row.emplace_back("[]");
                  }
                }
                catch(std::exception& e)
                {
                  field_row.emplace_back("[]");
                }
                break;
              }
              case MYSQL_TYPE_NULL:
              {
                if (check_dataType_null)
                {
                  field_row.emplace_back("objNull");
                } else {
                  field_row.emplace_back("\"\"");
                }
                break;
              }
              default:
              {
                field_row.emplace_back(row[i]);
              }
            }
          }
        }
        result_vec.push_back(std::move(field_row));
      }
    }
  }
  mysql_free_result(result);
}


void MariaDBQuery::send(std::string &sql_query)
{
  unsigned long len = sql_query.length();
  int return_code = mysql_real_query(connector_ptr->mysql_ptr, sql_query.c_str(), len);
  if (return_code != 0)
  {
    int error_code = mysql_errno(connector_ptr->mysql_ptr);
    if ((error_code == CR_SERVER_GONE_ERROR) || (error_code == CR_SERVER_LOST))
    {
      return_code = mysql_real_query(connector_ptr->mysql_ptr, sql_query.c_str(), len);
    }
  }
  if (return_code != 0) throw MariaDBQueryException(connector_ptr->mysql_ptr);
}