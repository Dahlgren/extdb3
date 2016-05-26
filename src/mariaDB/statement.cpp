/*
 * extDB3
 * © 2016 Declan Ireland <https://bitbucket.org/torndeco/extdb3>
 */

#include "statement.h"

#include <string>
#include <sstream>

#include <boost/algorithm/string.hpp>
#include <ma_errmsg.h>
//#include <errmsg.h>

#include "exceptions.h"
#include "../md5/md5.h"


MariaDBStatement::MariaDBStatement()
{
}


MariaDBStatement::~MariaDBStatement(void)
{
  if (mysql_stmt_result_metadata_ptr)
  {
    mysql_free_result(mysql_stmt_result_metadata_ptr);
    mysql_stmt_result_metadata_ptr = NULL;
  }
  if (mysql_stmt_ptr)
  {
    mysql_stmt_close(mysql_stmt_ptr);
  }
  bind_data.clear();
  if (mysql_bind_params)
  {
    delete[] mysql_bind_params;
  }
}


void MariaDBStatement::init(MariaDBConnector &connector)
{
  connector_ptr = &connector;
}


void MariaDBStatement::create()
{
  mysql_stmt_ptr = mysql_stmt_init(connector_ptr->mysql_ptr);
  if (!mysql_stmt_ptr)
  {
    throw MariaDBStatementException1(mysql_stmt_ptr);
  }
}


void MariaDBStatement::prepare(std::string &sql_query)
{
  if (!prepared)
  {
    unsigned long len = sql_query.length();
  	int return_code = mysql_stmt_prepare(mysql_stmt_ptr, sql_query.c_str(), len);
    if (return_code != 0)
    {
      int error_code = mysql_errno(connector_ptr->mysql_ptr);
      if ((error_code == CR_SERVER_GONE_ERROR) || (error_code == CR_SERVER_LOST))
      {
        return_code = mysql_stmt_prepare(mysql_stmt_ptr, sql_query.c_str(), len);
      }
	  if (return_code != 0) throw MariaDBStatementException0(connector_ptr->mysql_ptr);
    }
    prepared = true;
  }
}


unsigned long MariaDBStatement::getParamsCount()
{
  return mysql_stmt_param_count(mysql_stmt_ptr);
}


void MariaDBStatement::bindParams(std::vector<MariaDBStatement::mysql_bind_param> &params)
{
  int params_count = mysql_stmt_param_count(mysql_stmt_ptr); // mysql_stmt_ptr->param_count;

  if (params.size() != params_count)
  {
	  throw MariaDBStatementException2("SQL Invalid Number Number of Inputs Got " + std::to_string(params.size()) + " Expected " + std::to_string(params_count));
  }

  delete[] mysql_bind_params;
  mysql_bind_params = new MYSQL_BIND[params_count];
  //memset(&mysql_bind_params, 0, sizeof(&mysql_bind_params));

  for (int i=0 ; i<params_count ; i++)
  {
    MYSQL_BIND mysql_bind = {0};
    MariaDBStatement::mysql_bind_param &param = params[i];

    switch (param.type)
    {
      case MYSQL_TYPE_DATE:
      case MYSQL_TYPE_TIME:
      case MYSQL_TYPE_DATETIME:
      {
        /*
        unsigned int year	The year
        unsigned int month	The month of the year
        unsigned int day	The day of the month
        unsigned int hour	The hour of the day
        unsigned int minute	The minute of the hour
        unsigned int second	The second of the minute
        my_bool neg	A boolean flag indicating whether the time is negative
        unsigned long second_part	The fractional part of the second in microseconds
        */
        if (param.buffer.size() > 2)
        {
          param.buffer.erase(0,1);
          param.buffer.pop_back();

    			std::vector<std::string> tokens;
    			boost::split(tokens, param.buffer, boost::is_any_of(","));
          unsigned int time_value;
          for (unsigned int i = 0; i < tokens.size(); i++)
          {
            try
            {
              time_value = std::stoul(tokens[i]);
              switch (i)
              {
                case 0:
          		    param.time_buffer.year = time_value;
                case 1:
                  param.time_buffer.month = time_value;
                case 2:
                  param.time_buffer.day = time_value;
                case 3:
                  param.time_buffer.hour = time_value;
                case 4:
                  param.time_buffer.minute = time_value;
                case 5:
                  param.time_buffer.second = time_value;
                default:
                  throw MariaDBStatementException2("Invalid Time Format: [" + param.buffer + "]");
              }
            }
            catch(std::exception const &e)
            {
              throw MariaDBStatementException2("Invalid Time Format: [" + param.buffer + "]");
            }
          }
        }
        mysql_bind.buffer_type = param.type;
        mysql_bind.buffer = (char *)&(param.time_buffer);
        break;
      }

      case MYSQL_TYPE_TINY:
      case MYSQL_TYPE_SHORT:
      case MYSQL_TYPE_INT24:
      case MYSQL_TYPE_LONG:
      case MYSQL_TYPE_FLOAT:
      case MYSQL_TYPE_DOUBLE:
      case MYSQL_TYPE_LONGLONG:
      case MYSQL_TYPE_DECIMAL:
      case MYSQL_TYPE_NEWDECIMAL:
      case MYSQL_TYPE_STRING:
      case MYSQL_TYPE_VARCHAR:
      case MYSQL_TYPE_VAR_STRING:
      case MYSQL_TYPE_TINY_BLOB:
      case MYSQL_TYPE_MEDIUM_BLOB:
      case MYSQL_TYPE_BLOB:
      {
        mysql_bind.buffer_type = MYSQL_TYPE_STRING;
        param.buffer_c.reset(new char[param.length+1]);
	      //param.buffer_c = param.buffer;
		    std::copy(param.buffer.begin(), param.buffer.end(), param.buffer_c.get());
        mysql_bind.buffer  = param.buffer_c.get();
      	mysql_bind.buffer_length = param.length;
        break;
      }
      case MYSQL_TYPE_NULL:
      {
        mysql_bind.buffer_type   = param.type;
        mysql_bind.buffer_length = 0;
        break;
      }
      case MYSQL_TYPE_LONG_BLOB:
      {
        throw MariaDBStatementException2("Field Type not supported: LONGBLOB/LONGTEXT");
      }
      default:
        throw MariaDBStatementException2("Unknown Field Type: " + std::to_string(param.type));
    }
    mysql_bind_params[i] = (std::move(mysql_bind));
  }
  mysql_stmt_bind_param(mysql_stmt_ptr, mysql_bind_params);
}


void MariaDBStatement::execute(std::vector<sql_option> &output_options, std::string &strip_chars, int &strip_chars_mode, std::vector<std::vector<std::string>> &results)
{
  mysql_stmt_result_metadata_ptr = mysql_stmt_result_metadata(mysql_stmt_ptr);
  if (mysql_stmt_result_metadata_ptr)
  {
    num_fields = mysql_num_fields(mysql_stmt_result_metadata_ptr);
    fields = mysql_fetch_fields(mysql_stmt_result_metadata_ptr);

    mysql_bind_result = new MYSQL_BIND[num_fields];
    memset(mysql_bind_result, 0, sizeof(MYSQL_BIND)*num_fields);

    bind_data.clear();
    bind_data.resize(num_fields);

  	std::size_t size = 0;
  	for (unsigned int i = 0; i < num_fields; i++)
  	{
      // Setup Buffers
          // MYSQL Types http://dev.mysql.com/doc/refman/5.7/en/c-api-prepared-statement-type-codes.html

      switch (fields[i].type)
    	{
        case MYSQL_TYPE_DATE:
        case MYSQL_TYPE_TIME:
        case MYSQL_TYPE_DATETIME:
        {
          mysql_bind_result[i].buffer_type = fields[i].type;
          mysql_bind_result[i].buffer = (char *)&bind_data[i].buffer_mysql_time;
          size = fields[i].length;
          break;
        }
        case MYSQL_TYPE_LONG_BLOB:
        {
          throw MariaDBStatementException2("MYSQL_TYPE_LONG_BLOB type not supported when using Prepared Statements");
        }
        /*
        case MYSQL_TYPE_DECIMAL:
      	case MYSQL_TYPE_NEWDECIMAL:
      	case MYSQL_TYPE_STRING:
      	case MYSQL_TYPE_VAR_STRING:
      	case MYSQL_TYPE_TINY_BLOB:
      	case MYSQL_TYPE_MEDIUM_BLOB:
      	case MYSQL_TYPE_BLOB:
        */
        default:
        {
          mysql_bind_result[i].buffer_type = MYSQL_TYPE_STRING;
          size = fields[i].length;
          bind_data[i].buffer.resize(fields[i].length + 1);

          if (size == 0xFFFFFFFF) size = 0;
          unsigned int len = static_cast<unsigned int>(size);
          mysql_bind_result[i].buffer_length = len;
          mysql_bind_result[i].buffer        = (len > 0) ? &bind_data[i].buffer[0] : NULL;
          break;
        }
    	}
      mysql_bind_result[i].length        = &(bind_data[i].length);
      mysql_bind_result[i].is_null       = &(bind_data[i].isNull);
      mysql_bind_result[i].is_unsigned   = (fields[i].flags & UNSIGNED_FLAG) > 0;
      mysql_bind_result[i].error         = &(bind_data[i].isNull);
  	}
	if (mysql_stmt_bind_result(mysql_stmt_ptr, mysql_bind_result) != 0)
	{
		throw MariaDBStatementException1(mysql_stmt_ptr);
	}
  };

  if (mysql_stmt_execute(mysql_stmt_ptr) != 0)
  {
    throw MariaDBStatementException1(mysql_stmt_ptr);
  }
  if (mysql_stmt_store_result(mysql_stmt_ptr))
  {
    throw MariaDBStatementException1(mysql_stmt_ptr);
  }

  if (mysql_stmt_result_metadata_ptr)
  {
    int error_code = 0;
    while (true)
    {
    	error_code = mysql_stmt_fetch(mysql_stmt_ptr);
      	// we have specified zero buffers for BLOBs, so DATA_TRUNCATED is normal in this case

      if ((error_code !=0) && (error_code != MYSQL_DATA_TRUNCATED) && (error_code != MYSQL_NO_DATA))
      {
        throw MariaDBStatementException1(mysql_stmt_ptr);
      }
      if (error_code != 0) break;

      //Process Result
      std::vector<std::string> result;
      output_options.resize(num_fields);
      for (unsigned int i = 0; i < num_fields; i++)
      {
        if (bind_data[i].isNull)
        {
          if (output_options[i].nullConvert)
          {
            result.emplace_back("objNull");
          } else {
            result.emplace_back("\"\"");
          }
        } else {
          switch (fields[i].type)
          {
            case MYSQL_TYPE_DATE:
            case MYSQL_TYPE_TIME:
            case MYSQL_TYPE_DATETIME:
            {
             result.emplace_back("[" +
                                    std::to_string(bind_data[i].buffer_mysql_time.year) + "," +
                                    std::to_string(bind_data[i].buffer_mysql_time.month) + "," +
                                    std::to_string(bind_data[i].buffer_mysql_time.day) + "," +
                                    std::to_string(bind_data[i].buffer_mysql_time.hour) + "," +
                                    std::to_string(bind_data[i].buffer_mysql_time.minute) + "," +
                                    std::to_string(bind_data[i].buffer_mysql_time.second) +
                                "]");
              break;
            }
            case MYSQL_TYPE_STRING:
            case MYSQL_TYPE_VAR_STRING:
            case MYSQL_TYPE_TINY_BLOB:
            case MYSQL_TYPE_MEDIUM_BLOB:
            case MYSQL_TYPE_BLOB:
            {
				      std::string tmp_str(&bind_data[i].buffer[0], bind_data[i].length);
              if (output_options[i].strip)
              {
                std::string stripped_str(tmp_str);
                for (auto &strip_char : strip_chars)
                {
                  boost::erase_all(stripped_str, std::string(1, strip_char));
                }
                if (stripped_str != tmp_str)
                {
                  switch (strip_chars_mode)
                  {
                    case 2: // Log + Error
                      throw MariaDBStatementException2("Bad Character detected from database query");
                    //case 1: // Log
                      //logger->warn("extDB3: SQL_CUSTOM: Error Bad Char Detected: Input: {0} Token: {1}", input_str, processed_inputs[i].buffer);
                  }
                  tmp_str = std::move(stripped_str);
                }
              }
              if (output_options[i].beguidConvert)
              {
                try
                {
                  int64_t steamID = std::stoll(tmp_str, nullptr);
                  std::stringstream bestring;
                  int8_t i = 0, parts[8] = { 0 };
                  do parts[i++] = steamID & 0xFF;
                  while (steamID >>= 8);
                  bestring << "BE";
                  for (int i = 0; i < sizeof(parts); i++) {
                    bestring << char(parts[i]);
                  }
                  tmp_str = md5(bestring.str());
                }
                catch(std::exception const & e)
                {
                  tmp_str = "ERROR";
                }
              }
              if (output_options[i].boolConvert)
              {
                if (tmp_str == "1")
                {
                  tmp_str = "true";
                } else {
                  tmp_str = "false";
                }
              }
              if (output_options[i].string_escape_quotes)
              {
                boost::replace_all(tmp_str, "\"", "\"\"");
                tmp_str = "\"" + tmp_str + "\"";
              }
              if (output_options[i].stringify)
              {
                tmp_str = "\"" + tmp_str + "\"";
              }
              if (output_options[i].string_escape_quotes2)
              {
                boost::replace_all(tmp_str, "\"", "\"\"");
                tmp_str = "'" + tmp_str + "'";
              }
              if (output_options[i].stringify2)
              {
                tmp_str = "'" + tmp_str + "'";
              }
              result.push_back(std::move(tmp_str));
              break;
            }
            case MYSQL_TYPE_NULL:
            {
              if (output_options[i].nullConvert)
              {
                result.emplace_back("objNull");
              } else {
                result.emplace_back("\"\"");
              }
              break;
            }

            case MYSQL_TYPE_LONG_BLOB:
            {
              throw MariaDBStatementException2("MYSQL_TYPE_LONG_BLOB type not supported");
            }
            default:
              std::string tmp_str(&bind_data[i].buffer[0], bind_data[i].length);
              result.push_back(std::move(tmp_str));
          }
        }
      }
      results.push_back(std::move(result));
    }
  }

  mysql_free_result(mysql_stmt_result_metadata_ptr);
  mysql_stmt_result_metadata_ptr = NULL;
  delete[] mysql_bind_result;
  bind_data.clear();
}
