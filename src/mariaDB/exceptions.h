/*
 * extDB3
 * © 2016 Declan Ireland <https://bitbucket.org/torndeco/extdb3>
 */

#pragma once

#include <iostream>
#include <exception>
#include <string>

class MariaDBConnectorException: public std::exception
{
public:
  MariaDBConnectorException(MYSQL *mysql_ptr) : mysql_ptr(mysql_ptr) {}
  virtual const char* what() const throw()
  {
	  return mysql_error(mysql_ptr);
  }
private:
  MYSQL *mysql_ptr;
};


class MariaDBStatementException: public std::exception
{
public:
  MariaDBStatementException(MYSQL_STMT *mysql_stmt_ptr) : mysql_stmt_ptr(mysql_stmt_ptr) {}
  virtual const char* what() const throw()
  {
    std::cout << "ERROR" << std::endl;
    std::cout << mysql_stmt_error(mysql_stmt_ptr) << std::endl;
	  return mysql_stmt_error(mysql_stmt_ptr);
  }
private:
  MYSQL_STMT *mysql_stmt_ptr;
};


class MariaDBStatementException2 : public std::exception
{
public:
	MariaDBStatementException2(std::string msg) : msg(msg) {}
	virtual const char* what() const throw()
	{
		return msg.c_str();
	}
private:
	std::string msg;
};


class MariaDBQueryException: public std::exception
{
public:
  MariaDBQueryException(MYSQL *mysql_ptr): mysql_ptr(mysql_ptr) {}
  virtual const char* what() const throw()
  {
    return mysql_error(mysql_ptr);
  }
private:
  MYSQL *mysql_ptr;
};
