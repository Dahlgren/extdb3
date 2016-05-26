/*
 * extDB3
 * © 2016 Declan Ireland <https://bitbucket.org/torndeco/extdb3>
 */

#pragma once

//Lazy Method to prevent circular dependency issue between SQL_CUSTOM & MariaDBStatement Classes
struct sql_option
{
  bool beguidConvert = false;

  bool boolConvert = false;
  bool nullConvert = false;
  bool timeConvert = false;

  bool stringify = false;
  bool string_escape_quotes = false;

  bool stringify2 = false;
  bool string_escape_quotes2 = false;

  bool mysql_escape = false;

  bool strip = false;

  int value_number = -1;
};
