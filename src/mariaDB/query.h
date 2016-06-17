/*
 * extDB3
 * © 2016 Declan Ireland <https://bitbucket.org/torndeco/extdb3>
 */

#pragma once

#include <string>
#include <vector>

#include <boost/date_time/posix_time/posix_time.hpp>
#include <mysql.h>

#include "connector.h"


class MariaDBQuery
{
public:
  MariaDBQuery();
  ~MariaDBQuery();

  void init(MariaDBConnector &connector);
  void send(std::string &sql_query);
  void get(std::string &insertID, std::vector<std::vector<std::string>> &result_vec, int check_dataType_string=0, bool check_dataType_null=false);

private:
  MariaDBConnector *connector_ptr;
  std::locale loc_date;
  std::locale loc_datetime;
  std::locale loc_time;
  boost::posix_time::time_facet* facet;
};
