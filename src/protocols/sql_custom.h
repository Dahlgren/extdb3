/*
 * extDB3
 * © 2016 Declan Ireland <https://bitbucket.org/torndeco/extdb3>
 */

#pragma once

#include <boost/filesystem.hpp>
#include <boost/property_tree/ini_parser.hpp>
#include <thread>
#include <unordered_map>


#include "abstract_protocol.h"
#include "../mariaDB/abstract.h"

#define EXTDB_SQL_CUSTOM_REQUIRED_VERSION 1
#define EXTDB_SQL_CUSTOM_LATEST_VERSION 1


class SQL_CUSTOM: public AbstractProtocol
{
	public:
		bool init(AbstractExt *extension, const std::string &database_id, const std::string &options_str);
		bool callProtocol(std::string input_str, std::string &result, const bool async_method, const unsigned int unique_id=1);

	private:
		MariaDBPool *database_pool;
		boost::property_tree::ptree ptree;

		struct call_struct
		{
			bool preparedStatement = false;
			bool returnInsertID = false;

			std::string strip_chars;
			int strip_chars_mode = 0;

			int highest_input_value = 0;

			std::string sql;
			std::vector<sql_option> input_options;
			std::vector<sql_option> output_options;
		};
		std::unordered_map<std::string, call_struct> calls;

		bool loadConfig(boost::filesystem::path &config_path);
};
