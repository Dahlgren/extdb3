/*
 * extDB3
 * © 2016 Declan Ireland <https://bitbucket.org/torndeco/extdb3>
 */

#include "sql_custom.h"

#include <algorithm>
#include <thread>

#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/erase.hpp>
#include <boost/filesystem.hpp>
#include <boost/optional/optional.hpp>
#include <boost/property_tree/ini_parser.hpp>

#include "../mariaDB/exceptions.h"
#include "../mariaDB/session.h"
#include "../md5/md5.h"


bool SQL_CUSTOM::init(AbstractExt *extension, const std::string &database_id, const std::string &options_str)
{
	extension_ptr = extension;

	if (extension_ptr->mariadb_databases.count(database_id) == 0)
	{
		#ifdef DEBUG_TESTING
			extension_ptr->console->warn("extDB3: SQL_CUSTOM: No Database Connection: {0}", database_id);
		#endif
		extension_ptr->logger->warn("extDB3: SQL_CUSTOM: No Database Connection: {0}", database_id);
		return false;
	}
	database_pool = &extension->mariadb_databases[database_id];

	if (options_str.empty())
	{
		#ifdef DEBUG_TESTING
			extension_ptr->console->warn("extDB3: SQL_CUSTOM: Missing Config Parameter");
		#endif
		extension_ptr->logger->warn("extDB3: SQL_CUSTOM: Missing Config Parameter");
		return false;
	}

	try
	{
		boost::filesystem::path custom_ini_path(extension_ptr->ext_info.path);
		custom_ini_path /= "sql_custom";
		boost::filesystem::create_directories(custom_ini_path); // Create Directory if Missing
		custom_ini_path /= options_str;
		if (boost::filesystem::exists(custom_ini_path))
		{
			if (boost::filesystem::is_regular_file(custom_ini_path))
			{
				return loadConfig(custom_ini_path);
			} else {
				#ifdef DEBUG_TESTING
					extension_ptr->console->info("extDB3: SQL_CUSTOM: Loading Template Error: Not Regular File: {0}", custom_ini_path);
				#endif
				extension_ptr->logger->info("extDB3: SQL_CUSTOM: Loading Template Error: Not Regular File: {0}", custom_ini_path);
				return false;
			}
		} else {
			#ifdef DEBUG_TESTING
				extension_ptr->console->info("extDB3: SQL_CUSTOM: {0} doesn't exist", custom_ini_path);
			#endif
			extension_ptr->logger->info("extDB3: SQL_CUSTOM: {0} doesn't exist", custom_ini_path);
			return false;
		}
	}
	catch (boost::filesystem::filesystem_error &e)
	{
		#ifdef DEBUG_TESTING
			extension_ptr->console->info("extDB3: SQL_CUSTOM: filesystem_error: {0}", e.what());
		#endif
		extension_ptr->logger->info("extDB3: SQL_CUSTOM: filesystem_error: {0}", e.what());
		return false;
	}
}


bool SQL_CUSTOM::loadConfig(boost::filesystem::path &config_path)
{
	bool status = true;
	try
	{
		boost::property_tree::ini_parser::read_ini(config_path.string(), ptree);
		std::string strip_chars = ptree.get("Default.Strip Chars", "");
		int strip_chars_mode = ptree.get("Default.Strip Chars Mode", 0);

		ptree.get_child("Default").erase("Strip Chars");
		ptree.get_child("Default").erase("Strip Chars Mode");
		ptree.get_child("Default").erase("Version");

		for (auto& value : ptree.get_child("Default")) {
			#ifdef DEBUG_TESTING
				extension_ptr->console->info("extDB3: SQL_CUSTOM Config Error: Section Default Unknown Setting: {0}", value.first);
			#endif
			extension_ptr->logger->info("extDB3: SQL_CUSTOM Config Error: Section Default Unknown Setting: {0}", value.first);
			status = false;
		}

		ptree.erase("Default");
		for (auto& section : ptree) {

			int num_line = 1;
			int num_line_part = 1;
			std::string sql;
			std::string path;

			while(true)
			{
				// Check For SQLx_1
				path = section.first + ".SQL" + std::to_string(num_line) + "_" + std::to_string(num_line_part);
				auto child = ptree.get_child_optional(path);
				if (!child) break;

				// Fetch SQLx_x
				while(true)
				{
					path = section.first + ".SQL" + std::to_string(num_line) + "_" + std::to_string(num_line_part);
					auto child = ptree.get_child_optional(path);
					if (!child)
					{
						ptree.get_child(section.first).erase("SQL" + std::to_string(num_line) + "_" + std::to_string(num_line_part));
						break;
					}
					sql += ptree.get<std::string>(path) + " ";
					ptree.get_child(section.first).erase("SQL" + std::to_string(num_line) + "_" + std::to_string(num_line_part));
					++num_line_part;
				}


				// Fetch Custom
					// Fetch SQL INPUT OPTION
				path = section.first + ".SQL" + std::to_string(num_line) + "_INPUTS";
				std::string input_options_str = ptree.get<std::string>(path, "");
				ptree.get_child(section.first).erase("SQL" + std::to_string(num_line) + "_INPUTS");

					// Fetch SQL OUTPUT OPTION
				path = section.first + ".OUTPUT";
				std::string output_options_str = ptree.get<std::string>(path, "");
				ptree.get_child(section.first).erase("OUTPUT");

				// Parse SQL INPUT OPTIONS
				std::vector<std::string> tokens;
				std::vector<std::string> sub_tokens;

				if (!(input_options_str.empty()))
				{
					int highest_input_value = 0;
					boost::split(tokens, input_options_str, boost::is_any_of(","));
					for (auto &token : tokens)
					{
						sub_tokens.clear();
						boost::trim(token);
						boost::split(sub_tokens, token, boost::is_any_of("-"));
						sql_option option;
						for (auto &sub_token : sub_tokens)
						{
							if (boost::algorithm::iequals(sub_token, std::string("beguid")) == 1)
							{
								option.beguidConvert = true;
							}
							else if	(boost::algorithm::iequals(sub_token, std::string("bool")) == 1)
							{
								option.boolConvert = true;
							}
							else if	(boost::algorithm::iequals(sub_token, std::string("null")) == 1)
							{
								option.nullConvert = true;
							}
							else if (boost::algorithm::iequals(sub_token, std::string("time")) == 1)
							{
								option.timeConvert = true;
							}
							else if	(boost::algorithm::iequals(sub_token, std::string("string")) == 1)
							{
								option.stringify = true;
							}
							else if	(boost::algorithm::iequals(sub_token, std::string("string_escape_quotes")) == 1)
							{
								option.string_escape_quotes = true;
							}
							else if	(boost::algorithm::iequals(sub_token, std::string("strip")) == 1)
							{
								option.strip = true;
							}
							else if	(boost::algorithm::iequals(sub_token, std::string("mysql_escape")) == 1)
							{
								option.mysql_escape = true;
							}
							else
							{
								try
								{
									option.value_number = std::stoi(sub_token,nullptr);
								}
								catch(std::exception const &e)
								{
									#ifdef DEBUG_TESTING
										extension_ptr->console->info("extDB3: SQL_CUSTOM Config Error: Section: {0} Unknown INPUT Option: {1} in {2}", section.first, sub_token, token);
									#endif
									extension_ptr->logger->info("extDB3: SQL_CUSTOM Config Error: Section: {0} Unknown INPUT Option: {1} in {2}", section.first, sub_token, token);
									status = false;
								}
							}
						}
						if (option.value_number > 0)
						{
							if (option.value_number > highest_input_value)
							{
								highest_input_value = option.value_number;
							}
							calls[section.first].input_options.push_back(std::move(option));
						}
					}
					calls[section.first].highest_input_value = std::move(highest_input_value);
				}

				// Parse SQL OUTPUT OPTIONS
				tokens.clear();
				if (!(output_options_str.empty()))
				{
					boost::split(tokens, output_options_str, boost::is_any_of(","));
					for (auto &token : tokens)
					{
						sub_tokens.clear();
						boost::trim(token);
						boost::split(sub_tokens, token, boost::is_any_of("-"));
						sql_option option;
						for (auto &sub_token : sub_tokens)
						{
							if (boost::algorithm::iequals(sub_token, std::string("beguid")) == 1)
							{
								option.beguidConvert = true;
							}
							else if	(boost::algorithm::iequals(sub_token, std::string("bool")) == 1)
							{
								option.boolConvert = true;
							}
							else if	(boost::algorithm::iequals(sub_token, std::string("null")) == 1)
							{
								option.nullConvert = true;
							}
							else if	(boost::algorithm::iequals(sub_token, std::string("string")) == 1)
							{
								option.stringify = true;
							}
							else if	(boost::algorithm::iequals(sub_token, std::string("string_escape_quotes")) == 1)
							{
								option.string_escape_quotes = true;
							}
							else if	(boost::algorithm::iequals(sub_token, std::string("strip")) == 1)
							{
								option.strip = true;
							}
							else
							{
								try
								{
									option.value_number = std::stoi(sub_token,nullptr);
								}
								catch(std::exception const &e)
								{
									#ifdef DEBUG_TESTING
										extension_ptr->console->info("extDB3: SQL_CUSTOM Config Error: Section: {0} Unknown OUTPUT Option: {1} in {2}", section.first, sub_token, token);

									#endif
									extension_ptr->logger->info("extDB3: SQL_CUSTOM Config Error: Section: {0} Unknown OUTPUT Option: {1} in {2}", section.first, sub_token, token);
									extension_ptr->logger->info("extDB3: SQL_CUSTOM Config Error: Debug: {0}", output_options_str);
									status = false;
								}
							}
						}
						calls[section.first].output_options.push_back(std::move(option));
					}
				}

				// Foo
				++num_line;
				num_line_part = 1;
			}
			if (!sql.empty())
			{
				sql.pop_back();
			}
			calls[section.first].sql = std::move(sql);

			path = section.first + ".Prepared Statement";
			calls[section.first].preparedStatement = ptree.get(path, true);
			ptree.get_child(section.first).erase("Prepared Statement");

			path = section.first + ".Return InsertID";
			calls[section.first].returnInsertID = ptree.get(path, false);
			ptree.get_child(section.first).erase("Return InsertID");

			path = section.first + ".Strip Chars";
			calls[section.first].strip_chars = ptree.get(path, strip_chars);
			ptree.get_child(section.first).erase("Strip Chars");

			path = section.first + ".Strip Chars Mode";
			calls[section.first].strip_chars_mode = ptree.get(path, strip_chars_mode);
			ptree.get_child(section.first).erase("Strip Chars Mode");

			for (auto& value : section.second) {
				#ifdef DEBUG_TESTING
					extension_ptr->console->info("extDB3: SQL_CUSTOM Config Error: Section: {0} Unknown Setting: {1}", section.first, value.first);
				#endif
				extension_ptr->logger->info("extDB3: SQL_CUSTOM Config Error: Section: {0} Unknown Setting: {1}", section.first, value.first);
				status = false;
			}
		}
		return status;
	}
	catch (boost::property_tree::ini_parser::ini_parser_error const& e)
	{
		#ifdef DEBUG_TESTING
			extension_ptr->console->info("extDB3: SQL_CUSTOM Config Error: {0}", e.what());
		#endif
		extension_ptr->logger->info("extDB3: SQL_CUSTOM Config Error: {0}", e.what());
		return false;
	}
	catch (boost::property_tree::ptree_bad_path const &e)
	{
		#ifdef DEBUG_TESTING
			extension_ptr->console->info("extDB3: SQL_CUSTOM Config Error: {0}", e.what());
		#endif
		extension_ptr->logger->info("extDB3: SQL_CUSTOM Config Error: {0}", e.what());
		return false;
	}
}


bool SQL_CUSTOM::callProtocol(std::string input_str, std::string &result, const bool async_method, const unsigned int unique_id)
{
	#ifdef DEBUG_TESTING
		extension_ptr->console->info("extDB3: SQL_CUSTOM: Trace: UniqueID: {0} Input: {1}", unique_id, input_str);
	#endif
	#ifdef DEBUG_LOGGING
		extension_ptr->logger->info("extDB3: SQL_CUSTOM: Trace: UniqueID: {0} Input: {1}", unique_id, input_str);
	#endif

	std::string callname;
	std::string tokens_str;
	const std::string::size_type found = input_str.find(":");
	if (found != std::string::npos)
	{
		callname = input_str.substr(0, found);
		tokens_str = input_str.substr(found+1);
	}	else {
		callname = input_str;
	}
	auto calls_itr = calls.find(callname);
	if (calls_itr == calls.end())
	{
		// NO CALLNAME FOUND IN PROTOCOL
		result = "[0,\"Error No Custom Call Not Found\"]";
		extension_ptr->logger->warn("extDB3: SQL_CUSTOM: Error No Custom Call Not Found: Input String {0}", input_str);
		extension_ptr->logger->warn("extDB3: SQL_CUSTOM: Error No Custom Call Not Found: Callname {0}", callname);
		#ifdef DEBUG_TESTING
			extension_ptr->console->warn("extDB3: SQL_CUSTOM: Error No Custom Call Not Found: Input String {0}", input_str);
			extension_ptr->console->warn("extDB3: SQL_CUSTOM: Error No Custom Call Not Found: Callname {0}", callname);
		#endif
		return true;
	}

	std::vector<std::vector<std::string>> result_vec;
	try
	{
		MariaDBSession session(database_pool);

		std::vector<std::string> tokens;
		boost::split(tokens, input_str, boost::is_any_of(":"));

		if ((tokens.size()-1) != calls_itr->second.highest_input_value)
		{
			throw MariaDBStatementException2("Config Invalid Number Number of Inputs Got " + std::to_string(tokens.size()-1) + " Expected " + std::to_string(calls_itr->second.highest_input_value));
		}

		if (!calls_itr->second.preparedStatement)
		{
			// -------------------
			// Raw SQL
			// -------------------
			std::string sql_str = calls_itr->second.sql;
			std::string tmp_str;
			for (int i = 0; i < calls_itr->second.input_options.size(); ++i)
			{
				int value_number = calls_itr->second.input_options[i].value_number;
				tmp_str = tokens[value_number];
				if (calls_itr->second.input_options[i].strip)
				{
					std::string stripped_str = tmp_str;
					for (auto &strip_char : calls_itr->second.strip_chars)
					{
						boost::erase_all(stripped_str, std::string(1, strip_char));
					}
					if (stripped_str != tmp_str)
					{
						switch (calls_itr->second.strip_chars_mode)
						{
							case 2: // Log + Error
								extension_ptr->logger->warn("extDB3: SQL_CUSTOM: Error Bad Char Detected: Input: {0} Token: {1}", input_str, tmp_str);
								result = "[0,\"Error Strip Char Found\"]";
								return true;
							case 1: // Log
								extension_ptr->logger->warn("extDB3: SQL_CUSTOM: Error Bad Char Detected: Input: {0} Token: {1}", input_str, tmp_str);
						}
						tmp_str = std::move(stripped_str);
					}
				}
				if (calls_itr->second.input_options[i].beguidConvert)
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
					catch(std::exception const &e)
					{
						tmp_str = "ERROR";
					}
				}
				if (calls_itr->second.input_options[i].boolConvert)
				{
					if (boost::algorithm::iequals(tmp_str, std::string("1")) == 1)
					{
						tmp_str = "true";
					} else {
						tmp_str = "false";
					}
				}
				if (calls_itr->second.input_options[i].nullConvert)
				{
					if (tmp_str.empty())
					{
						tmp_str = "objNull";
					}
				}
				if (calls_itr->second.input_options[i].string_escape_quotes)
				{
						boost::replace_all(tmp_str, "\"", "\"\"");
				}
				if (calls_itr->second.input_options[i].stringify)
				{
						tmp_str = "\"" + tmp_str + "\"";
				}
				boost::replace_all(sql_str, ("$CUSTOM_" + std::to_string(i) + "$"), tmp_str);  //TODO Improve this
			}
			auto &session_query_itr = session.data->query;
			session.data->query.send(input_str);
			session.data->query.get(result_vec);
		} else {
			// -------------------
			// Prepared Statement
			// -------------------
			std::vector<MariaDBStatement::mysql_bind_param> processed_inputs;
			processed_inputs.resize(calls_itr->second.input_options.size());
			for (int i = 0; i < processed_inputs.size(); ++i)
			{
				processed_inputs[i].type = MYSQL_TYPE_VARCHAR;
				processed_inputs[i].buffer = std::move(tokens[calls_itr->second.input_options[i].value_number]);
				processed_inputs[i].length = processed_inputs[i].buffer.size();
				if (calls_itr->second.input_options[i].strip)
				{
					std::string stripped_str = processed_inputs[i].buffer;
					for (auto &strip_char : calls_itr->second.strip_chars)
					{
						boost::erase_all(stripped_str, std::string(1, strip_char));
					}
					if (stripped_str != processed_inputs[i].buffer)
					{
						switch (calls_itr->second.strip_chars_mode)
						{
							case 2: // Log + Error
								extension_ptr->logger->warn("extDB3: SQL_CUSTOM: Error Bad Char Detected: Input: {0} Token: {1}", input_str, processed_inputs[i].buffer);
								result = "[0,\"Error Strip Char Found\"]";
								return true;
							case 1: // Log
								extension_ptr->logger->warn("extDB3: SQL_CUSTOM: Error Bad Char Detected: Input: {0} Token: {1}", input_str, processed_inputs[i].buffer);
						}
						processed_inputs[i].buffer = std::move(stripped_str);
						processed_inputs[i].length = processed_inputs[i].buffer.size();
					}
				}
				if (calls_itr->second.input_options[i].beguidConvert)
				{
					try
					{
						int64_t steamID = std::stoll(processed_inputs[i].buffer, nullptr);
						std::stringstream bestring;
						int8_t i = 0, parts[8] = { 0 };
						do parts[i++] = steamID & 0xFF;
						while (steamID >>= 8);
						bestring << "BE";
						for (int i = 0; i < sizeof(parts); i++) {
							bestring << char(parts[i]);
						}
						processed_inputs[i].buffer = md5(bestring.str());
					}
					catch(std::exception const &e)
					{
						processed_inputs[i].buffer = "ERROR";
					}
					processed_inputs[i].length = processed_inputs[i].buffer.size();
				}
				if (calls_itr->second.input_options[i].boolConvert)
				{
					if (boost::algorithm::iequals(processed_inputs[i].buffer, std::string("true")) == 1)
					{
						processed_inputs[i].buffer = "1";
					} else {
						processed_inputs[i].buffer = "0";
					}
					processed_inputs[i].length = processed_inputs[i].buffer.size();
				}
				if (calls_itr->second.input_options[i].nullConvert)
				{
					if (processed_inputs[i].buffer.empty())
					{
						processed_inputs[i].type = MYSQL_TYPE_NULL;
					}
				}
				if (calls_itr->second.input_options[i].string_escape_quotes)
				{
						boost::replace_all(processed_inputs[i].buffer, "\"", "\"\"");
						processed_inputs[i].length = processed_inputs[i].buffer.size();
				}
				if (calls_itr->second.input_options[i].stringify)
				{
						processed_inputs[i].buffer = "\"" + processed_inputs[i].buffer + "\"";
						processed_inputs[i].length = processed_inputs[i].buffer.size();
				}
			}
			try
			{
				MariaDBStatement *session_statement_itr;
				session.data->statements.erase(callname);
				if (session.data->statements.count(callname) == 0)
				{
					session_statement_itr = &session.data->statements[callname];
					session_statement_itr->init(session.data->connector);
					session_statement_itr->create();
					session_statement_itr->prepare(calls_itr->second.sql);
				} else {
					session_statement_itr = &session.data->statements[callname];
				}
				session_statement_itr->bindParams(processed_inputs);
				session_statement_itr->execute(calls_itr->second.output_options, calls_itr->second.strip_chars, calls_itr->second.strip_chars_mode, result_vec);
			}
			catch (MariaDBStatementException &e)
			{
				session.data->statements.erase(callname);
				#ifdef DEBUG_TESTING
					extension_ptr->console->error("extDB3: SQL: Error MariaDBStatementException: {0}", e.what());
					extension_ptr->console->error("extDB3: SQL: Error MariaDBStatementException: Input: {0}", input_str);
				#endif
				extension_ptr->logger->error("extDB3: SQL: Error MariaDBStatementException: {0}", e.what());
				extension_ptr->logger->error("extDB3: SQL: Error MariaDBStatementException: Input: {0}", input_str);
				result = "[0,\"Error MariaDBStatementException Exception\"]";
				return true;
			}
			catch (MariaDBStatementException2 &e)
			{
				session.data->statements.erase(callname);
				#ifdef DEBUG_TESTING
					extension_ptr->console->error("extDB3: SQL: Error MariaDBStatementException2: {0}", e.what());
					extension_ptr->console->error("extDB3: SQL: Error MariaDBStatementException2: Input: {0}", input_str);
				#endif
				extension_ptr->logger->error("extDB3: SQL: Error MariaDBStatementException2: {0}", e.what());
				extension_ptr->logger->error("extDB3: SQL: Error MariaDBStatementException2: Input: {0}", input_str);
				result = "[0,\"Error MariaDBStatementException2 Exception\"]";
				return true;
			}
		}
		result = "[1,[";
		if (result_vec.size() > 0)
		{
			for(auto &row: result_vec)
			{
				result += "[";
				if (row.size() > 0)
				{
					for(auto &field: row)
					{
						if (field.empty())
						{
							result += "\"\"";
						} else {
							result += field;
						}
						result += ",";
					}
					result.pop_back();
				}
				result += "],";
			}
			result.pop_back();
		} else {
			result += "]";
		}
		result += "]";
		#ifdef DEBUG_TESTING
			extension_ptr->console->info("extDB3: SQL_CUSTOM: Trace: Result: {0}", result);
		#endif
		#ifdef DEBUG_LOGGING
			extension_ptr->logger->info("extDB3: SQL_CUSTOM: Trace: Result: {0}", result);
		#endif
	}
	catch (MariaDBStatementException2 &e) // Make new exception & renamed it
	{
		#ifdef DEBUG_TESTING
			extension_ptr->console->error("extDB3: SQL: Error MariaDBStatementException2: {0}", e.what());
			extension_ptr->console->error("extDB3: SQL: Error MariaDBStatementException2: Input: {0}", input_str);
		#endif
		extension_ptr->logger->error("extDB3: SQL: Error MariaDBStatementException2: {0}", e.what());
		extension_ptr->logger->error("extDB3: SQL: Error MariaDBStatementException2: Input: {0}", input_str);
		result = "[0,\"Error MariaDBStatementException2 Exception\"]";
		return true;
	}
	catch (MariaDBConnectorException &e)
	{
		#ifdef DEBUG_TESTING
			extension_ptr->console->error("extDB3: SQL: Error MariaDBConnectorException: {0}", e.what());
			extension_ptr->console->error("extDB3: SQL: Error MariaDBConnectorException: Input: {0}", input_str);
		#endif
		extension_ptr->logger->error("extDB3: SQL: Error MariaDBConnectorException: {0}", e.what());
		extension_ptr->logger->error("extDB3: SQL: Error MariaDBConnectorException: Input: {0}", input_str);
		result = "[0,\"Error MariaDBConnectorException Exception\"]";
	}
	return true;
}