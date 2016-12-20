#Change this if you need to target a specific CMake version

cmake_minimum_required(VERSION 3.2)
cmake_policy(SET CMP0015 NEW)

set(CMAKE_USER_MAKE_RULES_OVERRIDE
   ${CMAKE_CURRENT_SOURCE_DIR}/cmake_modules/c_flag_overrides.cmake)
set(CMAKE_USER_MAKE_RULES_OVERRIDE_CXX
   ${CMAKE_CURRENT_SOURCE_DIR}/cmake_modules/cxx_flag_overrides.cmake)

project(extDB3)

set(CMAKE_SUPPRESS_REGENERATION 1)
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE "Release" CACHE STRING
      "Choose the type of build, options are: Debug Release
RelWithDebInfo MinSizeRel."
      FORCE)
endif(NOT CMAKE_BUILD_TYPE)
set(CMAKE_CONFIGURATION_TYPES Release CACHE TYPE INTERNAL FORCE)
set(CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake_modules" ${CMAKE_MODULE_PATH})

#Check for 32bit vs. 64bit system
if (CMAKE_SIZEOF_VOID_P EQUAL 8) #64bit
	if(${WIN32})
		message(ERROR "Warning: You are on a 64-bit system, make sure you are pulling in the appropriate 64-bit libraries.")
	else()
		message(FATAL_ERROR "Warning: Arma Linuxserver doesn't support 64bit yet.")
	endif()
elseif (CMAKE_SIZEOF_VOID_P EQUAL 4) #32bit
	message(STATUS "You are on a 32-bit system.")
else()
	message(STATUS "CMake wasn't able to identify if you are on 32-bit or 64-bit...")
endif()


file(GLOB_RECURSE SOURCES src/*.cpp)

include_directories(${PROJECT_SOURCE_DIR}/3rdparty)

SET(COMPILE_EXTENSION TRUE CACHE BOOL "Compile extDB3")
SET(COMPILE_TEST_APPLICATION FALSE CACHE BOOL "Compiles a test application of extDB3.")

SET(DEBUG_LOGGING FALSE CACHE BOOL "Enable Debug Logging.")
SET(DEBUG_TESTING FALSE CACHE BOOL "Enable Extra Console Output.")

if(${WIN32})
	set(TBBMALLOC_OFFICAL FALSE CACHE BOOL "Use Offical TTBMalloc")
endif()


if (DEBUG_LOGGING)
	add_definitions(-DDEBUG_LOGGING)
	if(${WIN32})
		SET(RESOURCE_FILE ../src/dll-debug.rc)
	endif()
	message(STATUS "Debug Logging is enabled.")
else()
	if(${WIN32})
		SET(RESOURCE_FILE ../src/dll.rc)
	endif()
endif()
if (DEBUG_TESTING)
	add_definitions(-DDEBUG_TESTING)
	message(STATUS "Testing flag is enabled.")
endif()


if (COMPILE_TEST_APPLICATION)
	set(EXECUTABLE_NAME "extDB3-test")
	add_executable(${EXECUTABLE_NAME} ${SOURCES})
	add_definitions(-DTEST_APP)
	message(STATUS "Building Test Application")
elseif (COMPILE_RCON_APPLICATION)
	SET(SOURCES ../src/backends/rcon.cpp) # Override Sources
	set(EXECUTABLE_NAME "extDB3-rcon")
	add_executable(${EXECUTABLE_NAME} ${SOURCES})
	add_definitions(-DRCON_APP)
	message(STATUS "Building Rcon Applicaton")
elseif (COMPILE_SANITIZE_APPLICATION)
	SET(SOURCES ../src/sanitize.cpp) # Override Sources
	set(EXECUTABLE_NAME "extDB3-sanitize")
	add_executable(${EXECUTABLE_NAME} ${SOURCES})
	add_definitions(-DSANITIZE_APP)
	message(STATUS "Building Sanitize Applicaton")
else()
	LIST(APPEND SOURCES ${RESOURCE_FILE})
	LIST(APPEND SOURCES ../src/main.cpp)
	set(EXECUTABLE_NAME "extDB3")
	add_library (${EXECUTABLE_NAME} SHARED ${SOURCES})
	message(STATUS "Building Test Application")
endif()


# -----------------------
# START STATIC LIBRARIES
# -----------------------
if(${UNIX})
     set(CMAKE_FIND_LIBRARY_SUFFIXES ".a")
endif()

# BOOST
set(Boost_USE_STATIC_LIBS ON)
set(Boost_USE_MULTITHREADED ON)
set(Boost_USE_STATIC_RUNTIME ON)
if(${WIN32})
	set(BOOST_ROOT "C:/local/boost_1_62_0")
	if (CMAKE_CL_64)
		set(BOOST_LIBRARYDIR "C:/local/boost_1_62_0/lib64-msvc-14.0")
	else()
  	set(BOOST_LIBRARYDIR "C:/local/boost_1_62_0/lib32-msvc-14.0")
	endif()
	add_definitions(-D _WIN32_WINNT=0x0600)
endif()
if (COMPILE_EXTENSION)
	find_package(Boost REQUIRED COMPONENTS chrono date_time filesystem random regex system thread)
else()
	find_package(Boost REQUIRED COMPONENTS chrono date_time filesystem program_options random regex system thread)
endif()
if(Boost_FOUND)
	include_directories(${Boost_INCLUDE_DIRS})
else()
	message(FATAL_ERROR "\nBOOST not found\n Please Set BOOST_ROOT & BOOST_LIBRARYDIR")
endif()

# MYSQL
include(FindMariaDB)  # mariadb-connector-c-fbf6fd1598914801414b0b0a7f2a169b0506dc4c
if(MYSQL_FOUND)
	include_directories(${MYSQL_INCLUDE_DIR})
else()
	message(FATAL_ERROR "\nMYSQL not found")
endif()

# MySQL
#include(FindMySQL)
#if(MYSQL_FOUND)
#	include_directories(${MYSQL_INCLUDE_DIR})
#else()
#	message(FATAL_ERROR "\nMySQL not found")
#endif()

if(${UNIX})
	set(CMAKE_FIND_LIBRARY_SUFFIXES ".so")
endif()


# -----------------------
# END STATIC LIBRARIES
# -----------------------


# -----------------------
# START DYNAMIC LIBRARIES
# -----------------------

set (TBB_SEARCH TRUE)
add_definitions(-D TBB_MALLOC)
if(${WIN32})
	if(TBBMALLOC_OFFICAL)
	else()
		if (CMAKE_CL_64)
			set(TBB_MALLOC_LIBRARY "C:/local/tbb2017_20161128oss/build/vs2012/x64/Release/tbbmalloc_x64.lib")
		else()
			set(TBB_MALLOC_LIBRARY "C:/local/tbb2017_20161128oss/build/vs2012/Win32/Release/tbbmalloc.lib")
		endif()
		include_directories("C:/local/tbb2017_20161128oss/include")
		set (TBB_SEARCH FALSE)
	endif()
endif()

if(TBB_SEARCH)
	include(FindTBB)
	if(TBB_FOUND)
		include_directories(${TBB_INCLUDE_DIRS})
	else()
	 message(FATAL_ERROR "\nIntel TBB not found\n Please Set TBB_ROOT")
	endif()
endif()

# -----------------------
# END DYNAMIC LIBRARIES
# -----------------------



if(${WIN32})
	# Windows
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} /wd4996")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /wd4996")
	target_link_libraries(${EXECUTABLE_NAME} ${TBB_MALLOC_LIBRARY}  ${Boost_LIBRARIES} ${MYSQL_LIBRARY} Iphlpapi)
	add_definitions(-DUNICODE -D_UNICODE -DWIN32_LEAN_AND_MEAN)
	SET_TARGET_PROPERTIES(${EXECUTABLE_NAME} PROPERTIES LINK_FLAGS " /MANIFEST:NO /ERRORREPORT:NONE")
else()
	# Linux
	target_link_libraries(${EXECUTABLE_NAME} -Wl,-Bstatic ${Boost_LIBRARIES} ${MYSQL_LIBRARY}  -Wl,-Bdynamic ${TBB_MALLOC_LIBRARY} -ldl -pthread -lcrypto -lssl -lz)
	set(CMAKE_CXX_FLAGS "-std=c++0x -static-libstdc++ -static-libgcc ${CMAKE_CXX_FLAGS}")

	if (NOT COMPILE_TEST_APPLICATION)
		ADD_CUSTOM_COMMAND(
			TARGET ${EXECUTABLE_NAME}
			POST_BUILD
			COMMAND ${CMAKE_COMMAND} -E rename libextDB3.so extDB3.so
		)
	endif()
endif()

mark_as_advanced(CMAKE_INSTALL_PREFIX)
mark_as_advanced(CMAKE_CONFIGURATION_TYPES)


message(STATUS "C_FLAGS:  =${CMAKE_C_FLAGS}")
message(STATUS "CMAKE_C_FLAGS_DEBUG:=${CMAKE_C_FLAGS_DEBUG}")
message(STATUS "CMAKE_C_FLAGS_RELEASE:=${CMAKE_C_FLAGS_RELEASE}")
message(STATUS "CMAKE_C_FLAGS_MINSIZEREL:=${CMAKE_C_FLAGS_MINSIZEREL}")
message(STATUS "CMAKE_C_FLAGS_RELWITHDEBINFO:=${CMAKE_C_FLAGS_RELWITHDEBINFO}")
message(STATUS "")
message(STATUS "")
message(STATUS "CXX_FLAGS:=${CMAKE_CXX_FLAGS}")
message(STATUS "CMAKE_CXX_FLAGS_DEBUG:=${CMAKE_CXX_FLAGS_DEBUG}")
message(STATUS "CMAKE_CXX_FLAGS_RELEASE:=${CMAKE_CXX_FLAGS_RELEASE}")
message(STATUS "CMAKE_CXX_FLAGS_MINSIZEREL:=${CMAKE_CXX_FLAGS_MINSIZEREL}")
message(STATUS "CMAKE_CXX_FLAGS_RELWITHDEBINFO:=${CMAKE_CXX_FLAGS_RELWITHDEBINFO}")
