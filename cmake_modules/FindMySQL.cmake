# - Find mysqlclient
# Find the native MYSQL includes and library
#
#  MYSQL_INCLUDE_DIR - where to find mysql.h, etc.
#  MYSQL_LIBRARIES   - List of libraries when using MYSQL.
#  MYSQL_FOUND       - True if MYSQL found.

FIND_PATH(MYSQL_INCLUDE_DIR NAMES mysql.h
  PATHS
  /usr/include/mysql
  /usr/local/include/mysql
  /usr/local/mysql/include
  "C:/Program Files (x86)/MYSQL/include"
  C:/MYSQL/include
)

FIND_PATH(MYSQL_LIB_DIR NAMES mysqlclient.lib  mysqlclient.a libmysqlclient.a
  PATHS
  /usr/lib
  /usr/lib/i386-linux-gnu
  /usr/local/lib
  /usr/local/mysql/lib
  "C:/Program Files (x86)/MYSQL/lib"
  C:/MYSQL/lib
)

SET(MYSQL_NAMES mysqlclient mysqlclient_r)
FIND_LIBRARY(MYSQL_LIBRARY
  NAMES ${MYSQL_NAMES}
  PATHS /usr/lib /usr/local/lib /usr/local/mysql/lib "C:/Program Files (x86)/MYSQL/lib" C:/MYSQL/lib
  PATH_SUFFIXES mysql
)

IF (MYSQL_INCLUDE_DIR AND MYSQL_LIBRARY)
  SET(MYSQL_FOUND TRUE)
  SET(MYSQL_LIBRARIES ${MYSQL_LIBRARY} )
ELSE (MYSQL_INCLUDE_DIR AND MYSQL_LIBRARY)
  SET(MYSQL_FOUND FALSE)
  SET( MYSQL_LIBRARIES )
ENDIF (MYSQL_INCLUDE_DIR AND MYSQL_LIBRARY)

IF (MYSQL_FOUND)
  IF (NOT MYSQL_FIND_QUIETLY)
    MESSAGE(STATUS "Found MYSQL: ${MYSQL_LIBRARY}")
  ENDIF (NOT MYSQL_FIND_QUIETLY)
ELSE (MYSQL_FOUND)
  IF (MYSQL_FIND_REQUIRED)
    MESSAGE(STATUS "Looked for MYSQL libraries named ${MYSQL_NAMES}.")
    MESSAGE(FATAL_ERROR "Could NOT find MYSQL library")
  ENDIF (MYSQL_FIND_REQUIRED)
ENDIF (MYSQL_FOUND)

MARK_AS_ADVANCED(
  MYSQL_LIBRARY
  MYSQL_LIB_DIR
  MYSQL_INCLUDE_DIR
  )
