idf_component_register(SRCS "esp32.c" "sqlite3.c"
                       INCLUDE_DIRS "include"
                       PRIV_INCLUDE_DIRS ".")

target_compile_options(${COMPONENT_LIB} PRIVATE -DSQLITE_OS_OTHER -DSQLITE_OMIT_LOAD_EXTENSION -DNDEBUG)