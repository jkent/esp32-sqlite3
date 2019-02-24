#pragma once

#include "sqlite3.h"

sqlite3_mutex_methods const *sqlite3FreertosMutex(void);
