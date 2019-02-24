#pragma once

#ifdef NDEBUG
#define assert(x) 
#else
#include_next <assert.h>
#endif
