#include <cstdio>

// debug info warning error

#ifndef INFO_LEVEL
#define INFO_LEVEL 3
#endif
#if INFO_LEVEL >= 1
#define LOG_ERR(str) fprintf(stderr, "ERROR [%s:%d]: " str "\n", __FILE__, __LINE__);
#else
#define LOG_ERR
#endif
#if INFO_LEVEL >= 2
#define LOG_WARN(str) fprintf(stdout, "WARNING [%s:%d]: " str "\n", __FILE__, __LINE__);
#else
#define LOG_WARN
#endif
#if INFO_LEVEL >= 3
#define LOG_INFO(str) fprintf(stdout, "INFO [%s:%d]: " str "\n", __FILE__, __LINE__);
#else
#define LOG_INFO
#endif
#if INFO_LEVEL >= 4
#define LOG_DEBUG(str) fprintf(stderr, "DEBUG [%s:%d]: " str "\n", __FILE__, __LINE__);
#else
#define LOG_DEBUG
#endif