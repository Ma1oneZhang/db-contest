#ifndef LOGGER_H
#define LOGGER_H
#include <cstdio>

// debug info warning error
static char log_buf[1024];
#ifndef INFO_LEVEL
#define INFO_LEVEL 4
#endif
#if INFO_LEVEL >= 1
#define LOG_ERR(fmt, args...)                                               \
	sprintf(log_buf, "ERROR [%s:%d]: " fmt "\n", __FILE__, __LINE__, ##args); \
	fprintf(stderr, log_buf);
#else
#define LOG_ERR
#endif
#if INFO_LEVEL >= 2
#define LOG_WARN(fmt, args...)                                                \
	sprintf(log_buf, "WARNING [%s:%d]: " fmt "\n", __FILE__, __LINE__, ##args); \
	fprintf(stderr, log_buf);
#else
#define LOG_WARN
#endif
#if INFO_LEVEL >= 3
#define LOG_INFO(fmt, args...)                                             \
	sprintf(log_buf, "INFO [%s:%d]: " fmt "\n", __FILE__, __LINE__, ##args); \
	fprintf(stderr, log_buf);
#else
#define LOG_INFO
#endif
#if INFO_LEVEL >= 4
#define LOG_DEBUG(fmt, args...)                                             \
	sprintf(log_buf, "DEBUG [%s:%d]: " fmt "\n", __FILE__, __LINE__, ##args); \
	fprintf(stderr, log_buf);
#else
#define LOG_DEBUG
#endif
#endif