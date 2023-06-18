#ifndef LOGGER_H
#define LOGGER_H
#include <cstdio>

// debug info warning error
#ifndef INFO_LEVEL
#define INFO_LEVEL 1
#endif
#if INFO_LEVEL >= 1
#define LOG_ERR(fmt, args...)                                                 \
	{                                                                           \
		char log_buf[1024];                                                       \
		sprintf(log_buf, "ERROR [%s:%d]: " fmt "\n", __FILE__, __LINE__, ##args); \
		fprintf(stderr, "%s", log_buf);                                           \
	}
#else
#define LOG_ERR(fmt, args...)
#endif
#if INFO_LEVEL >= 2
#define LOG_WARN(fmt, args...)                                                  \
	{                                                                             \
		char log_buf[1024];                                                         \
		sprintf(log_buf, "WARNING [%s:%d]: " fmt "\n", __FILE__, __LINE__, ##args); \
		fprintf(stderr, "%s", log_buf);                                             \
	}
#else
#define LOG_WARN(fmt, args...)
#endif
#if INFO_LEVEL >= 3
#define LOG_INFO(fmt, args...)                                               \
	{                                                                          \
		char log_buf[1024];                                                      \
		sprintf(log_buf, "INFO [%s:%d]: " fmt "\n", __FILE__, __LINE__, ##args); \
		fprintf(stderr, "%s", log_buf);                                          \
	}
#else
#define LOG_INFO(fmt, args...)
#endif
#if INFO_LEVEL >= 4
#define LOG_DEBUG(fmt, args...)                                               \
	{                                                                           \
		char log_buf[1024];                                                       \
		sprintf(log_buf, "DEBUG [%s:%d]: " fmt "\n", __FILE__, __LINE__, ##args); \
		fprintf(stderr, "%s", log_buf);                                           \
	}
#else
#define LOG_DEBUG(fmt, args...)
#endif
#endif