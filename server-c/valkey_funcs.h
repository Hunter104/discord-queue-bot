#pragma once
#include <hiredis/hiredis.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#define MAX_NAME_SIZE 256

#define CHECK_REPLY(ctx, reply, rc)                                            \
  do {                                                                         \
    if (!(reply)) {                                                            \
      fprintf(stderr, "Redis error: %s\n", (ctx)->errstr);                     \
      rc = 1;                                                                  \
      goto cleanup;                                                            \
    }                                                                          \
    if ((reply)->type == REDIS_REPLY_ERROR) {                                  \
      fprintf(stderr, "Redis error: %s\n", (reply)->str);                      \
      rc = 1;                                                                  \
      goto cleanup;                                                            \
    }                                                                          \
  } while (0)

#define CHECK_AND_CLEAN_REPLY(ctx, reply, rc)                                  \
  do {                                                                         \
    CHECK_REPLY(ctx, reply, rc);                                               \
    if (reply) {                                                               \
      freeReplyObject(reply);                                                  \
      reply = NULL;                                                            \
    }                                                                          \
  } while (0)

extern const char *WAITING_QUEUE_KEY;
extern const char *REGISTERED_HOSTS_KEY;
extern const char *PROCESSING_QUEUE_KEY;

extern const char *HOST_PREFIX;
extern const char *HOST_STATUS_SUFFIX;

extern const char *USER_PREFIX;
extern const char *USER_LOCK_SUFFIX;
extern const char *USER_ASSIGNMENT_SUFFIX;

typedef struct HostStatus {
  char hostname[MAX_NAME_SIZE];
  bool is_occupied;
  uint64_t expiry;
  char current_user[MAX_NAME_SIZE];
  uint64_t last_timestamp;
} HostStatus;

int register_host(redisContext *c, char *hostname);

int pop_waiting_queue(redisContext *c, int timeout, char *str, size_t n);

int release_user(redisContext *c, char *username);

redisReply *set_status_command(redisContext *c, HostStatus hostStatus,
                               int timeout);

int set_status(redisContext *c, HostStatus hostStatus, int timeout);

/*
 * Return codes:
 * 0 = success
 * 1 = error
 * 2 = not found
 */
int get_status(redisContext *c, char *hostname, HostStatus *hostStatus);

int finish_assignment(redisContext *c, HostStatus newStatus, int timeout);
