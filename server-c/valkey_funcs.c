#include "valkey_funcs.h"
#include <hiredis/hiredis.h>
#include <hiredis/read.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

const char *WAITING_QUEUE_KEY = "queue";
const char *REGISTERED_HOSTS_KEY = "hosts";
const char *PROCESSING_QUEUE_KEY = "processing_queue";

const char *HOST_PREFIX = "rpi";
const char *HOST_STATUS_SUFFIX = "status";

const char *USER_PREFIX = "user";
const char *USER_LOCK_SUFFIX = "lock";
const char *USER_ASSIGNMENT_SUFFIX = "assigned_to";

int register_host(redisContext *c, char *hostname) {
  redisReply *reply = (redisReply *)redisCommand(
      c,
      "SADD %s %s",
      REGISTERED_HOSTS_KEY,
      hostname
      );
  if (reply == NULL) {
    freeReplyObject(reply);
    return 1;
  }
  freeReplyObject(reply);
  return 0;
}

int pop_waiting_queue(redisContext *c, int timeout, char *str, size_t n) {
  int rc = 0;
  redisReply *reply = (redisReply *)redisCommand(
      c,
      "BLMOVE %s %s LEFT RIGHT 0",
      WAITING_QUEUE_KEY,
      PROCESSING_QUEUE_KEY
      );
  CHECK_REPLY(c, reply, rc);

  strncpy(str, reply->str, n - 1);
  str[n - 1] = '\0';
  freeReplyObject(reply);
  reply = NULL;

  reply = (redisReply *)redisCommand(
    c,
    "SET %s:%s:%s %s NX EX %d",
    USER_PREFIX,
    str,
    USER_LOCK_SUFFIX,
    "locked",
    timeout);
  CHECK_REPLY(c, reply, rc);

cleanup:
  if (reply)
    freeReplyObject(reply);
  return rc;
}

int release_user(redisContext *c, char *username) {
  int rc = 0;
  redisReply *reply = (redisReply *)redisCommand(
    c,
    "DEL %s:%s:%s",
    USER_PREFIX,
    username,
    USER_LOCK_SUFFIX
    );
  CHECK_AND_CLEAN_REPLY(c, reply, rc);

cleanup:
  if (reply)
    freeReplyObject(reply);
  return rc;
}

redisReply *set_status_command(redisContext *c, HostStatus hostStatus,
                               int timeout) {
  return redisCommand(
    c,
    "HSETEX %s:%s:%s EX %d "
    "FIELDS 5 "
    "hostname %s "
    "is_occupied %d "
    "expiry %lu "
    "current_user %s "
    "last_timestamp %lu",
    HOST_PREFIX, hostStatus.hostname, HOST_STATUS_SUFFIX,
    timeout,
    hostStatus.hostname,
    hostStatus.is_occupied,
    hostStatus.expiry,
    hostStatus.current_user,
    hostStatus.last_timestamp
  );
}

int set_status(redisContext *c, HostStatus hostStatus, int timeout) {
  int rc = 0;
  redisReply *reply = redisCommand(c, "MULTI");
  CHECK_AND_CLEAN_REPLY(c, reply, rc);

  freeReplyObject(reply);
  if (hostStatus.is_occupied) {
    reply = redisCommand(
      c,
      "SET %s:%s:%s %s EX %d",
      USER_PREFIX,
      hostStatus.current_user,
      USER_ASSIGNMENT_SUFFIX,
      hostStatus.hostname, timeout
    );
    CHECK_AND_CLEAN_REPLY(c, reply, rc);
  }
  reply = set_status_command(c, hostStatus, timeout);
  CHECK_AND_CLEAN_REPLY(c, reply, rc);

  reply = redisCommand(c, "EXEC");
  CHECK_AND_CLEAN_REPLY(c, reply, rc);

cleanup:
  if (reply)
    freeReplyObject(reply);
  return rc;
}

// 2 == Not found, 1 == error, 0 == success
int get_status(redisContext *c, char *hostname, HostStatus *hostStatus) {
  int rc = 0;
  redisReply *reply = (redisReply *)redisCommand(
      c,
      "HGETALL %s:%s:%s",
      HOST_PREFIX,
      hostname,
      HOST_STATUS_SUFFIX
      );
  CHECK_REPLY(c, reply, rc);

  if (reply->type == REDIS_REPLY_NIL || reply->elements == 0) {
    rc = 2;
    goto cleanup;
  }

  if (reply->elements != 10) {
    printf("Wrong elements: %lu\n", reply->elements);
    rc = 1;
    goto cleanup;
  }

  strncpy(hostStatus->hostname, reply->element[1]->str, MAX_NAME_SIZE - 1);
  hostStatus->hostname[MAX_NAME_SIZE - 1] = '\0';

  hostStatus->is_occupied = strtol(reply->element[3]->str, NULL, 10);
  hostStatus->expiry = strtoul(reply->element[5]->str, NULL, 10);

  strncpy(hostStatus->current_user, reply->element[7]->str, MAX_NAME_SIZE - 1);
  hostStatus->current_user[MAX_NAME_SIZE - 1] = '\0';

  hostStatus->last_timestamp = strtoul(reply->element[9]->str, NULL, 10);

  freeReplyObject(reply);
  return 0;

cleanup:
  if (reply)
    freeReplyObject(reply);
  return rc;
}

// Set user as assigned to host, update host status, push notification and clear
// processing queue
int finish_assignment(redisContext *c, HostStatus newStatus, int timeout) {
  redisReply *reply = NULL;
  int rc = 0;
  if (!newStatus.is_occupied) {
    printf("Host %s is not occupied\n", newStatus.hostname);
    rc = 1;
    goto cleanup;
  }

  // Open transaction
  reply = redisCommand(c, "MULTI");
  CHECK_AND_CLEAN_REPLY(c, reply, rc);

  // Update user status
  reply = redisCommand(
    c,
    "SET %s:%s:%s %s EX %d", USER_PREFIX,
    newStatus.current_user,
    USER_ASSIGNMENT_SUFFIX,
    newStatus.hostname,
    timeout
    );
  CHECK_AND_CLEAN_REPLY(c, reply, rc);

  // Update host status
  reply = set_status_command(c, newStatus, timeout);
  CHECK_AND_CLEAN_REPLY(c, reply, rc);

  // Remove user from processing queu
  reply = redisCommand(
    c,
    "LREM %s 0 %s",
    PROCESSING_QUEUE_KEY,
    newStatus.current_user
    );
  CHECK_AND_CLEAN_REPLY(c, reply, rc);

  // Publish notification
  reply = redisCommand(
    c,
    "PUBLISH %s:%s %s",
    HOST_PREFIX,
    newStatus.hostname,
    newStatus.current_user
    );
  CHECK_AND_CLEAN_REPLY(c, reply, rc);

  // Commit transaction
  reply = redisCommand(c, "EXEC");
  CHECK_AND_CLEAN_REPLY(c, reply, rc);
cleanup:
  if (reply)
    freeReplyObject(reply);
  return rc;
}
