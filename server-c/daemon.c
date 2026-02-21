#include "valkey_funcs.h"
#include <bits/pthreadtypes.h>
#include <errno.h>
#include <hiredis/hiredis.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#define HEARTBEAT_COOLDOWN 3

pthread_mutex_t status_lock;

#define CHECK_RESULT(rc)                                                       \
  do {                                                                         \
    if (rc == 1) {                                                             \
      fprintf(stderr, "Error: %d\n", rc);                                      \
      exit(EXIT_FAILURE);                                                      \
    }                                                                          \
  } while (0)

// TODO: username might have problems with null terminators and rewriting
struct AppStatus {
  HostStatus status;
  char whitelist_path[256];
  char redis_ip[MAX_NAME_SIZE];
  int redis_port;
  int time_slice;
} status;

redisContext *create_client(const char *ip, int port) {
  redisContext *c = redisConnect(ip, port);
  if (c == NULL || c->err) {
    if (c) {
      fprintf(stderr, "Redis connection error: %s\n", c->errstr);
      redisFree(c);
    } else {
      fprintf(stderr, "Redis connection error: can't allocate redis context\n");
    }
    exit(EXIT_FAILURE);
  }
  return c;
}

void *update_heartbeat_loop(void *arg) {
  printf("Starting heartbeat loop\n");
  redisContext *c = create_client(status.redis_ip, status.redis_port);
  while (1) {
    pthread_mutex_lock(&status_lock);
    status.status.last_timestamp = time(NULL);
    pthread_mutex_unlock(&status_lock);
    set_status(c, status.status, 5);
    sleep(HEARTBEAT_COOLDOWN);
  }
}

void set_user(const char *user) {
  pthread_mutex_lock(&status_lock);
  status.status.is_occupied = true;
  status.status.expiry = time(NULL) + status.time_slice;
  strncpy(status.status.current_user, user, MAX_NAME_SIZE);
  pthread_mutex_unlock(&status_lock);
}

void unset_user(FILE **fp) {
  pthread_mutex_lock(&status_lock);
  status.status.is_occupied = false;
  status.status.expiry = -1;
  status.status.current_user[0] = '\0';
  pthread_mutex_unlock(&status_lock);
  *fp = freopen(NULL, "w", *fp);
}

void sleep_timer(redisContext *c, FILE **fp) {
  time_t sleep_duration = status.status.expiry - time(NULL);
  if (sleep_duration < 0) {
    printf("User %s has already expired\n", status.status.current_user);
    return;
  }

  printf("Sleeping for %ld seconds\n", sleep_duration);
  sleep(sleep_duration);
  printf("Releasing user\n");

  char command[256];
  // sprintf(command, "pkill -SIGTERM -u %s", status.status.current_user);
  // int rc = system(command);
  // if (rc == -1) {
  //   fprintf(stderr, "Failed to execute command: %s\n", strerror(errno));
  // } else if (WIFEXITED(rc) && WEXITSTATUS(rc) != 0) {
  //   fprintf(stderr, "Command exited with status: %d\n", WEXITSTATUS(rc));
  // } else if (WIFSIGNALED(rc)) {
  //   fprintf(stderr, "Command killed by signal: %d\n", WTERMSIG(rc));
  // }

  unset_user(fp);
  int rc = release_user(c, status.status.current_user);
  CHECK_RESULT(rc);
}

void *fetch_user_loop(void *arg) {
  redisContext *c = create_client(status.redis_ip, status.redis_port);
  FILE *fp = fopen(status.whitelist_path, "w");
  if (!fp) {
    fprintf(stderr, "Failed to open whitelist file: %s\n", strerror(errno));
    exit(EXIT_FAILURE);
  }

  HostStatus last_status;
  register_host(c, status.status.hostname);

  int rc = get_status(c, status.status.hostname, &last_status);
  CHECK_RESULT(rc);
  if (rc == 0) {
    printf("Recovering last status\n");
    pthread_mutex_lock(&status_lock);
    status.status = last_status;
    pthread_mutex_unlock(&status_lock);
  }

  while (true) {
    if (!status.status.is_occupied) {
      char next_user[MAX_NAME_SIZE];
      printf("Waiting for user\n");
      rc =
          pop_waiting_queue(c, status.time_slice, next_user, sizeof(next_user));
      CHECK_RESULT(rc);
      printf("Found user: %s\n", next_user);
      set_user(next_user);
      finish_assignment(c, status.status, 10);
    }

    int n = fwrite(status.status.current_user, 1,
                   strlen(status.status.current_user), fp);
    if (n <= 0) {
      fprintf(stderr, "Failed to write to whitelist: %s\n", strerror(errno));
      exit(EXIT_FAILURE);
    }
    fflush(fp);
    sleep_timer(c, &fp);
  }
}

int main(int argc, char *argv[]) {

  pthread_mutex_init(&status_lock, NULL);

  strcpy(status.redis_ip, "127.0.0.1");
  status.redis_port = 6379;
  status.time_slice = 5;
  status.status.is_occupied = false;
  status.status.expiry = -1;
  status.status.current_user[0] = '\0';
  gethostname(status.status.hostname, MAX_NAME_SIZE);
  status.status.last_timestamp = time(NULL);
  strcpy(status.whitelist_path, "/tmp/whitelist.txt");

  pthread_t heartbeat_thread, user_thread;
  int rc = pthread_create(&heartbeat_thread, NULL, update_heartbeat_loop, NULL);
  if (rc != 0) {
    fprintf(stderr, "Failed to create heartbeat thread: %s\n", strerror(rc));
    exit(EXIT_FAILURE);
  }
  rc = pthread_create(&user_thread, NULL, fetch_user_loop, NULL);
  if (rc != 0) {
    fprintf(stderr, "Failed to create user thread: %s\n", strerror(rc));
    exit(EXIT_FAILURE);
  }

  pthread_join(heartbeat_thread, NULL);
  pthread_join(user_thread, NULL);

  pthread_mutex_destroy(&status_lock);

  return EXIT_SUCCESS;
}
