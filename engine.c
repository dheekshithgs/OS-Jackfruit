/*
 * engine.c - Supervised Multi-Container Runtime (User Space)
 *
 * Full implementation covering:
 *   Task 1: Multi-container runtime with parent supervisor
 *   Task 2: Supervisor CLI and signal handling (UNIX domain socket IPC)
 *   Task 3: Bounded-buffer logging pipeline (producer/consumer threads)
 *   Task 4: Kernel monitor integration (ioctl register/unregister)
 *   Task 6: Resource cleanup
 */

#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mount.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "monitor_ioctl.h"

#define STACK_SIZE          (1024 * 1024)
#define CONTAINER_ID_LEN    32
#define CONTROL_PATH        "/tmp/mini_runtime.sock"
#define LOG_DIR             "logs"
#define CONTROL_MESSAGE_LEN 256
#define CHILD_COMMAND_LEN   256
#define LOG_CHUNK_SIZE      4096
#define LOG_BUFFER_CAPACITY 16
#define DEFAULT_SOFT_LIMIT  (40UL << 20)
#define DEFAULT_HARD_LIMIT  (64UL << 20)

typedef enum {
    CMD_SUPERVISOR = 0,
    CMD_START,
    CMD_RUN,
    CMD_PS,
    CMD_LOGS,
    CMD_STOP
} command_kind_t;

typedef enum {
    CONTAINER_STARTING = 0,
    CONTAINER_RUNNING,
    CONTAINER_STOPPED,
    CONTAINER_KILLED,
    CONTAINER_EXITED
} container_state_t;

typedef struct container_record {
    char                    id[CONTAINER_ID_LEN];
    pid_t                   host_pid;
    time_t                  started_at;
    container_state_t       state;
    unsigned long           soft_limit_bytes;
    unsigned long           hard_limit_bytes;
    int                     exit_code;
    int                     exit_signal;
    int                     stop_requested;
    char                    log_path[PATH_MAX];
    struct container_record *next;
} container_record_t;

typedef struct {
    char   container_id[CONTAINER_ID_LEN];
    size_t length;
    char   data[LOG_CHUNK_SIZE];
} log_item_t;

typedef struct {
    log_item_t      items[LOG_BUFFER_CAPACITY];
    size_t          head;
    size_t          tail;
    size_t          count;
    int             shutting_down;
    pthread_mutex_t mutex;
    pthread_cond_t  not_empty;
    pthread_cond_t  not_full;
} bounded_buffer_t;

typedef struct {
    command_kind_t kind;
    char           container_id[CONTAINER_ID_LEN];
    char           rootfs[PATH_MAX];
    char           command[CHILD_COMMAND_LEN];
    unsigned long  soft_limit_bytes;
    unsigned long  hard_limit_bytes;
    int            nice_value;
} control_request_t;

typedef struct {
    int  status;
    char message[CONTROL_MESSAGE_LEN];
} control_response_t;

typedef struct {
    char  id[CONTAINER_ID_LEN];
    char  rootfs[PATH_MAX];
    char  command[CHILD_COMMAND_LEN];
    int   nice_value;
    int   log_write_fd;
} child_config_t;

typedef struct {
    int              pipe_fd;
    char             container_id[CONTAINER_ID_LEN];
    bounded_buffer_t *buffer;
} producer_arg_t;

typedef struct {
    int                 server_fd;
    int                 monitor_fd;
    volatile int        should_stop;
    pthread_t           logger_thread;
    bounded_buffer_t    log_buffer;
    pthread_mutex_t     metadata_lock;
    container_record_t *containers;
} supervisor_ctx_t;

static supervisor_ctx_t *g_ctx = NULL;

static void usage(const char *prog)
{
    fprintf(stderr,
            "Usage:\n"
            "  %s supervisor <base-rootfs>\n"
            "  %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s run   <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s ps\n"
            "  %s logs <id>\n"
            "  %s stop <id>\n",
            prog, prog, prog, prog, prog, prog);
}

static int parse_mib_flag(const char *flag, const char *value, unsigned long *target_bytes)
{
    char *end = NULL;
    unsigned long mib;
    errno = 0;
    mib = strtoul(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0') {
        fprintf(stderr, "Invalid value for %s: %s\n", flag, value);
        return -1;
    }
    if (mib > ULONG_MAX / (1UL << 20)) {
        fprintf(stderr, "Value for %s is too large: %s\n", flag, value);
        return -1;
    }
    *target_bytes = mib * (1UL << 20);
    return 0;
}

static int parse_optional_flags(control_request_t *req, int argc, char *argv[], int start_index)
{
    int i;
    for (i = start_index; i < argc; i += 2) {
        char *end = NULL;
        long nice_value;
        if (i + 1 >= argc) { fprintf(stderr, "Missing value for option: %s\n", argv[i]); return -1; }
        if (strcmp(argv[i], "--soft-mib") == 0) {
            if (parse_mib_flag("--soft-mib", argv[i + 1], &req->soft_limit_bytes) != 0) return -1;
            continue;
        }
        if (strcmp(argv[i], "--hard-mib") == 0) {
            if (parse_mib_flag("--hard-mib", argv[i + 1], &req->hard_limit_bytes) != 0) return -1;
            continue;
        }
        if (strcmp(argv[i], "--nice") == 0) {
            errno = 0;
            nice_value = strtol(argv[i + 1], &end, 10);
            if (errno != 0 || end == argv[i + 1] || *end != '\0' || nice_value < -20 || nice_value > 19) {
                fprintf(stderr, "Invalid value for --nice (expected -20..19): %s\n", argv[i + 1]);
                return -1;
            }
            req->nice_value = (int)nice_value;
            continue;
        }
        fprintf(stderr, "Unknown option: %s\n", argv[i]);
        return -1;
    }
    if (req->soft_limit_bytes > req->hard_limit_bytes) {
        fprintf(stderr, "Invalid limits: soft limit cannot exceed hard limit\n");
        return -1;
    }
    return 0;
}

static const char *state_to_string(container_state_t state)
{
    switch (state) {
    case CONTAINER_STARTING: return "starting";
    case CONTAINER_RUNNING:  return "running";
    case CONTAINER_STOPPED:  return "stopped";
    case CONTAINER_KILLED:   return "killed";
    case CONTAINER_EXITED:   return "exited";
    default:                 return "unknown";
    }
}

/* ---------------------------------------------------------------
 * Bounded buffer
 * Synchronisation: mutex + two condition variables (not_full, not_empty).
 * Producers block on not_full; consumers block on not_empty.
 * Shutdown broadcasts both CVs so all waiters can observe the flag.
 * --------------------------------------------------------------- */
static int bounded_buffer_init(bounded_buffer_t *buffer)
{
    int rc;
    memset(buffer, 0, sizeof(*buffer));
    rc = pthread_mutex_init(&buffer->mutex, NULL);
    if (rc != 0) return rc;
    rc = pthread_cond_init(&buffer->not_empty, NULL);
    if (rc != 0) { pthread_mutex_destroy(&buffer->mutex); return rc; }
    rc = pthread_cond_init(&buffer->not_full, NULL);
    if (rc != 0) { pthread_cond_destroy(&buffer->not_empty); pthread_mutex_destroy(&buffer->mutex); return rc; }
    return 0;
}

static void bounded_buffer_destroy(bounded_buffer_t *buffer)
{
    pthread_cond_destroy(&buffer->not_full);
    pthread_cond_destroy(&buffer->not_empty);
    pthread_mutex_destroy(&buffer->mutex);
}

static void bounded_buffer_begin_shutdown(bounded_buffer_t *buffer)
{
    pthread_mutex_lock(&buffer->mutex);
    buffer->shutting_down = 1;
    pthread_cond_broadcast(&buffer->not_empty);
    pthread_cond_broadcast(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
}

/*
 * bounded_buffer_push
 *
 * Blocks while full (unless shutting down, in which case the item is
 * discarded and -1 is returned so the caller can exit).
 */
int bounded_buffer_push(bounded_buffer_t *buffer, const log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);
    while (buffer->count == LOG_BUFFER_CAPACITY && !buffer->shutting_down)
        pthread_cond_wait(&buffer->not_full, &buffer->mutex);

    if (buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }

    buffer->items[buffer->tail] = *item;
    buffer->tail = (buffer->tail + 1) % LOG_BUFFER_CAPACITY;
    buffer->count++;
    pthread_cond_signal(&buffer->not_empty);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

/*
 * bounded_buffer_pop
 *
 * Returns 1 with an item on success.
 * Returns 0 when shutdown is signalled AND the buffer is empty (drain complete).
 */
int bounded_buffer_pop(bounded_buffer_t *buffer, log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);
    while (buffer->count == 0 && !buffer->shutting_down)
        pthread_cond_wait(&buffer->not_empty, &buffer->mutex);

    if (buffer->count == 0) {
        pthread_mutex_unlock(&buffer->mutex);
        return 0;
    }

    *item = buffer->items[buffer->head];
    buffer->head = (buffer->head + 1) % LOG_BUFFER_CAPACITY;
    buffer->count--;
    pthread_cond_signal(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
    return 1;
}

/*
 * logging_thread (consumer)
 *
 * Drains log_item_t records from the bounded buffer and appends them to
 * per-container log files under logs/<id>.log.  Exits when the buffer
 * is shut down and fully drained.
 */
void *logging_thread(void *arg)
{
    supervisor_ctx_t *ctx = (supervisor_ctx_t *)arg;
    log_item_t item;

    mkdir(LOG_DIR, 0755);

    while (1) {
        int rc = bounded_buffer_pop(&ctx->log_buffer, &item);
        if (rc == 0)
            break;  /* shutdown + drained */

        char log_path[PATH_MAX];
        snprintf(log_path, sizeof(log_path), "%s/%s.log", LOG_DIR, item.container_id);

        int fd = open(log_path, O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (fd < 0) {
            fprintf(stderr, "[logger] open(%s): %s\n", log_path, strerror(errno));
            continue;
        }
        ssize_t written = 0;
        while (written < (ssize_t)item.length) {
            ssize_t n = write(fd, item.data + written, item.length - (size_t)written);
            if (n < 0) break;
            written += n;
        }
        close(fd);
    }

    fprintf(stderr, "[logger] Consumer thread done.\n");
    return NULL;
}

/*
 * producer_thread
 *
 * One per container.  Reads from the container's stdout/stderr pipe and
 * pushes chunks into the bounded buffer.  Exits when the pipe's write end
 * is closed (container exited).
 */
static void *producer_thread(void *arg)
{
    producer_arg_t *parg = (producer_arg_t *)arg;
    log_item_t item;

    while (1) {
        memset(&item, 0, sizeof(item));
        strncpy(item.container_id, parg->container_id, CONTAINER_ID_LEN - 1);
        ssize_t n = read(parg->pipe_fd, item.data, LOG_CHUNK_SIZE);
        if (n <= 0)
            break;
        item.length = (size_t)n;
        bounded_buffer_push(parg->buffer, &item);
    }

    close(parg->pipe_fd);
    free(parg);
    return NULL;
}

/* ---------------------------------------------------------------
 * Monitor ioctl helpers
 * --------------------------------------------------------------- */
int register_with_monitor(int monitor_fd, const char *container_id, pid_t host_pid,
                           unsigned long soft_limit_bytes, unsigned long hard_limit_bytes)
{
    struct monitor_request req;
    memset(&req, 0, sizeof(req));
    req.pid              = host_pid;
    req.soft_limit_bytes = soft_limit_bytes;
    req.hard_limit_bytes = hard_limit_bytes;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);
    if (ioctl(monitor_fd, MONITOR_REGISTER, &req) < 0) return -1;
    return 0;
}

int unregister_from_monitor(int monitor_fd, const char *container_id, pid_t host_pid)
{
    struct monitor_request req;
    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);
    if (ioctl(monitor_fd, MONITOR_UNREGISTER, &req) < 0) return -1;
    return 0;
}

/* ---------------------------------------------------------------
 * Container child entrypoint (executes inside the cloned namespaces)
 *
 * Steps:
 *   1. Redirect stdout/stderr to the supervisor pipe.
 *   2. Set hostname (UTS namespace).
 *   3. Mount /proc inside rootfs (PID namespace).
 *   4. chroot() into the container rootfs (mount namespace).
 *   5. Apply nice value.
 *   6. execvp() the requested command.
 * --------------------------------------------------------------- */
int child_fn(void *arg)
{
    child_config_t *cfg = (child_config_t *)arg;

    /* Redirect stdout/stderr to logging pipe */
    if (dup2(cfg->log_write_fd, STDOUT_FILENO) < 0 ||
        dup2(cfg->log_write_fd, STDERR_FILENO) < 0)
        _exit(1);
    close(cfg->log_write_fd);

    /* Set hostname via UTS namespace */
    sethostname(cfg->id, strlen(cfg->id));

    /* Mount /proc inside rootfs so tools like ps work */
    char proc_path[PATH_MAX];
    snprintf(proc_path, sizeof(proc_path), "%s/proc", cfg->rootfs);
    mkdir(proc_path, 0555);
    if (mount("proc", proc_path, "proc", 0, NULL) < 0)
        fprintf(stderr, "[%s] Warning: mount /proc: %s\n", cfg->id, strerror(errno));

    /* chroot into container rootfs */
    if (chdir(cfg->rootfs) < 0 || chroot(".") < 0) {
        fprintf(stderr, "[%s] chroot failed: %s\n", cfg->id, strerror(errno));
        _exit(1);
    }
    chdir("/");

    /* Apply scheduling priority */
    if (cfg->nice_value != 0)
        nice(cfg->nice_value);

    /* Tokenise and exec the command */
    char cmd_copy[CHILD_COMMAND_LEN];
    strncpy(cmd_copy, cfg->command, CHILD_COMMAND_LEN - 1);
    cmd_copy[CHILD_COMMAND_LEN - 1] = '\0';

    char *exec_argv[64];
    int   exec_argc = 0;
    char *tok = strtok(cmd_copy, " \t");
    while (tok && exec_argc < 63) { exec_argv[exec_argc++] = tok; tok = strtok(NULL, " \t"); }
    exec_argv[exec_argc] = NULL;

    if (exec_argc == 0) { fprintf(stderr, "[%s] Empty command\n", cfg->id); _exit(1); }

    execvp(exec_argv[0], exec_argv);
    fprintf(stderr, "[%s] execvp(%s) failed: %s\n", cfg->id, exec_argv[0], strerror(errno));
    _exit(127);
}

/* ---------------------------------------------------------------
 * Metadata helpers (call with metadata_lock held)
 * --------------------------------------------------------------- */
static container_record_t *find_container(supervisor_ctx_t *ctx, const char *id)
{
    container_record_t *c = ctx->containers;
    while (c) { if (strcmp(c->id, id) == 0) return c; c = c->next; }
    return NULL;
}

static container_record_t *alloc_container_record(const control_request_t *req)
{
    container_record_t *rec = calloc(1, sizeof(*rec));
    if (!rec) return NULL;
    strncpy(rec->id, req->container_id, CONTAINER_ID_LEN - 1);
    rec->state            = CONTAINER_STARTING;
    rec->started_at       = time(NULL);
    rec->soft_limit_bytes = req->soft_limit_bytes;
    rec->hard_limit_bytes = req->hard_limit_bytes;
    rec->exit_code        = -1;
    snprintf(rec->log_path, sizeof(rec->log_path), "%s/%s.log", LOG_DIR, req->container_id);
    return rec;
}

/* ---------------------------------------------------------------
 * Launch a container
 * --------------------------------------------------------------- */
static int launch_container(supervisor_ctx_t *ctx,
                             container_record_t *rec,
                             const control_request_t *req)
{
    int pipefd[2];
    if (pipe(pipefd) < 0) { perror("pipe"); return -1; }

    child_config_t *cfg = calloc(1, sizeof(*cfg));
    if (!cfg) { close(pipefd[0]); close(pipefd[1]); return -1; }
    strncpy(cfg->id,      req->container_id, CONTAINER_ID_LEN - 1);
    strncpy(cfg->rootfs,  req->rootfs,       PATH_MAX - 1);
    strncpy(cfg->command, req->command,      CHILD_COMMAND_LEN - 1);
    cfg->nice_value   = req->nice_value;
    cfg->log_write_fd = pipefd[1];

    char *stack = malloc(STACK_SIZE);
    if (!stack) { free(cfg); close(pipefd[0]); close(pipefd[1]); return -1; }
    char *stack_top = stack + STACK_SIZE;

    int clone_flags = CLONE_NEWPID | CLONE_NEWUTS | CLONE_NEWNS | SIGCHLD;
    pid_t pid = clone(child_fn, stack_top, clone_flags, cfg);

    close(pipefd[1]);  /* parent closes write end */
    free(stack);

    if (pid < 0) { perror("clone"); free(cfg); close(pipefd[0]); return -1; }

    rec->host_pid = pid;
    rec->state    = CONTAINER_RUNNING;

    mkdir(LOG_DIR, 0755);

    if (ctx->monitor_fd >= 0) {
        if (register_with_monitor(ctx->monitor_fd, rec->id, pid,
                                   rec->soft_limit_bytes, rec->hard_limit_bytes) < 0)
            fprintf(stderr, "[supervisor] monitor register failed for %s: %s\n",
                    rec->id, strerror(errno));
    }

    /* Detached producer thread per container */
    producer_arg_t *parg = calloc(1, sizeof(*parg));
    if (parg) {
        parg->pipe_fd = pipefd[0];
        strncpy(parg->container_id, rec->id, CONTAINER_ID_LEN - 1);
        parg->buffer = &ctx->log_buffer;
        pthread_t pt;
        pthread_attr_t attr;
        pthread_attr_init(&attr);
        pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
        if (pthread_create(&pt, &attr, producer_thread, parg) != 0) {
            close(pipefd[0]); free(parg);
        }
        pthread_attr_destroy(&attr);
    } else {
        close(pipefd[0]);
    }

    return 0;
}

/* ---------------------------------------------------------------
 * Signal handling via self-pipes
 * --------------------------------------------------------------- */
static int sigchld_pipe[2] = {-1, -1};
static int sigterm_pipe[2] = {-1, -1};

static void handle_sigchld(int sig) { (void)sig; char b = 'C'; write(sigchld_pipe[1], &b, 1); }
static void handle_sigterm(int sig) { (void)sig; char b = 'T'; write(sigterm_pipe[1], &b, 1); }

static void reap_children(supervisor_ctx_t *ctx)
{
    int status;
    pid_t pid;
    while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
        pthread_mutex_lock(&ctx->metadata_lock);
        container_record_t *c = ctx->containers;
        while (c) {
            if (c->host_pid == pid) {
                if (WIFEXITED(status)) {
                    c->exit_code = WEXITSTATUS(status);
                    c->state     = CONTAINER_EXITED;
                } else if (WIFSIGNALED(status)) {
                    c->exit_signal = WTERMSIG(status);
                    c->exit_code   = 128 + c->exit_signal;
                    if (c->stop_requested)
                        c->state = CONTAINER_STOPPED;
                    else if (c->exit_signal == SIGKILL)
                        c->state = CONTAINER_KILLED;
                    else
                        c->state = CONTAINER_EXITED;
                }
                if (ctx->monitor_fd >= 0)
                    unregister_from_monitor(ctx->monitor_fd, c->id, pid);
                fprintf(stderr, "[supervisor] Container %s (pid %d) -> %s\n",
                        c->id, pid, state_to_string(c->state));
                break;
            }
            c = c->next;
        }
        pthread_mutex_unlock(&ctx->metadata_lock);
    }
}

/* ---------------------------------------------------------------
 * Control-plane request handler (called per accepted connection)
 * --------------------------------------------------------------- */
static void handle_control_request(supervisor_ctx_t *ctx, int client_fd)
{
    control_request_t req;
    control_response_t resp;
    memset(&resp, 0, sizeof(resp));

    ssize_t n = recv(client_fd, &req, sizeof(req), 0);
    if (n != (ssize_t)sizeof(req)) {
        resp.status = -1;
        snprintf(resp.message, sizeof(resp.message), "bad request size");
        send(client_fd, &resp, sizeof(resp), 0);
        return;
    }

    switch (req.kind) {

    case CMD_START:
    case CMD_RUN: {
        pthread_mutex_lock(&ctx->metadata_lock);
        if (find_container(ctx, req.container_id)) {
            pthread_mutex_unlock(&ctx->metadata_lock);
            resp.status = -1;
            snprintf(resp.message, sizeof(resp.message),
                     "container '%s' already exists", req.container_id);
            break;
        }
        container_record_t *rec = alloc_container_record(&req);
        if (!rec) {
            pthread_mutex_unlock(&ctx->metadata_lock);
            resp.status = -1;
            snprintf(resp.message, sizeof(resp.message), "out of memory");
            break;
        }
        rec->next       = ctx->containers;
        ctx->containers = rec;
        pthread_mutex_unlock(&ctx->metadata_lock);

        if (launch_container(ctx, rec, &req) < 0) {
            resp.status = -1;
            snprintf(resp.message, sizeof(resp.message),
                     "failed to launch '%s'", req.container_id);
        } else {
            resp.status = 0;
            if (req.kind == CMD_START)
                snprintf(resp.message, sizeof(resp.message),
                         "started '%s' pid=%d", rec->id, rec->host_pid);
            else
                snprintf(resp.message, sizeof(resp.message),
                         "run:pid=%d", rec->host_pid);
        }
        break;
    }

    case CMD_PS: {
        char buf[4096];
        int  off = 0;
        off += snprintf(buf + off, sizeof(buf) - off,
                        "%-16s %-8s %-10s %-10s %-10s %s\n",
                        "ID", "PID", "STATE", "SOFT(MiB)", "HARD(MiB)", "STARTED");
        pthread_mutex_lock(&ctx->metadata_lock);
        container_record_t *c = ctx->containers;
        while (c && off < (int)sizeof(buf) - 1) {
            char ts[32];
            struct tm *tm = localtime(&c->started_at);
            strftime(ts, sizeof(ts), "%H:%M:%S", tm);
            off += snprintf(buf + off, sizeof(buf) - off,
                            "%-16s %-8d %-10s %-10lu %-10lu %s\n",
                            c->id, c->host_pid, state_to_string(c->state),
                            c->soft_limit_bytes >> 20, c->hard_limit_bytes >> 20, ts);
            c = c->next;
        }
        pthread_mutex_unlock(&ctx->metadata_lock);
        resp.status = 0;
        strncpy(resp.message, buf, sizeof(resp.message) - 1);
        break;
    }

    case CMD_LOGS: {
        pthread_mutex_lock(&ctx->metadata_lock);
        container_record_t *c = find_container(ctx, req.container_id);
        char log_path[PATH_MAX] = {0};
        if (c) strncpy(log_path, c->log_path, sizeof(log_path) - 1);
        pthread_mutex_unlock(&ctx->metadata_lock);
        if (!c) {
            resp.status = -1;
            snprintf(resp.message, sizeof(resp.message), "no container '%s'", req.container_id);
        } else {
            resp.status = 0;
            snprintf(resp.message, sizeof(resp.message), "log:%s", log_path);
        }
        break;
    }

    case CMD_STOP: {
        pthread_mutex_lock(&ctx->metadata_lock);
        container_record_t *c = find_container(ctx, req.container_id);
        if (!c || c->state != CONTAINER_RUNNING) {
            pthread_mutex_unlock(&ctx->metadata_lock);
            resp.status = -1;
            snprintf(resp.message, sizeof(resp.message),
                     "container '%s' not running", req.container_id);
            break;
        }
        c->stop_requested = 1;
        pid_t target_pid  = c->host_pid;
        pthread_mutex_unlock(&ctx->metadata_lock);
        kill(target_pid, SIGTERM);
        resp.status = 0;
        snprintf(resp.message, sizeof(resp.message),
                 "SIGTERM -> '%s' (pid %d)", req.container_id, target_pid);
        break;
    }

    default:
        resp.status = -1;
        snprintf(resp.message, sizeof(resp.message), "unknown command %d", req.kind);
        break;
    }

    send(client_fd, &resp, sizeof(resp), 0);
}

/* ---------------------------------------------------------------
 * Supervisor main loop
 * --------------------------------------------------------------- */
static int run_supervisor(const char *rootfs)
{
    supervisor_ctx_t ctx;
    int rc;

    memset(&ctx, 0, sizeof(ctx));
    ctx.server_fd  = -1;
    ctx.monitor_fd = -1;
    g_ctx = &ctx;

    rc = pthread_mutex_init(&ctx.metadata_lock, NULL);
    if (rc != 0) { errno = rc; perror("pthread_mutex_init"); return 1; }

    rc = bounded_buffer_init(&ctx.log_buffer);
    if (rc != 0) { errno = rc; perror("bounded_buffer_init"); return 1; }

    mkdir(LOG_DIR, 0755);

    /* 1. Open kernel monitor (optional) */
    ctx.monitor_fd = open("/dev/container_monitor", O_RDWR);
    if (ctx.monitor_fd < 0)
        fprintf(stderr, "[supervisor] /dev/container_monitor not available: %s\n", strerror(errno));

    /* 2. Create control UNIX domain socket */
    unlink(CONTROL_PATH);
    ctx.server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (ctx.server_fd < 0) { perror("socket"); return 1; }

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);
    if (bind(ctx.server_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) { perror("bind"); return 1; }
    if (listen(ctx.server_fd, 16) < 0) { perror("listen"); return 1; }

    /* 3. Signal self-pipes */
    if (pipe(sigchld_pipe) < 0 || pipe(sigterm_pipe) < 0) { perror("pipe(signal)"); return 1; }
    fcntl(sigchld_pipe[1], F_SETFL, O_NONBLOCK);
    fcntl(sigterm_pipe[1], F_SETFL, O_NONBLOCK);

    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_flags   = SA_RESTART | SA_NOCLDSTOP;
    sa.sa_handler = handle_sigchld;
    sigaction(SIGCHLD, &sa, NULL);
    sa.sa_handler = handle_sigterm;
    sigaction(SIGTERM, &sa, NULL);
    sigaction(SIGINT,  &sa, NULL);

    /* 4. Start logging consumer thread */
    if (pthread_create(&ctx.logger_thread, NULL, logging_thread, &ctx) != 0) {
        perror("pthread_create logger");
        return 1;
    }

    fprintf(stderr, "[supervisor] Ready. base-rootfs=%s socket=%s\n", rootfs, CONTROL_PATH);

    /* 5. Event loop */
    while (!ctx.should_stop) {
        fd_set rfds;
        FD_ZERO(&rfds);
        FD_SET(ctx.server_fd,   &rfds);
        FD_SET(sigchld_pipe[0], &rfds);
        FD_SET(sigterm_pipe[0], &rfds);

        int nfds = ctx.server_fd;
        if (sigchld_pipe[0] > nfds) nfds = sigchld_pipe[0];
        if (sigterm_pipe[0] > nfds) nfds = sigterm_pipe[0];
        nfds++;

        int ret = select(nfds, &rfds, NULL, NULL, NULL);
        if (ret < 0) { if (errno == EINTR) continue; perror("select"); break; }

        if (FD_ISSET(sigchld_pipe[0], &rfds)) {
            char buf[16]; read(sigchld_pipe[0], buf, sizeof(buf));
            reap_children(&ctx);
        }
        if (FD_ISSET(sigterm_pipe[0], &rfds)) {
            char buf[16]; read(sigterm_pipe[0], buf, sizeof(buf));
            fprintf(stderr, "[supervisor] Shutdown signal received.\n");
            ctx.should_stop = 1;
            break;
        }
        if (FD_ISSET(ctx.server_fd, &rfds)) {
            int client_fd = accept(ctx.server_fd, NULL, NULL);
            if (client_fd >= 0) {
                handle_control_request(&ctx, client_fd);
                close(client_fd);
            }
        }
    }

    /* Orderly shutdown: SIGTERM all running containers */
    fprintf(stderr, "[supervisor] Shutting down containers...\n");
    pthread_mutex_lock(&ctx.metadata_lock);
    container_record_t *c = ctx.containers;
    while (c) {
        if (c->state == CONTAINER_RUNNING) { c->stop_requested = 1; kill(c->host_pid, SIGTERM); }
        c = c->next;
    }
    pthread_mutex_unlock(&ctx.metadata_lock);

    struct timespec ts = {.tv_sec = 2, .tv_nsec = 0};
    nanosleep(&ts, NULL);
    reap_children(&ctx);

    /* Force-kill stragglers */
    pthread_mutex_lock(&ctx.metadata_lock);
    c = ctx.containers;
    while (c) { if (c->state == CONTAINER_RUNNING) kill(c->host_pid, SIGKILL); c = c->next; }
    pthread_mutex_unlock(&ctx.metadata_lock);
    while (waitpid(-1, NULL, 0) > 0 || errno == EINTR) ;

    /* Drain and join logger */
    bounded_buffer_begin_shutdown(&ctx.log_buffer);
    pthread_join(ctx.logger_thread, NULL);
    bounded_buffer_destroy(&ctx.log_buffer);

    /* Free metadata */
    pthread_mutex_lock(&ctx.metadata_lock);
    c = ctx.containers;
    while (c) { container_record_t *next = c->next; free(c); c = next; }
    pthread_mutex_unlock(&ctx.metadata_lock);
    pthread_mutex_destroy(&ctx.metadata_lock);

    if (ctx.monitor_fd >= 0) close(ctx.monitor_fd);
    close(ctx.server_fd);
    unlink(CONTROL_PATH);
    close(sigchld_pipe[0]); close(sigchld_pipe[1]);
    close(sigterm_pipe[0]); close(sigterm_pipe[1]);

    fprintf(stderr, "[supervisor] Clean exit.\n");
    return 0;
}

/* ---------------------------------------------------------------
 * CLI client
 * --------------------------------------------------------------- */
static int send_control_request(const control_request_t *req)
{
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) { perror("socket"); return 1; }

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        fprintf(stderr, "Cannot connect to supervisor (%s): %s\n", CONTROL_PATH, strerror(errno));
        close(fd);
        return 1;
    }
    if (send(fd, req, sizeof(*req), 0) != (ssize_t)sizeof(*req)) { perror("send"); close(fd); return 1; }

    control_response_t resp;
    if (recv(fd, &resp, sizeof(resp), 0) != (ssize_t)sizeof(resp)) { perror("recv"); close(fd); return 1; }
    close(fd);

    /* CMD_RUN: poll until container finishes */
    if (req->kind == CMD_RUN && resp.status == 0) {
        int cpid = -1;
        if (sscanf(resp.message, "run:pid=%d", &cpid) == 1) {
            printf("[run] Container '%s' started (pid %d), waiting...\n", req->container_id, cpid);
            struct sockaddr_un a2;
            memset(&a2, 0, sizeof(a2));
            a2.sun_family = AF_UNIX;
            strncpy(a2.sun_path, CONTROL_PATH, sizeof(a2.sun_path) - 1);
            while (1) {
                struct timespec s = {.tv_sec = 1}; nanosleep(&s, NULL);
                control_request_t ps_req; memset(&ps_req, 0, sizeof(ps_req)); ps_req.kind = CMD_PS;
                int pfd = socket(AF_UNIX, SOCK_STREAM, 0);
                if (pfd < 0) break;
                if (connect(pfd, (struct sockaddr *)&a2, sizeof(a2)) < 0) { close(pfd); break; }
                send(pfd, &ps_req, sizeof(ps_req), 0);
                control_response_t ps_resp; recv(pfd, &ps_resp, sizeof(ps_resp), 0); close(pfd);
                char *line = strstr(ps_resp.message, req->container_id);
                if (!line || !strstr(line, "running")) {
                    printf("[run] Container '%s' finished.\n", req->container_id);
                    break;
                }
            }
            return 0;
        }
    }

    /* CMD_LOGS: retrieve and print the log file */
    if (req->kind == CMD_LOGS && resp.status == 0 && strncmp(resp.message, "log:", 4) == 0) {
        const char *log_path = resp.message + 4;
        FILE *fp = fopen(log_path, "r");
        if (!fp) { fprintf(stderr, "Log not found: %s\n", log_path); return 1; }
        char line[512];
        while (fgets(line, sizeof(line), fp)) fputs(line, stdout);
        fclose(fp);
        return 0;
    }

    printf("%s\n", resp.message);
    return resp.status == 0 ? 0 : 1;
}

static int cmd_start(int argc, char *argv[])
{
    control_request_t req;
    if (argc < 5) {
        fprintf(stderr, "Usage: %s start <id> <rootfs> <command> [opts]\n", argv[0]);
        return 1;
    }
    memset(&req, 0, sizeof(req));
    req.kind = CMD_START;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs,       argv[3], sizeof(req.rootfs)       - 1);
    strncpy(req.command,      argv[4], sizeof(req.command)      - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;
    if (parse_optional_flags(&req, argc, argv, 5) != 0) return 1;
    return send_control_request(&req);
}

static int cmd_run(int argc, char *argv[])
{
    control_request_t req;
    if (argc < 5) {
        fprintf(stderr, "Usage: %s run <id> <rootfs> <command> [opts]\n", argv[0]);
        return 1;
    }
    memset(&req, 0, sizeof(req));
    req.kind = CMD_RUN;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs,       argv[3], sizeof(req.rootfs)       - 1);
    strncpy(req.command,      argv[4], sizeof(req.command)      - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;
    if (parse_optional_flags(&req, argc, argv, 5) != 0) return 1;
    return send_control_request(&req);
}

static int cmd_ps(void)
{
    control_request_t req;
    memset(&req, 0, sizeof(req));
    req.kind = CMD_PS;
    return send_control_request(&req);
}

static int cmd_logs(int argc, char *argv[])
{
    control_request_t req;
    if (argc < 3) { fprintf(stderr, "Usage: %s logs <id>\n", argv[0]); return 1; }
    memset(&req, 0, sizeof(req));
    req.kind = CMD_LOGS;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    return send_control_request(&req);
}

static int cmd_stop(int argc, char *argv[])
{
    control_request_t req;
    if (argc < 3) { fprintf(stderr, "Usage: %s stop <id>\n", argv[0]); return 1; }
    memset(&req, 0, sizeof(req));
    req.kind = CMD_STOP;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    return send_control_request(&req);
}

int main(int argc, char *argv[])
{
    if (argc < 2) { usage(argv[0]); return 1; }
    if (strcmp(argv[1], "supervisor") == 0) {
        if (argc < 3) { fprintf(stderr, "Usage: %s supervisor <base-rootfs>\n", argv[0]); return 1; }
        return run_supervisor(argv[2]);
    }
    if (strcmp(argv[1], "start") == 0) return cmd_start(argc, argv);
    if (strcmp(argv[1], "run")   == 0) return cmd_run(argc, argv);
    if (strcmp(argv[1], "ps")    == 0) return cmd_ps();
    if (strcmp(argv[1], "logs")  == 0) return cmd_logs(argc, argv);
    if (strcmp(argv[1], "stop")  == 0) return cmd_stop(argc, argv);
    usage(argv[0]);
    return 1;
}
