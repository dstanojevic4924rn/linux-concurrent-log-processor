#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <dirent.h>
#include <sys/stat.h>
#include <unistd.h>
#include <time.h>
#include <sys/select.h>
#include <fcntl.h>
#include <errno.h>

#define MAX_PATH        1024
#define MAX_MSG         4096
#define WORK_QUEUE_CAP  100
#define NUM_WORKERS     6

typedef struct {
    char   path[MAX_PATH];
    time_t mtime;
    int    last_seq;
    int    in_queue;
    int    in_progress;
    int    processed;      // 1 = fajl je bar jednom obradjen
} scanned_file_t;

typedef struct {
    scanned_file_t *items;
    int            count;
    int            capacity;
    pthread_mutex_t mutex;
} scanned_db_t;

typedef struct {
    char path[MAX_PATH];
} work_item_t;

typedef struct {
    work_item_t items[WORK_QUEUE_CAP];
    int         head, tail, count;
    pthread_mutex_t mutex;
    pthread_cond_t  not_empty;
    pthread_cond_t  not_full;
} work_queue_t;

typedef struct msg_node {
    char msg[MAX_MSG];
    int  count;
    struct msg_node *next;
} msg_node_t;

typedef struct {
    msg_node_t *head;
    pthread_mutex_t mutex;
} msg_list_t;

typedef struct {
    int total_files;
    int total_lines;
    int error_count;
    int warning_count;
    int info_count;
    pthread_mutex_t mutex;
} stats_t;

volatile int running = 1;
volatile int watch_running = 0;
pthread_t watch_tid;

scanned_db_t scanned_db;
work_queue_t work_queue;
msg_list_t   msg_list;
stats_t      stats;
pthread_mutex_t print_mutex;


void init_scanned_db(scanned_db_t *db) {
    db->capacity = 16;
    db->count    = 0;
    db->items    = calloc(db->capacity, sizeof(scanned_file_t));
    pthread_mutex_init(&db->mutex, NULL);
}

scanned_file_t* find_scanned(scanned_db_t *db, const char *path) {
    for (int i = 0; i < db->count; i++) {
        if (strcmp(db->items[i].path, path) == 0)
            return &db->items[i];
    }
    return NULL;
}

void init_work_queue(work_queue_t *q) {
    q->head = q->tail = q->count = 0;
    pthread_mutex_init(&q->mutex, NULL);
    pthread_cond_init(&q->not_empty, NULL);
    pthread_cond_init(&q->not_full, NULL);
}

void init_msg_list(msg_list_t *list) {
    list->head = NULL;
    pthread_mutex_init(&list->mutex, NULL);
}

void init_stats(stats_t *s) {
    memset(s, 0, sizeof(stats_t));
    pthread_mutex_init(&s->mutex, NULL);
}

void enqueue(work_queue_t *q, const char *path) {
    pthread_mutex_lock(&q->mutex);
    while (q->count == WORK_QUEUE_CAP && running)
        pthread_cond_wait(&q->not_full, &q->mutex);

    if (!running) {
        pthread_mutex_unlock(&q->mutex);
        return;
    }
    strncpy(q->items[q->tail].path, path, MAX_PATH - 1);
    q->items[q->tail].path[MAX_PATH - 1] = '\0';
    q->tail = (q->tail + 1) % WORK_QUEUE_CAP;
    q->count++;
    pthread_cond_signal(&q->not_empty);
    pthread_mutex_unlock(&q->mutex);
}

int dequeue(work_queue_t *q, char *path) {
    pthread_mutex_lock(&q->mutex);
    while (q->count == 0 && running)
        pthread_cond_wait(&q->not_empty, &q->mutex);

    if (q->count == 0) {
        pthread_mutex_unlock(&q->mutex);
        return 0;
    }
    strncpy(path, q->items[q->head].path, MAX_PATH - 1);
    path[MAX_PATH - 1] = '\0';
    q->head = (q->head + 1) % WORK_QUEUE_CAP;
    q->count--;
    pthread_cond_signal(&q->not_full);
    pthread_mutex_unlock(&q->mutex);
    return 1;
}

void add_message(msg_list_t *list, const char *msg) {
    pthread_mutex_lock(&list->mutex);
    msg_node_t *cur = list->head;
    while (cur != NULL) {
        if (strcmp(cur->msg, msg) == 0) {
            cur->count++;
            pthread_mutex_unlock(&list->mutex);
            return;
        }
        cur = cur->next;
    }
    msg_node_t *node = malloc(sizeof(msg_node_t));
    strncpy(node->msg, msg, MAX_MSG - 1);
    node->msg[MAX_MSG - 1] = '\0';
    node->count = 1;
    node->next  = list->head;
    list->head  = node;
    pthread_mutex_unlock(&list->mutex);
}

void print_stats() {
    pthread_mutex_lock(&stats.mutex);
    int tf  = stats.total_files;
    int tl  = stats.total_lines;
    int er  = stats.error_count;
    int wa  = stats.warning_count;
    int inf = stats.info_count;
    pthread_mutex_unlock(&stats.mutex);

    pthread_mutex_lock(&print_mutex);
    printf("======================================\n");
    printf("| Ukupno procitanih log fajlova: %d\n", tf);
    printf("| Ukupno procitanih linija: %d\n", tl);
    printf("| ERROR: %d\n", er);
    printf("| WARNING: %d\n", wa);
    printf("| INFO: %d\n", inf);
    printf("======================================\n");
    fflush(stdout);
    pthread_mutex_unlock(&print_mutex);
}

typedef struct {
    char msg[MAX_MSG];
    int  count;
} top_item_t;

static int cmp_top_desc(const void *a, const void *b) {
    const top_item_t *ia = a, *ib = b;
    return ib->count - ia->count;
}

void print_top(int N) {
    pthread_mutex_lock(&msg_list.mutex);
    int n = 0;
    for (msg_node_t *c = msg_list.head; c; c = c->next) n++;

    if (n == 0) {
        pthread_mutex_unlock(&msg_list.mutex);
        pthread_mutex_lock(&print_mutex);
        printf("===================================\n");
        printf("Nema podataka.\n");
        printf("===================================\n");
        fflush(stdout);
        pthread_mutex_unlock(&print_mutex);
        return;
    }

    top_item_t *arr = malloc(n * sizeof(top_item_t));
    msg_node_t *c = msg_list.head;
    for (int i = 0; i < n; i++) {
        strncpy(arr[i].msg, c->msg, MAX_MSG - 1);
        arr[i].msg[MAX_MSG - 1] = '\0';
        arr[i].count = c->count;
        c = c->next;
    }
    pthread_mutex_unlock(&msg_list.mutex);

    qsort(arr, n, sizeof(top_item_t), cmp_top_desc);

    pthread_mutex_lock(&print_mutex);
    printf("===================================\n");
    int limit = (N < n) ? N : n;
    for (int i = 0; i < limit; i++)
        printf("|%d. %s (%d)\n", i + 1, arr[i].msg, arr[i].count);
    printf("===================================\n");
    fflush(stdout);
    pthread_mutex_unlock(&print_mutex);
    free(arr);
}

void* worker_thread(void *arg) {
    (void)arg;
    char path[MAX_PATH];

    while (running) {
        if (!dequeue(&work_queue, path)) {
            if (!running) break;
            continue;
        }

        int last_seq = 0;
        pthread_mutex_lock(&scanned_db.mutex);
        scanned_file_t *f = find_scanned(&scanned_db, path);
        if (f) {
            f->in_queue   = 0;
            f->in_progress = 1;
            last_seq      = f->last_seq;
        }
        pthread_mutex_unlock(&scanned_db.mutex);

        FILE *fp = fopen(path, "r");
        if (!fp) {
            pthread_mutex_lock(&scanned_db.mutex);
            f = find_scanned(&scanned_db, path);
            if (f) f->in_progress = 0;
            pthread_mutex_unlock(&scanned_db.mutex);
            continue;
        }

        char line[MAX_MSG + 256];
        int max_seq = last_seq;
        int local_lines = 0, local_err = 0, local_warn = 0, local_info = 0;

        while (fgets(line, sizeof(line), fp)) {
            int seq;
            char level[16];
            char msg[MAX_MSG];

            if (sscanf(line, "%d. %*s severity=\"%15[^\"]\" %[^\n]",
                &seq, level, msg) == 3) {
                if (seq > last_seq) {
                    local_lines++;
                    if (strcmp(level, "ERROR")   == 0) local_err++;
                    else if (strcmp(level, "WARNING") == 0) local_warn++;
                    else if (strcmp(level, "INFO")    == 0) local_info++;
                    add_message(&msg_list, msg);
                    if (seq > max_seq) max_seq = seq;
                }
                }
        }
        fclose(fp);

        int is_new_file = 0;
        pthread_mutex_lock(&scanned_db.mutex);
        f = find_scanned(&scanned_db, path);
        if (f) {
            if (!f->processed) is_new_file = 1;
            f->last_seq    = max_seq;
            f->in_progress = 0;
            f->processed   = 1;
        }
        pthread_mutex_unlock(&scanned_db.mutex);

        pthread_mutex_lock(&stats.mutex);
        if (is_new_file) stats.total_files++;
        stats.total_lines   += local_lines;
        stats.error_count   += local_err;
        stats.warning_count += local_warn;
        stats.info_count    += local_info;
        pthread_mutex_unlock(&stats.mutex);
    }
    return NULL;
}

typedef struct {
    char dir[MAX_PATH];
    pthread_t tid;
} scanner_data_t;

void* scanner_thread(void *arg) {
    scanner_data_t *data = arg;

    while (running) {
        DIR *d = opendir(data->dir);
        if (!d) {
            for (int i = 0; i < 5 && running; i++) sleep(1);
            continue;
        }

        struct dirent *entry;
        while ((entry = readdir(d)) != NULL && running) {
            if (strcmp(entry->d_name, ".") == 0 ||
                strcmp(entry->d_name, "..") == 0)
                continue;

            char path[MAX_PATH];
            snprintf(path, MAX_PATH, "%s/%s", data->dir, entry->d_name);

            struct stat st;
            if (stat(path, &st) != 0) continue;
            if (!S_ISREG(st.st_mode)) continue;

            pthread_mutex_lock(&scanned_db.mutex);
            scanned_file_t *f = find_scanned(&scanned_db, path);
            if (f == NULL) {
                if (scanned_db.count >= scanned_db.capacity) {
                    scanned_db.capacity *= 2;
                    scanned_db.items = realloc(scanned_db.items,
                                               scanned_db.capacity * sizeof(scanned_file_t));
                }
                f = &scanned_db.items[scanned_db.count++];
                memset(f, 0, sizeof(scanned_file_t));
                strncpy(f->path, path, MAX_PATH - 1);
                f->path[MAX_PATH - 1] = '\0';
                f->mtime = st.st_mtime;
                f->in_queue = 1;
                pthread_mutex_unlock(&scanned_db.mutex);
                enqueue(&work_queue, path);
            } else if (f->mtime != st.st_mtime) {
                f->mtime = st.st_mtime;
                if (!f->in_queue && !f->in_progress) {
                    f->in_queue = 1;
                    pthread_mutex_unlock(&scanned_db.mutex);
                    enqueue(&work_queue, path);
                } else {
                    pthread_mutex_unlock(&scanned_db.mutex);
                }
            } else {
                pthread_mutex_unlock(&scanned_db.mutex);
            }
        }
        closedir(d);

        for (int i = 0; i < 5 && running; i++) sleep(1);
    }
    return NULL;
}

void* watch_thread_func(void *arg) {
    (void)arg;
    while (watch_running) {
        system("clear");
        print_stats();
        sleep(1);
    }
    return NULL;
}

int main() {
    init_scanned_db(&scanned_db);
    init_work_queue(&work_queue);
    init_msg_list(&msg_list);
    init_stats(&stats);
    pthread_mutex_init(&print_mutex, NULL);

    pthread_t workers[NUM_WORKERS];
    for (int i = 0; i < NUM_WORKERS; i++)
        pthread_create(&workers[i], NULL, worker_thread, NULL);

    scanner_data_t *scanners = NULL;
    int scanner_count = 0, scanner_cap = 0;

    char cmd[1024];

    while (running) {
        printf("> ");
        fflush(stdout);

        if (fgets(cmd, sizeof(cmd), stdin) == NULL) {
            break;
        }

        cmd[strcspn(cmd, "\n")] = '\0';

        if (strncmp(cmd, "add ", 4) == 0) {
            char *dir = cmd + 4;
            if (scanner_count >= scanner_cap) {
                scanner_cap = scanner_cap ? scanner_cap * 2 : 4;
                scanners = realloc(scanners, scanner_cap * sizeof(scanner_data_t));
            }
            strncpy(scanners[scanner_count].dir, dir, MAX_PATH - 1);
            scanners[scanner_count].dir[MAX_PATH - 1] = '\0';
            pthread_create(&scanners[scanner_count].tid, NULL,
                           scanner_thread, &scanners[scanner_count]);
            scanner_count++;
        }
        else if (strcmp(cmd, "stats") == 0) {
            print_stats();
        }
        else if (strncmp(cmd, "top ", 4) == 0) {
            int N = atoi(cmd + 4);
            print_top(N);
        }
        else if (strcmp(cmd, "watch") == 0) {
            watch_running = 1;
            pthread_create(&watch_tid, NULL, watch_thread_func, NULL);

            while (watch_running) {
                fd_set fds;
                FD_ZERO(&fds);
                FD_SET(STDIN_FILENO, &fds);
                struct timeval tv = {1, 0};

                int ret = select(STDIN_FILENO + 1, &fds, NULL, NULL, &tv);
                if (ret < 0 && errno == EINTR) continue;

                if (ret > 0 && FD_ISSET(STDIN_FILENO, &fds)) {
                    int c;
                    while ((c = getchar()) != EOF && c != '\n');
                    if (c == EOF) {
                        watch_running = 0;
                        pthread_join(watch_tid, NULL);
                        printf("\n");
                        clearerr(stdin);
                        break;
                    }
                }
            }
        }
        else if (strcmp(cmd, "stop") == 0) {
            running = 0;
            watch_running = 0;
            pthread_cond_broadcast(&work_queue.not_empty);
            pthread_cond_broadcast(&work_queue.not_full);
            printf("Application stopped.\n");
            break;
        }
        else if (strlen(cmd) > 0) {
            printf("Unknown command: %s\n", cmd);
        }
    }

    running = 0;
    watch_running = 0;
    pthread_cond_broadcast(&work_queue.not_empty);
    pthread_cond_broadcast(&work_queue.not_full);

    for (int i = 0; i < NUM_WORKERS; i++)
        pthread_join(workers[i], NULL);
    for (int i = 0; i < scanner_count; i++)
        pthread_join(scanners[i].tid, NULL);

    free(scanned_db.items);
    msg_node_t *cur = msg_list.head;
    while (cur) {
        msg_node_t *tmp = cur;
        cur = cur->next;
        free(tmp);
    }
    free(scanners);

    return 0;
}
