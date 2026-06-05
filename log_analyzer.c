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

#define MAX_PATH        1024 //duzinja putanje
#define MAX_MSG         4096//duzina poruke max
#define WORK_QUEUE_CAP  100// kapacitet reda cekanja
#define NUM_WORKERS     6// broj niti koje obrajduju log fajlove

typedef struct {
    char   path[MAX_PATH]; //putanja fajla
    time_t mtime; //vreme posledeje modifikacije
    int    last_seq; //poslednji redni broj log linije koja je procitana
    int    in_queue; //da li je u redu za obradu
    int    in_progress; //da li se obradjuje
    int    processed;      // 1 = fajl je bar jednom obradjen
} scanned_file_t;

typedef struct {
    scanned_file_t *items; //dinamicki niz od ovog gore
    int            count; //trenutni broj fajlova
    int            capacity; //trenutni kapacitet niza
    pthread_mutex_t mutex; //pristup pazi mutex
} scanned_db_t;

typedef struct {
    char path[MAX_PATH]; //element reda za obradu
} work_item_t;

typedef struct {
    work_item_t items[WORK_QUEUE_CAP]; //niz stavki
    int         head, tail, count; //indeksi i broj zauzetih mesta
    pthread_mutex_t mutex; //
    pthread_cond_t  not_empty; //promenljive za sinhronizaciju proizvodjaca i potrosaca
    pthread_cond_t  not_full; //ovo gore
} work_queue_t;

typedef struct msg_node {
    char msg[MAX_MSG];
    int  count;
    struct msg_node *next;
} msg_node_t; // cuva tekst poruke i broj ponavljanja poruke

typedef struct {
    msg_node_t *head;
    pthread_mutex_t mutex;
} msg_list_t; //lista poruka sa zastitom

typedef struct {
    int total_files;
    int total_lines;
    int error_count;
    int warning_count;
    int info_count;
    pthread_mutex_t mutex;
} stats_t; //statistika globalna

volatile int running = 1; //signal da li program treba da radi, ako je 0 niti se gase
volatile int watch_running = 0; //da li je watch aktivan
pthread_t watch_tid; //ID niti za watch mod

scanned_db_t scanned_db;
work_queue_t work_queue;
msg_list_t   msg_list;
stats_t      stats;
pthread_mutex_t print_mutex; //globalne strukture i mutex protiv mesanja niti


void init_scanned_db(scanned_db_t *db) {
    db->capacity = 16; // pocetni kapacitet je 16
    db->count    = 0; //
    db->items    = calloc(db->capacity, sizeof(scanned_file_t)); // alocira memoriju za 16 stavki
    pthread_mutex_init(&db->mutex, NULL);
}

scanned_file_t* find_scanned(scanned_db_t *db, const char *path) {
    for (int i = 0; i < db->count; i++) {
        if (strcmp(db->items[i].path, path) == 0)
            return &db->items[i];
    }
    return NULL;
}// linearno ide kroz bazu po putanji i vraca pogazivac na odredjeni slog ili NULL

void init_work_queue(work_queue_t *q) { // inicijalizuje red i njegove mutexe/promenljive
    q->head = q->tail = q->count = 0;
    pthread_mutex_init(&q->mutex, NULL);
    pthread_cond_init(&q->not_empty, NULL);
    pthread_cond_init(&q->not_full, NULL);
}

void init_msg_list(msg_list_t *list) {// prazna lista poruka
    list->head = NULL;
    pthread_mutex_init(&list->mutex, NULL);
}

void init_stats(stats_t *s) { // brojaci postavljeni na 0
    memset(s, 0, sizeof(stats_t));
    pthread_mutex_init(&s->mutex, NULL);
}

void enqueue(work_queue_t *q, const char *path) {
    pthread_mutex_lock(&q->mutex); // zakljucava red
    while (q->count == WORK_QUEUE_CAP && running)
        pthread_cond_wait(&q->not_full, &q->mutex); // ako je red pun ceka da se oslobodi ili da running postane 0

        if (!running) {
            pthread_mutex_unlock(&q->mutex); // ako je running 0 izlazi bez dodavanja
            return;
        }
        strncpy(q->items[q->tail].path, path, MAX_PATH - 1); // kopira prvu putanju i postavi je na slobodan slot
        q->items[q->tail].path[MAX_PATH - 1] = '\0';
    q->tail = (q->tail + 1) % WORK_QUEUE_CAP; // pomera tail kruzno
    q->count++; // i povecava brojac
    pthread_cond_signal(&q->not_empty); // signalizira da red nije prazan
    pthread_mutex_unlock(&q->mutex); // orkljucava mutex
}

int dequeue(work_queue_t *q, char *path) {
    pthread_mutex_lock(&q->mutex); // ceka dok red ne postane neprazan ili program ne zaustavi
    while (q->count == 0 && running)
        pthread_cond_wait(&q->not_empty, &q->mutex);

    if (q->count == 0) {
        pthread_mutex_unlock(&q->mutex); // ako je red prazan i running 0 vraca 0
        return 0;
    }
    strncpy(path, q->items[q->head].path, MAX_PATH - 1); // kopira putanju sa head i pomera head kruzno
    path[MAX_PATH - 1] = '\0';
    q->head = (q->head + 1) % WORK_QUEUE_CAP;
    q->count--;
    pthread_cond_signal(&q->not_full); // signalizira nor full, moze novi posao
    pthread_mutex_unlock(&q->mutex);
    return 1; // uspesno
}

void add_message(msg_list_t *list, const char *msg) {
    pthread_mutex_lock(&list->mutex);
    msg_node_t *cur = list->head;
    while (cur != NULL) {
        if (strcmp(cur->msg, msg) == 0) { // ako je poruka u listi samo povecava brojac
            cur->count++;
            pthread_mutex_unlock(&list->mutex);
            return;
        }
        cur = cur->next;
    }
    msg_node_t *node = malloc(sizeof(msg_node_t)); // alocira novi cvor
    strncpy(node->msg, msg, MAX_MSG - 1); // kopira tekst
    node->msg[MAX_MSG - 1] = '\0';
    node->count = 1; // postavlja count na 1
    node->next  = list->head; // i stavlja ga na pocetak liste
    list->head  = node;
    pthread_mutex_unlock(&list->mutex);
}

void print_stats() {
    pthread_mutex_lock(&stats.mutex); // sigurno citanje statistiku
    int tf  = stats.total_files;
    int tl  = stats.total_lines;
    int er  = stats.error_count;
    int wa  = stats.warning_count;
    int inf = stats.info_count;
    pthread_mutex_unlock(&stats.mutex);

    pthread_mutex_lock(&print_mutex); // ispisuje mutexom da se ne bi mesalo sa drugim ispisom, npr tastature
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
} top_item_t; // struktura za sortiranje

static int cmp_top_desc(const void *a, const void *b) {
    const top_item_t *ia = a, *ib = b;
    return ib->count - ia->count;
}// uporedjuje opadajuce sortiranje po broju ponavljanja

void print_top(int N) {
    pthread_mutex_lock(&msg_list.mutex);
    int n = 0;
    for (msg_node_t *c = msg_list.head; c; c = c->next) n++; // broji cvorove u listi

    if (n == 0) { // ako nema ispisuje..
        pthread_mutex_unlock(&msg_list.mutex);
        pthread_mutex_lock(&print_mutex);
        printf("===================================\n");
        printf("Nema podataka.\n");
        printf("===================================\n");
        fflush(stdout);
        pthread_mutex_unlock(&print_mutex);
        return;
    }

    top_item_t *arr = malloc(n * sizeof(top_item_t)); // kopira poruke i brojace u niz
    msg_node_t *c = msg_list.head;
    for (int i = 0; i < n; i++) {
        strncpy(arr[i].msg, c->msg, MAX_MSG - 1);
        arr[i].msg[MAX_MSG - 1] = '\0';
        arr[i].count = c->count;
        c = c->next;
    }
    pthread_mutex_unlock(&msg_list.mutex);

    qsort(arr, n, sizeof(top_item_t), cmp_top_desc); // sortira niz opadajuce

    pthread_mutex_lock(&print_mutex);
    printf("===================================\n");
    int limit = (N < n) ? N : n; // ispisuje prvih N poruka, ili manje
    for (int i = 0; i < limit; i++)
        printf("|%d. %s (%d)\n", i + 1, arr[i].msg, arr[i].count);
    printf("===================================\n");
    fflush(stdout);
    pthread_mutex_unlock(&print_mutex);
    free(arr); // oslobadja privremeni niz
}

void* worker_thread(void *arg) {
    (void)arg;
    char path[MAX_PATH];

    while (running) { // uzima posao iz reda
        if (!dequeue(&work_queue, path)) { // ako je dequeue vratio 0 (kraj reda), i running== 0 izlazi iz petlje, ako ne nastavlja
            if (!running) break;
            continue;
        }

        int last_seq = 0;
        pthread_mutex_lock(&scanned_db.mutex);
        scanned_file_t *f = find_scanned(&scanned_db, path); // pronalazi slog fajla u bazi
        if (f) {
            f->in_queue   = 0;
            f->in_progress = 1; // postavlja ga u stanje za obradu
            last_seq      = f->last_seq; // pamti poslednji procitani redni broj
        }
        pthread_mutex_unlock(&scanned_db.mutex);

        FILE *fp = fopen(path, "r"); // ako ne moze da ga otvori (npr obrisan) ili vraca in progress = 0 i nastavlja
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

        while (fgets(line, sizeof(line), fp)) { // cita fajl liniju po liniju
            int seq;
            char level[16];
            char msg[MAX_MSG];

            if (sscanf(line, "%d. %*s severity=\"%15[^\"]\" %[^\n]", // parsira ga
                &seq, level, msg) == 3) {
                if (seq > last_seq) { // ako je redni broj veci od last_seq, broji liniju i azurira statistiku
                    local_lines++;
                    if (strcmp(level, "ERROR")   == 0) local_err++;
                    else if (strcmp(level, "WARNING") == 0) local_warn++;
                    else if (strcmp(level, "INFO")    == 0) local_info++;
                    add_message(&msg_list, msg);
                    if (seq > max_seq) max_seq = seq; // pamti najveci seq
                }
                }
        }
        fclose(fp);

        int is_new_file = 0;
        pthread_mutex_lock(&scanned_db.mutex); // pod zastitom
        f = find_scanned(&scanned_db, path);
        if (f) {
            if (!f->processed) is_new_file = 1;
            f->last_seq    = max_seq; // azurira last_seq
            f->in_progress = 0;
            f->processed   = 1;
        }
        pthread_mutex_unlock(&scanned_db.mutex);

        pthread_mutex_lock(&stats.mutex); // uvecava globalne brojace, ako je nov dajl dodaje na total_files
        if (is_new_file) stats.total_files++;
        stats.total_lines   += local_lines;
        stats.error_count   += local_err;
        stats.warning_count += local_warn;
        stats.info_count    += local_info;
        pthread_mutex_unlock(&stats.mutex);
    }
    return NULL;
}

typedef struct { //podaci koje dobija nit skenera
    char dir[MAX_PATH]; // putanja direktorijuma
    pthread_t tid; // njen ID
} scanner_data_t;

void* scanner_thread(void *arg) {
    scanner_data_t *data = arg;

    while (running) {
        DIR *d = opendir(data->dir); // otvara dir ako moze
        if (!d) {
            for (int i = 0; i < 5 && running; i++) sleep(1); // ako ne spava 5 sek i pokusa ponovo
            continue;
        }

        struct dirent *entry;
        while ((entry = readdir(d)) != NULL && running) { // cita sve stavke u dir
            if (strcmp(entry->d_name, ".") == 0 ||
                strcmp(entry->d_name, "..") == 0) // osim . i ..
            continue;

            char path[MAX_PATH]; // formira putanju
            snprintf(path, MAX_PATH, "%s/%s", data->dir, entry->d_name); // uzima podatke o fajlu

            struct stat st;
            if (stat(path, &st) != 0) continue;
            if (!S_ISREG(st.st_mode)) continue; // preskace sve sto nije regularan fajl

            pthread_mutex_lock(&scanned_db.mutex);
            scanned_file_t *f = find_scanned(&scanned_db, path);
            if (f == NULL) { // ako fajl nije u bazi dodaje ga
                if (scanned_db.count >= scanned_db.capacity) {
                    scanned_db.capacity *= 2; // prosiruje niz ako je pun
                    scanned_db.items = realloc(scanned_db.items,
                                               scanned_db.capacity * sizeof(scanned_file_t)); // inicijalizuje novi slog?
                }
                f = &scanned_db.items[scanned_db.count++];
                memset(f, 0, sizeof(scanned_file_t));
                strncpy(f->path, path, MAX_PATH - 1);
                f->path[MAX_PATH - 1] = '\0';
                f->mtime = st.st_mtime;
                f->in_queue = 1; // postavlja da je u queue red
                pthread_mutex_unlock(&scanned_db.mutex);
                enqueue(&work_queue, path);
            } else if (f->mtime != st.st_mtime) { // ako postoji izmenjen je
                f->mtime = st.st_mtime; // novi mtime
                if (!f->in_queue && !f->in_progress) {
                    f->in_queue = 1; // ako nije u redu dodaje ga
                    pthread_mutex_unlock(&scanned_db.mutex); // ako jeste otkljucava bazu
                    enqueue(&work_queue, path);
                } else {
                    pthread_mutex_unlock(&scanned_db.mutex);
                }
            } else {
                pthread_mutex_unlock(&scanned_db.mutex);
            }
        }
        closedir(d); // zatvara direktorijum

        for (int i = 0; i < 5 && running; i++) sleep(1); // spava 5 sek
    }
    return NULL;
}

void* watch_thread_func(void *arg) {
    (void)arg;
    while (watch_running) {
        system("clear");
        print_stats(); // svake sekunde brise ekran i postavlja statistiku
        sleep(1);
    }
    return NULL;
}

int main() {
    init_scanned_db(&scanned_db);
    init_work_queue(&work_queue);
    init_msg_list(&msg_list);
    init_stats(&stats);
    pthread_mutex_init(&print_mutex, NULL); // inicijalizuje globalne strukture

    pthread_t workers[NUM_WORKERS];
    for (int i = 0; i < NUM_WORKERS; i++)
        pthread_create(&workers[i], NULL, worker_thread, NULL); // pokrece 6 radnih niti

        scanner_data_t *scanners = NULL; // dinamicki niz za skenere
        int scanner_count = 0, scanner_cap = 0; // prazan pocetno

        char cmd[1024]; // glavna komandna petlja

        while (running) {
            printf("> "); // ispisuje prompt
            fflush(stdout);

            if (fgets(cmd, sizeof(cmd), stdin) == NULL) { // cita komandu
                break;
            }

            cmd[strcspn(cmd, "\n")] = '\0'; // uklanja novi red

            if (strncmp(cmd, "add ", 4) == 0) { // ako je komanda add
                char *dir = cmd + 4; // dodaje novi direktorijum
                if (scanner_count >= scanner_cap) { // prosiruje niz skenera ako treba
                    scanner_cap = scanner_cap ? scanner_cap * 2 : 4;
                    scanners = realloc(scanners, scanner_cap * sizeof(scanner_data_t));
                }
                strncpy(scanners[scanner_count].dir, dir, MAX_PATH - 1);
                scanners[scanner_count].dir[MAX_PATH - 1] = '\0';
                pthread_create(&scanners[scanner_count].tid, NULL, // pokrece novu nit skenera
                               scanner_thread, &scanners[scanner_count]);
                scanner_count++;
            }
            else if (strcmp(cmd, "stats") == 0) { // ako je stats
                print_stats(); // ispisuje statistiku
            }
            else if (strncmp(cmd, "top", 4) == 0) { // ako je top
                int N = atoi(cmd + 4); // prikazuje N najcescih poruka
                print_top(N);
            }
            else if (strcmp(cmd, "watch") == 0) { // ako je watch
                watch_running = 1; // pokrece watch
                pthread_create(&watch_tid, NULL, watch_thread_func, NULL); // watch pokrece

                while (watch_running) {
                    fd_set fds;
                    FD_ZERO(&fds);
                    FD_SET(STDIN_FILENO, &fds);
                    struct timeval tv = {1, 0};

                    int ret = select(STDIN_FILENO + 1, &fds, NULL, NULL, &tv); // ceka pritisak tastera
                    if (ret < 0 && errno == EINTR) continue; // prekida rezim

                    if (ret > 0 && FD_ISSET(STDIN_FILENO, &fds)) {
                        int c;
                        while ((c = getchar()) != EOF && c != '\n');
                        if (c == EOF) { // cisti eof
                            watch_running = 0;
                            pthread_join(watch_tid, NULL);
                            printf("\n");
                            clearerr(stdin);
                            break;
                        }
                    }
                }
            }
            else if (strcmp(cmd, "stop") == 0) { // gasi program
                running = 0;
                watch_running = 0;
                pthread_cond_broadcast(&work_queue.not_empty);
                pthread_cond_broadcast(&work_queue.not_full); // signalizira svim cekajucim nitima
                printf("Application stopped.\n");
                break; // izlazi iz glavne petlje
            }
            else if (strlen(cmd) > 0) { // nepoznate komande
                printf("Unknown command: %s\n", cmd);
            }
        }

        running = 0;
        watch_running = 0;
        pthread_cond_broadcast(&work_queue.not_empty);
        pthread_cond_broadcast(&work_queue.not_full); // osigurava gasenje

        for (int i = 0; i < NUM_WORKERS; i++)
            pthread_join(workers[i], NULL);
    for (int i = 0; i < scanner_count; i++)
        pthread_join(scanners[i].tid, NULL); // ceka da se sve radne i skener niti zavrse

        free(scanned_db.items); // oslobadja svu dinamicku memoriju, baza skeniranih fajlova, lista poruka, niz skenera...
        msg_node_t *cur = msg_list.head;
    while (cur) {
        msg_node_t *tmp = cur;
        cur = cur->next;
        free(tmp);
    }
    free(scanners);

    return 0;
}
