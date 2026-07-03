#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <time.h>
#include <unistd.h>
#include <libwebsockets.h>
#include <cJSON.h>

#define BUFFER_SIZE 256  // Μέγεθος κυκλικού buffer
#define MAX_MSG_LEN 4096  // Μέγιστο μέγεθος ενός JSON μηνύματος

// Δομή Κυκλικού Buffer
char ring_buffer[BUFFER_SIZE][MAX_MSG_LEN];
int head = 0; // Δείκτης εγγραφής (Παραγωγός)
int tail = 0; // Δείκτης ανάγνωσης (Καταναλωτής)
int buffer_count = 0;// Τρέχουσα πληρότητα buffer

// Καθολικοί Μετρητές Μηνυμάτων 
int count_commit = 0;
int count_identity = 0;
int count_account = 0;
int count_info = 0;
volatile int network_active = 0;

// Εργαλεία Συγχρονισμού (POSIX)
// Mutex και Condition Variable για τον Κυκλικό Buffer
pthread_mutex_t buffer_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t data_cond = PTHREAD_COND_INITIALIZER;

// Mutex για την προστασία των μετρητών (όταν τους διαβάζει ο Καταγραφέας)
pthread_mutex_t stats_mutex = PTHREAD_MUTEX_INITIALIZER;

// Συνάρτηση Καταναλωτή (Νήμα 2) 
void *consumer_thread(void *arg) {
    char local_msg[MAX_MSG_LEN];

    while (1) {
        //  Προστασία και Ανάγνωση από τον Κυκλικό Buffer
        pthread_mutex_lock(&buffer_mutex);

        // Το νήμα κοιμάται όσο ο buffer είναι άδειος, περιμένοντας σήμα από τον Παραγωγό
        while (buffer_count == 0) {
            pthread_cond_wait(&data_cond, &buffer_mutex);
        }

        // Αντιγραφή του μηνύματος σε τοπική μεταβλητή για να ελευθερώσουμε τον buffer γρήγορα
        strncpy(local_msg, ring_buffer[tail], MAX_MSG_LEN);
        local_msg[MAX_MSG_LEN - 1] = '\0'; // Ασφάλεια τερματισμού string

        // Ενημέρωση των δεικτών του κυκλικού buffer (αφαίρεση στοιχείου)
        tail = (tail + 1) % BUFFER_SIZE;
        buffer_count--;

        // Ξεκλειδώνουμε αμέσως τον buffer για να μπορεί ο Παραγωγός να συνεχίσει να γράφει
        pthread_mutex_unlock(&buffer_mutex);

        //  Επεξεργασία JSON (Εκτός του buffer_mutex για να μην καθυστερούμε το δίκτυο)
        cJSON *json = cJSON_Parse(local_msg);
        if (json == NULL) {
            continue; // Αν το JSON είναι άκυρο ή σφάλμα δικτύου, πάμε στο επόμενο
        }

        // Αναζήτηση του πεδίου "kind"
        cJSON *kind = cJSON_GetObjectItemCaseSensitive(json, "kind");

        //  Ενημέρωση των Καθολικών Μετρητών με ασφάλεια (stats_mutex)
        if (cJSON_IsString(kind) && (kind->valuestring != NULL)) {
            
            // Κλειδώνουμε μόνο τους μετρητές, όχι όλο τον buffer
            pthread_mutex_lock(&stats_mutex);

            if (strcmp(kind->valuestring, "commit") == 0) {
                count_commit++;
            } else if (strcmp(kind->valuestring, "identity") == 0) {
                count_identity++;
            } else if (strcmp(kind->valuestring, "account") == 0) {
                count_account++;
            } else {
                // Αν είναι "info" ή οτιδήποτε άλλο άγνωστο
                count_info++;
            }

            pthread_mutex_unlock(&stats_mutex);
        }

        // Απελευθέρωση μνήμης του cJSON struct (για να μην έχουμε memory leaks)
        cJSON_Delete(json);
    }

    return NULL;
}

// Βοηθητικές μεταβλητές για τον υπολογισμό της CPU 
unsigned long long prev_total_jiffies = 0;
unsigned long long prev_idle_jiffies = 0;

// Συνάρτηση για τον υπολογισμό της χρήσης CPU από το /proc/stat
double get_cpu_usage() {
    FILE *fp = fopen("/proc/stat", "r");
    if (fp == NULL) return 0.0;

    char buffer[256];
    fgets(buffer, sizeof(buffer), fp);
    fclose(fp);

    unsigned long long user, nice, system, idle, iowait, irq, softirq, steal, guest, guest_nice;
    sscanf(buffer, "cpu %llu %llu %llu %llu %llu %llu %llu %llu %llu %llu", 
           &user, &nice, &system, &idle, &iowait, &irq, &softirq, &steal, &guest, &guest_nice);

    unsigned long long current_idle = idle + iowait;
    unsigned long long current_total = user + nice + system + current_idle + irq + softirq + steal + guest + guest_nice;

    double cpu_usage = 0.0;
    if (prev_total_jiffies != 0) {
        unsigned long long totald = current_total - prev_total_jiffies;
        unsigned long long idled = current_idle - prev_idle_jiffies;
        cpu_usage = (double)(totald - idled) / totald * 100.0;
    }

    prev_total_jiffies = current_total;
    prev_idle_jiffies = current_idle;

    return cpu_usage;
}


// Συνάρτηση ανάγνωσης θερμοκρασίας CPU 
double get_cpu_temp() {
    FILE *fp = fopen("/sys/class/thermal/thermal_zone0/temp", "r");
    if (fp == NULL) return 0.0;
    
    int temp_millidegrees;
    fscanf(fp, "%d", &temp_millidegrees);
    fclose(fp);
    
    return temp_millidegrees / 1000.0; // Μετατροπή σε βαθμούς Κελσίου
}


// Συνάρτηση Καταγραφέα / Monitor (Νήμα 3) 
void *monitor_thread(void *arg) {
    FILE *log_file = fopen("metrics_log.txt", "a");
    if (log_file == NULL) {
        perror("Αποτυχία ανοίγματος αρχείου καταγραφής");
        pthread_exit(NULL);
    }
    
    // Εγγραφή της επικεφαλίδας του CSV
    fseek(log_file, 0, SEEK_END);
    if (ftell(log_file) == 0) {
        fprintf(log_file, "Seconds,Nanoseconds,Commit_Count,Identity_Count,Account_Count,Info_Count,Buffer_Occupancy_Pct,CPU_Pct\n");
        fflush(log_file);
    }

    struct timespec next_wakeup, current_time;
    // Χρησιμοποιούμε CLOCK_MONOTONIC για τον ακριβή ρυθμό του 1 δευτερολέπτου
    clock_gettime(CLOCK_MONOTONIC, &next_wakeup);

    while (1) {
        // Ακριβής προσθήκη 1 δευτερολέπτου (1.000.000.000 νανοδευτερόλεπτα) 
        next_wakeup.tv_sec += 1;
        
        // Το νήμα κοιμάται ΜΕΧΡΙ την απόλυτη στιγμή (TIMER_ABSTIME)
        clock_nanosleep(CLOCK_MONOTONIC, TIMER_ABSTIME, &next_wakeup, NULL);

        //  Λήψη ακριβούς ώρας καταγραφής (CLOCK_REALTIME)
        clock_gettime(CLOCK_REALTIME, &current_time);

        // Λήψη και άμεσος μηδενισμός στατιστικών 
        pthread_mutex_lock(&stats_mutex);
        int l_commit = count_commit; count_commit = 0;
        int l_identity = count_identity; count_identity = 0;
        int l_account = count_account; count_account = 0;
        int l_info = count_info; count_info = 0;
        pthread_mutex_unlock(&stats_mutex);

        //  Υπολογισμός πληρότητας Buffer (%) 
        pthread_mutex_lock(&buffer_mutex);
        int current_buffer_count = buffer_count;
        pthread_mutex_unlock(&buffer_mutex);
        double buffer_pct = ((double)current_buffer_count / BUFFER_SIZE) * 100.0;

       
        // Υπολογισμός Χρήσης CPU (%) 
        double cpu_pct = get_cpu_usage();

        //  ΕΛΕΓΧΟΣ ΘΕΡΜΟΚΡΑΣΙΑΣ 
        double temp_celsius = get_cpu_temp();
        if (temp_celsius > 65.0) {
            printf("\n[ΠΡΟΕΙΔΟΠΟΙΗΣΗ] Υψηλή θερμοκρασία: %.1f°C (Timestamp: %ld)\n", temp_celsius, current_time.tv_sec);
        }

       if (network_active) {
            // Κανονική καταγραφή όταν έχουμε ίντερνετ
            fprintf(log_file, "%ld,%ld,%d,%d,%d,%d,%.2f,%.2f\n",
                    current_time.tv_sec, current_time.tv_nsec,
                    l_commit, l_identity, l_account, l_info,
                    buffer_pct, cpu_pct);
        } else {
            // Καταγραφή NaN στα μηνύματα και στο buffer. Ο χρόνος και η CPU συνεχίζουν
            fprintf(log_file, "%ld,%ld,NaN,NaN,NaN,NaN,NaN,%.2f\n",
                    current_time.tv_sec, current_time.tv_nsec,
                    cpu_pct);
        }
        
        // Εξαναγκασμός εγγραφής (flush) ώστε να μην μένουν δεδομένα στη μνήμη RAM του Pi
        fflush(log_file);
    }

    fclose(log_file);
    return NULL;
}


//  Callback Συνάρτηση για τη libwebsockets 
static int callback_jetstream(struct lws *wsi, enum lws_callback_reasons reason,
                              void *user, void *in, size_t len) {
    struct timespec ts;

    switch (reason) {
        case LWS_CALLBACK_CLIENT_ESTABLISHED:
        network_active = 1;
            printf("[NETWORK] Συνδέθηκε επιτυχώς στο Jetstream Firehose!\n");
            break;

        case LWS_CALLBACK_CLIENT_RECEIVE:
            pthread_mutex_lock(&buffer_mutex);
            if (buffer_count < BUFFER_SIZE) {
                size_t copy_len = (len < MAX_MSG_LEN - 1) ? len : (MAX_MSG_LEN - 1);
                strncpy(ring_buffer[head], (char *)in, copy_len);
                ring_buffer[head][copy_len] = '\0'; 
                head = (head + 1) % BUFFER_SIZE;
                buffer_count++;
                pthread_cond_signal(&data_cond);
            }
            pthread_mutex_unlock(&buffer_mutex);
            break;

        case LWS_CALLBACK_CLIENT_CONNECTION_ERROR:
        network_active = 0;
        case LWS_CALLBACK_CLIENT_CLOSED:
            clock_gettime(CLOCK_REALTIME, &ts);
            printf("\n[NETWORK] Η σύνδεση χάθηκε στο δευτερόλεπτο %ld!\n", ts.tv_sec);
            printf("[NETWORK] Ενεργοποίηση αυτόματης επανασύνδεσης...\n");
            network_active = 0; // Δίνουμε σήμα να σπάσει ο βρόχος
            break;

        default:
            break;
    }
    return 0;
}

// Ορισμός των πρωτοκόλλων LWS
static struct lws_protocols protocols[] = {
    { "jetstream-protocol", callback_jetstream, 0, MAX_MSG_LEN, 0, NULL, 0 },
    { NULL, NULL, 0, 0, 0, NULL, 0 }
};

//  Συνάρτηση Παραγωγού (Νήμα 1) 
void *producer_thread(void *arg) {
    while (1) { 
        struct lws_context_creation_info info;
        struct lws_client_connect_info ccinfo;
        struct lws_context *context;

        memset(&info, 0, sizeof info);
        info.port = CONTEXT_PORT_NO_LISTEN;
        info.protocols = protocols;
        info.options = LWS_SERVER_OPTION_DO_SSL_GLOBAL_INIT;

        context = lws_create_context(&info);
        if (!context) {
            sleep(2);
            continue;
        }

        memset(&ccinfo, 0, sizeof ccinfo);
        ccinfo.context = context;
        ccinfo.address = "jetstream1.us-east.bsky.network";
        ccinfo.port = 443;
        ccinfo.path = "/subscribe?wantedCollections=app.bsky.feed.post";
        ccinfo.host = ccinfo.address;
        ccinfo.origin = ccinfo.address;
        ccinfo.protocol = protocols[0].name;
        ccinfo.ssl_connection = LCCSCF_USE_SSL;

        struct lws *wsi = lws_client_connect_via_info(&ccinfo);
        if (wsi) {
            network_active = 1; // Ενεργοποίηση της σημαίας πριν τον βρόχο
            // Τρέχει ασταμάτητα όσο η σημαία είναι 1 και η lws_service δεν βγάζει σφάλμα
            while (network_active && lws_service(context, 0) >= 0) {
                // To libwebsockets αναλαμβάνει τη λήψη στο παρασκήνιο
            }
        }

        lws_context_destroy(context);
        sleep(3); 
    }
    return NULL;
}

int main() {
    printf("==========================================\n");
    printf("Εκκίνηση Συστήματος Τηλεμετρίας Jetstream\n");
    printf("==========================================\n");

    pthread_t producer, consumer, monitor;

    // Δημιουργία των 3 Νημάτων
    pthread_create(&consumer, NULL, consumer_thread, NULL);
    pthread_create(&monitor, NULL, monitor_thread, NULL);
    pthread_create(&producer, NULL, producer_thread, NULL);

    // Το πρόγραμμα περιμένει τα νήματα επ' άπειρον (δεν θα τερματίσουν ποτέ)
    pthread_join(producer, NULL);
    pthread_join(consumer, NULL);
    pthread_join(monitor, NULL);

    return 0;
} 
