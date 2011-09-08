/*
 * Copyright (c) 2010 - 2011, Liexusong <liexusong@qq.com>
 * All rights reserved.
 * Please visit http://code.google.com/p/squirrel-message-queue/
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

/*
 * commands:=======================================
 * Put item into the queue: (PUTF/PUTL)
 * Pop item from the queue: (POPF/POPL/POPP/POPS)
 * Get item from the queue: (GETF/GETL/GETP/GETS)
 * Get status of the queue: (STAT/SLAB)
 * Get the size of the queue: (SIZE)
 * Auth the server: (AUTH)
 * Or more ......
 *=================================================
 *                   d(>_<)b
 *                  liexusong
 *                  @旭松_Lie
 *=================================================
 */

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/socket.h>
#include <sys/signal.h>
#include <sys/resource.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <fcntl.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdarg.h>
#include <unistd.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <errno.h>
#include <time.h>
#include <pthread.h>
/* lua libs */
#include <lua.h>
#include <lualib.h>
#include <lauxlib.h>
/* squirrel libs */
#include "ae.h"
#include "slabs.h"
#include "dlist.h"

/* defined DEBUG macro can show the message on console */
#if 0
#define DEBUG 1
#endif

#define SQUIRREL_VERSION "1.3 Final"

#define DEFAULT_PORT 6061
#define CLIENT_CACHES_SIZE 1000
#define RDB_HEADER_BLOCK "SQUIRREL0013"

#define RECV_BUFF_SIZE 2048
#define SEND_BUFF_SIZE 2048
#define IGNORE_BUFF_SIZE 2048

#define LOG_ERROR  0
#define LOG_NOTICE 1
#define LOG_DEBUG  2

#define TOKEN_MAX_LEN 256
#define TOKEN_DEFAULT_SIZE 20

typedef enum
{
    RECV_STATE,  /* read state */
    NRECV_STATE, /* nread state */
    CRLF_STATE,  /* crlf state */
    IGNORE_STATE,/* ignore state */
    SEND_STATE,  /* write state */
    MSEND_STATE, /* msend state */
    CLOSE_STATE  /* close state */
} smq_status_t;

typedef struct smq_client_s smq_client_t;
struct smq_client_s {
    int fd;
    int flag;
    smq_status_t state;
    char cRecvBuffer[RECV_BUFF_SIZE];
    int cRecvBytes;
    int cRecvWhere;
    short crlf_bytes;
    char cSendBuffer[SEND_BUFF_SIZE];
    int cSendBytes;
    int cSendWhere;
    char cIgnoreBuffer[IGNORE_BUFF_SIZE];
    int cIgnoreBytes;
    struct list *replyList;
    int replyStage;
    time_t lastTransportTime;
    char *mSendBuffer;
    char *mRecvBuffer;
    int isauth;
    smq_client_t *prev;
    smq_client_t *next;
};

typedef struct {
    int fd;
    short listingPort;
    aeEventLoop *eventLoop;
    struct list *queue;
    smq_client_t *clientCaches[CLIENT_CACHES_SIZE];
    int clientCachesUsed;
    int secondsToSaveDisk;
    int memoryLimitUsed;
    int memoryUsedTotal;
    pid_t bgSaveChildpid;
    time_t lastSaveTime;
    int clientExpiredTime;
    int dirty;
    int chagesToSaveDisk;
    int cronLoops;
    int daemonize;
    int connections;
    int enableauth;
    char *authpwd;
    char *luaFilePath;
    char *luaMainFunction;
    char *pidFilePath;
    smq_client_t *clientListHead;
    smq_client_t *clientListTail;
} smq_server_t;

struct config_t {
    char *key;
    char *val;
    struct config_t *next;
};

typedef struct {
    int refcount;
    int blockSize;
    char block[0];
} smq_block_t;

typedef struct {
    char token[TOKEN_MAX_LEN + 1];
} token_t;

typedef int (*do_command_cb)(smq_client_t *conn, token_t *tokens, int tokencount);

typedef struct {
    char *cmdname;
    do_command_cb cb;
    int argc;
} command_cb_t;

int putf_command(smq_client_t *conn, token_t *tokens, int tokencount);
int putl_command(smq_client_t *conn, token_t *tokens, int tokencount);
int popf_command(smq_client_t *conn, token_t *tokens, int tokencount);
int popl_command(smq_client_t *conn, token_t *tokens, int tokencount);
int popp_command(smq_client_t *conn, token_t *tokens, int tokencount);
int pops_command(smq_client_t *conn, token_t *tokens, int tokencount);
int getf_command(smq_client_t *conn, token_t *tokens, int tokencount);
int getl_command(smq_client_t *conn, token_t *tokens, int tokencount);
int getp_command(smq_client_t *conn, token_t *tokens, int tokencount);
int gets_command(smq_client_t *conn, token_t *tokens, int tokencount);
int stat_command(smq_client_t *conn, token_t *tokens, int tokencount);
int slab_command(smq_client_t *conn, token_t *tokens, int tokencount);
int size_command(smq_client_t *conn, token_t *tokens, int tokencount);
int close_command(smq_client_t *conn, token_t *tokens, int tokencount);

static command_cb_t callbacks[] = {
    {"PUTF", putf_command, 2},
    {"PUTL", putl_command, 2},
    {"POPF", popf_command, 1},
    {"POPL", popl_command, 1},
    {"POPP", popp_command, 2},
    {"POPS", pops_command, 2},
    {"GETF", getf_command, 1},
    {"GETL", getl_command, 1},
    {"GETP", getp_command, 2},
    {"GETS", gets_command, 2},
    {"STAT", stat_command, 1},
    {"SLAB", slab_command, 1},
    {"SIZE", size_command, 1},
    {"CLOSE", close_command, 1},
    {NULL, NULL, 0}
};

void machine(smq_client_t *conn);
void clientCloseRemove(smq_client_t *conn);
int serverCron(struct aeEventLoop *eventLoop, long long id, void *clientData);
int doItemDecrRefcount(void *item);

#define do_connection_status(conn, _state) ((conn)->state = _state)

#define do_connection_recv_empty(conn) do \
{\
    (conn)->cRecvBytes = 0;\
    (conn)->cRecvWhere = 0;\
} while(0)

#define do_connection_send_empty(conn) do \
{\
    (conn)->cSendBytes = 0;\
    (conn)->cSendWhere = 0;\
} while(0)

#define do_connection_nrecvbuf_empty(conn) ((conn)->mRecvBuffer = NULL)
#define do_connection_nsendbuf_empty(conn) ((conn)->mSendBuffer = NULL)

#define do_client_link(conn) do \
{\
    conn->prev = NULL;\
    conn->next = server.clientListHead;\
    if (server.clientListHead)\
        server.clientListHead->prev = conn;\
    server.clientListHead = conn;\
    if (!server.clientListTail)\
        server.clientListTail = server.clientListHead;\
} while(0)

#define do_client_unlink(conn) do \
{\
    if (conn->prev)\
        conn->prev->next = conn->next;\
    else\
        server.clientListHead = conn->next;\
    if (conn->next)\
        conn->next->prev = conn->prev;\
    else\
        server.clientListTail = conn->prev;\
} while(0)

#define thread_lock_list()   pthread_mutex_lock(&listGlobalLock)
#define thread_unlock_list() pthread_mutex_unlock(&listGlobalLock)

/* There are global varibles for server */
static smq_server_t server;
static struct config_t *config_item_head = NULL;
static pthread_mutex_t listGlobalLock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t listGlobalCondition = PTHREAD_COND_INITIALIZER;
static pthread_mutex_t serverGlobalLock = PTHREAD_MUTEX_INITIALIZER;

void smqLog(int level, const char *fmt, ...) {
    va_list ap;
    FILE *fp;
    char log_path[1024];
    struct tm *tm;
    time_t ts;

#if defined(DEBUG) && DEBUG
    fp = stdout;
#else
    ts = time(NULL);
    tm = gmtime(&ts);
    
    sprintf(log_path, "squirrel.%d%d.log", tm->tm_year + 1900, tm->tm_mon + 1);
    
    fp = fopen(log_path, "a+");
    if (!fp) return;
#endif

    va_start(ap, fmt);
    char *c = "-*+";
    char buf[64];
    time_t now;

    now = time(NULL);
    strftime(buf, 64, "[%d %b %H:%M:%S]", gmtime(&now));
    fprintf(fp, "%s %c ", buf, c[level]);
    vfprintf(fp, fmt, ap);
    fprintf(fp, "\n");
    fflush(fp);
    va_end(ap);

#ifndef DEBUG
    fclose(fp);
#endif
}

int setNonblock(int fd) {
    int flags;
    if ((flags = fcntl(fd, F_GETFL, 0)) < 0 || fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0)
        return -1;
    return 0;
}

void eventActionFunction(struct aeEventLoop *eventLoop, int fd, void *clientData, int mask) {
    smq_client_t *conn = (smq_client_t *)clientData;
    
    if (conn->fd != fd) {
        clientCloseRemove(conn);
        smqLog(LOG_NOTICE, "catastrophic, event fd doesn't match conn fd!\n");
        return;
    }
    machine(conn);
    return;
}

smq_client_t *clientAllocObject(int fd, smq_status_t state, int flag) {
    smq_client_t *conn = NULL;
    
    pthread_mutex_lock(&serverGlobalLock);
    if (server.clientCachesUsed > 0)
        conn = server.clientCaches[--server.clientCachesUsed];
    pthread_mutex_unlock(&serverGlobalLock);
    
    if (NULL == conn) {
        conn = (smq_client_t *)malloc(sizeof(smq_client_t));
        if (!conn) {
            smqLog(LOG_NOTICE, "Not enough memory to alloc squirrelClient");
            return NULL;
        }
        
        conn->replyList = list_init((list_destroy_function_t)doItemDecrRefcount, 128);
        if (!conn->replyList) {
            free(conn);
            smqLog(LOG_NOTICE, "Not enough memory to alloc replyList");
            return NULL;
        }
    }
    
    conn->fd = fd;
    conn->flag = flag;
    do_connection_status(conn, state);
    do_connection_recv_empty(conn);
    do_connection_send_empty(conn);
    conn->crlf_bytes = 0;
    conn->cIgnoreBytes = 0;
    conn->replyStage = 0;
    conn->mSendBuffer = NULL;
    conn->mRecvBuffer = NULL;
    conn->lastTransportTime = time(NULL);
    conn->isauth = 0;
    
    pthread_mutex_lock(&serverGlobalLock);
    aeCreateFileEvent(server.eventLoop, conn->fd, AE_READABLE, eventActionFunction, (void *)conn);
    do_client_link(conn);
    server.connections++;
    pthread_mutex_unlock(&serverGlobalLock);
    
    return conn;
}

void clientCloseRemove(smq_client_t *conn) {
    if (conn == NULL)
        return;
    close(conn->fd);
    pthread_mutex_lock(&serverGlobalLock);
    aeDeleteFileEvent(server.eventLoop, conn->fd, AE_READABLE | AE_WRITABLE);
    do_client_unlink(conn);
    server.connections--;
    if (server.clientCachesUsed < CLIENT_CACHES_SIZE) {
        server.clientCaches[server.clientCachesUsed++] = conn;
        pthread_mutex_unlock(&serverGlobalLock);
        return;
    }
    pthread_mutex_unlock(&serverGlobalLock);
    
    list_destroy(conn->replyList);
    free(conn);
    
    return;
}

#if 0
void acceptActionFunction(struct aeEventLoop *eventLoop, int fd, void *clientData, int mask) {
    int cfd, flags = 1;
    socklen_t addrlen;
    struct sockaddr addr;
    smq_client_t *nc;
    
    addrlen = sizeof(addr);
    if ((cfd = accept(fd, &addr, &addrlen)) == -1) {
        if (errno != EAGAIN && errno != EWOULDBLOCK) {
            smqLog(LOG_ERROR, "Unable accept client connection");
        }
        goto EXIT_FALG;
    }
    
    if (setNonblock(cfd) == -1) {
        smqLog(LOG_ERROR, "Unable set file to nonblock mode");
        close(cfd);
        goto EXIT_FALG;
    }
    
    nc = clientAllocObject(cfd, RECV_STATE, AE_READABLE);
    if (!nc) {
        smqLog(LOG_ERROR, "Unable create new client connection");
        close(cfd);
    }

EXIT_FALG:
    return;
}
#endif

void *acceptThreadFunction(void *arg) {
    fd_set read_fd;
    int sfd = server.fd, cfd;
    int flags = 1;
    socklen_t addrlen;
    struct sockaddr addr;
    smq_client_t *conn;
    int retval;
    
    FD_ZERO(&read_fd);
    FD_SET(sfd, &read_fd);
    
    while (1) {
        retval = select(sfd + 1, &read_fd, NULL, NULL, NULL);
        switch (retval) {
        case -1:
            FD_ZERO(&read_fd);
            FD_SET(sfd, &read_fd);
            perror("select()");
            continue;
        break;
        case 0:
            continue;
        break;
        }
        
        if (FD_ISSET(sfd, &read_fd)) {
            addrlen = sizeof(addr);
            if ((cfd = accept(sfd, &addr, &addrlen)) == -1) {
                if (errno != EAGAIN && errno != EWOULDBLOCK) {
                    smqLog(LOG_ERROR, "Unable accept client connection");
                }
                continue;
            }
            
            if (setNonblock(cfd) == -1) {
                smqLog(LOG_ERROR, "Unable set file to nonblock mode");
                close(cfd);
                continue;
            }
            
            conn = clientAllocObject(cfd, RECV_STATE, AE_READABLE);
            if (!conn) {
                smqLog(LOG_ERROR, "Unable create new client connection");
                close(cfd);
                continue;
            }
        } else {
            FD_ZERO(&read_fd);
            FD_SET(sfd, &read_fd);
            continue;
        }
    }
    
    pthread_exit(NULL);
}

static char *skipSpace(char *string) {
    int len;
    while (*string == ' ' || *string == '\t')
        string++;
    len = strlen(string);
    if (string[len - 2] == '\r')/* windows? */
        string[len - 2] = '\0';
    if (string[len - 1] == '\n')
        string[len - 1] = '\0';
    return string;
}

void loadConfigFile(const char *file) {
    FILE *handle;
    char buf[1024], *cursor;
    char key[256], val[512];
    struct config_t *item;
    
    handle = fopen(file, "r");
    if (NULL == handle) {
        fprintf(stderr, "[Fatal] Unable open configure file.\n");
        exit(-1);
    }
    
    while (fgets(buf, 1024, handle)) {
        cursor = skipSpace(buf);
        if (*cursor == '#')/* comments */
            continue;
        sscanf(cursor, "%[^ ]%*[ ]%512[^ #]", key, val);
        item = (struct config_t *)malloc(sizeof(*item));
        if (NULL == item) {
            fprintf(stderr, "[Fatal] Not enough memory to alloc config entry.\n");
            exit(-1);
        }
        item->key = strdup(key);
        item->val = strdup(val);
        item->next = config_item_head;
        config_item_head = item;
    }
    
    fclose(handle);
}

void loadConfig() {
    struct config_t *item = config_item_head, *temp;
    
    if (NULL == item) {
        fprintf(stderr, "[Fatal] Haven't load config file yet.\n");
        exit(-1);
    }
    
    while (item) {
        if (!strcasecmp(item->key, "listingport"))
            server.listingPort = atoi(item->val);
        else if (!strcasecmp(item->key, "memorylimitused"))
            server.memoryLimitUsed = atoi(item->val);
        else if (!strcasecmp(item->key, "secondstosavedisk"))
            server.secondsToSaveDisk = atoi(item->val);
        else if (!strcasecmp(item->key, "chagestosavedisk"))
            server.chagesToSaveDisk = atoi(item->val);
        else if (!strcasecmp(item->key, "clientexpiredtime"))
            server.clientExpiredTime = atoi(item->val);
        else if (!strcasecmp(item->key, "cronloops"))
            server.cronLoops = atoi(item->val);
        else if (!strcasecmp(item->key, "daemonize"))
            server.daemonize = atoi(item->val);
        else if (!strcasecmp(item->key, "enableauth"))
            server.enableauth = atoi(item->val);
        else if (!strcasecmp(item->key, "authpwd"))
            server.authpwd = strdup(item->val);
        else if (!strcasecmp(item->key, "luafilepath"))
            server.luaFilePath = strdup(item->val);
        else if (!strcasecmp(item->key, "luamainfunction"))
            server.luaMainFunction = strdup(item->val);
        else if (!strcasecmp(item->key, "pidfilepath"))
            server.pidFilePath = strdup(item->val);
        else
            fprintf(stderr, "[Warning] Not found the config option (%s => %s)\n", 
                item->key, item->val);
        temp = item->next;
        free(item);
        item = temp;
    }
    return;
}

void initServer()
{
    struct linger ling = {0, 0};
    struct sockaddr_in addr;
    int flags = 1;
    
    server.fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server.fd == -1) {
        fprintf(stderr, "[Fatal] Unable create socket\n");
        exit(-1);
    }
    
    if (setNonblock(server.fd) == -1) {
        fprintf(stderr, "[Fatal] Unable set nonblock\n");
        exit(-1);
    }
    
    setsockopt(server.fd, SOL_SOCKET, SO_REUSEADDR, &flags, sizeof(flags));
    setsockopt(server.fd, SOL_SOCKET, SO_KEEPALIVE, &flags, sizeof(flags));
    setsockopt(server.fd, SOL_SOCKET, SO_LINGER, &ling, sizeof(ling));
#if !defined(TCP_NOPUSH)
    setsockopt(server.fd, IPPROTO_TCP, TCP_NODELAY, &flags, sizeof(flags));
#endif
    
    addr.sin_family = AF_INET;
    addr.sin_port = htons(server.listingPort);
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    
    if (bind(server.fd, (struct sockaddr *) &addr, sizeof(addr)) == -1) {
        fprintf(stderr, "[Fatal] Unable bind socket\n");
        close(server.fd);
        exit(-1);
    }
    
    if (listen(server.fd, 1024) == -1) {
        fprintf(stderr, "[Fatal] Unable bind listen\n");
        close(server.fd);
        exit(-1);
    }
    
    server.eventLoop = aeCreateEventLoop();
    if (!server.eventLoop) {
        fprintf(stderr, "[Fatal] Unable alloc EventLoop.\n");
        exit(-1);
    }
    
    server.queue = list_init((list_destroy_function_t)doItemDecrRefcount, 2048);
    if (!server.queue) {
        fprintf(stderr, "[Fatal] Unable alloc DoubleList.\n");
        exit(-1);
    }
    
    server.clientListHead = NULL;
    server.clientListTail = NULL;
    server.clientCachesUsed = 0;
    server.memoryUsedTotal = 0;
    server.bgSaveChildpid = -1;
    server.lastSaveTime = 0;
    server.dirty = 0;
    server.connections = 0;
    
    /* Remove it: accept client connect */
    /* aeCreateFileEvent(server.eventLoop, server.fd, AE_READABLE, acceptActionFunction, NULL); */
    aeCreateTimeEvent(server.eventLoop, 1, serverCron, NULL, NULL);
    
    return;
}

/* send $SUC when success, send #ERR when faild */
void doSendReply(smq_client_t *conn, char *str, int createEvent) {
    int len;

    len = strlen(str);
    if (len + 2 > SEND_BUFF_SIZE) {
        /* ought to be always enough. just fail for simplicity */
        str = "#ERR: Output string too long";
        len = strlen(str);
    }

    strcpy(conn->cSendBuffer, str);
    strcat(conn->cSendBuffer, "\r\n");
    conn->cSendBytes = len + 2;
    conn->cSendWhere = 0;
    do_connection_status(conn, SEND_STATE);
    
    /* send the string to client */
    if (createEvent)
        aeCreateFileEvent(server.eventLoop, conn->fd, AE_WRITABLE, eventActionFunction, conn);
    return;
}

smq_block_t *doItemAlloc(int size) {
    smq_block_t *item;
    int doAllocSize = sizeof(smq_block_t) + size;
    
    item = slabs_alloc(doAllocSize);
    if (item) {
        item->refcount = 1;
        item->blockSize = size;
        server.memoryUsedTotal += size;
    }
    return item;
}

smq_block_t *doItemAllocString(const char *string) {
    smq_block_t *item;
    int size = strlen(string);
    int doAllocSize = sizeof(smq_block_t) + size;
    
    item = slabs_alloc(doAllocSize);
    if (item) {
        item->refcount = 1;
        item->blockSize = size;
        memcpy(item->block, string, size);
        server.memoryUsedTotal += size;
    }
    return item;
}

int doItemIncrRefcount(smq_block_t *item) {
    return item->refcount++;/* return last item refcount number */
}

int doItemDecrRefcount(void *item) {
    if (item) {
        smq_block_t *it = (smq_block_t *)item;
        if (--it->refcount == 0) {
            int doAllocSize = it->blockSize + sizeof(smq_block_t);
            slabs_free(it, doAllocSize);
            server.memoryUsedTotal -= it->blockSize;
        }
        return 0;
    }
    return -1;
}

/* make crlf item */
smq_block_t *makeNLObject() {
    smq_block_t *it = doItemAlloc(2);
    if (it) {
        it->block[0] = '\r';
        it->block[1] = '\n';
    } else {
        smqLog(LOG_ERROR, "Slabs not enough memory to alloc CRLF item");
        return NULL;
    }
    return it;
}

int parseToken(const char *str, token_t *tokens, int *count) {
    int begin = 0, end = 0;
    int index = 0;
    int tokenLen;
    const char *sptr = str;
    
    for (; *sptr; sptr++, end++) {
        if (*sptr == ' ') {
            tokenLen = end - begin;
            if (tokenLen > TOKEN_MAX_LEN) {
                fprintf(stderr, "[Fatal] Token too long.\n");
                return -1;
            }
            memcpy(tokens[index++].token, str + begin, tokenLen);
            begin = end + 1;
            if (index == TOKEN_DEFAULT_SIZE) {
                fprintf(stderr, "[Fatal] Tokens too must.\n");
                *count = index;
                return -1;
            }
        }
    }
    
    memcpy(tokens[index++].token, str + begin, end - begin); /* must copy the last token */
    *count = index;
    
    return 0;
}

command_cb_t lookupCommand(char *cmdname) {
    command_cb_t cb;
    int i = 0;
    
    while (1) {
        cb = callbacks[i];
        if (!cb.cmdname || !strcasecmp(cb.cmdname, cmdname))
            break;
        i++;
    }
    return cb;
}

void tryProcessCommand(smq_client_t *conn) {
    char *el, *cont;
    token_t tokens[TOKEN_DEFAULT_SIZE];
    int tokencount;
    int where, retval;
    int authCmd = 0;
    command_cb_t cb;
    
    if (conn->cRecvWhere <= 0)
        return;
    el = memchr(conn->cRecvBuffer, '\n', conn->cRecvWhere);
    if (!el)
        return;
    cont = el + 1;
    if (el - conn->cRecvBuffer > 1 && *(el - 1) == '\r')
        el--;
    *el = '\0';
    
    memset(tokens, 0, sizeof(tokens));
    if (parseToken(conn->cRecvBuffer, tokens, &tokencount) != 0) {
        doSendReply(conn, "#ERR: Tokens too must", 1);
        return;
    }
    
    /* Login check */
    if (server.enableauth && !conn->isauth) {
        if (!strcasecmp(tokens[0].token, "AUTH")) {
            authCmd = 1;
        } else {
            do_connection_status(conn, CLOSE_STATE);/* close this connection */
            return;
        }
    }
    
    if (authCmd) {
        if (tokencount != 2) {
            doSendReply(conn, "#ERR: Command parameters wrong", 1);
            return;
        }
        
        if (!strcmp(tokens[1].token, server.authpwd)) {
            conn->isauth = 1;
            doSendReply(conn, "+FIN: OK", 1);
            return;
        } else {
            conn->isauth = 0;
            doSendReply(conn, "#ERR: Password is incorrect", 1);
            return;
        }
    }
    
    cb = lookupCommand(tokens[0].token);
    if (!cb.cb) {
        doSendReply(conn, "#ERR: Not found this command in system", 1);
        return;
    }
    
    if (tokencount != cb.argc) {
        doSendReply(conn, "#ERR: Command parameters wrong", 1);
        return;
    }
    cb.cb(conn, tokens, tokencount);
    return;
}

/* drive machine: all event proccess in this function */
void machine(smq_client_t *conn) {
    int exit = 0;
    int flags = 1;
    int retval;

    while (!exit)
    {
        switch(conn->state)
        {
        case RECV_STATE: {
            if (conn->cRecvWhere >= RECV_BUFF_SIZE) {/* command line too long? */
                do_connection_status(conn, CLOSE_STATE);
                fprintf(stderr, "[Fatal] Command line too long\n");
                break;
            }
            
            retval = read(conn->fd, conn->cRecvBuffer + conn->cRecvWhere, RECV_BUFF_SIZE - conn->cRecvWhere);
            if (retval == -1) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    exit = 1;
                    break;
                } else {
                    fprintf(stderr, "[Fatal] Failed to read, and not due to blocking\n");
                    do_connection_status(conn, CLOSE_STATE);
                    break;
                }
            } else if (retval == 0) {/* This client connection was closed */
                do_connection_status(conn, CLOSE_STATE);
                break;
            }
            
            conn->cRecvWhere += retval;
            conn->lastTransportTime = time(NULL);
            tryProcessCommand(conn);
            break;
        }
        case NRECV_STATE: {
            if (conn->cRecvBytes <= 0) {
                conn->crlf_bytes = 0;
                do_connection_status(conn, CRLF_STATE);
                break;
            }
            
            retval = read(conn->fd, conn->mRecvBuffer + conn->cRecvWhere, conn->cRecvBytes);
            if (retval == -1) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    exit = 1;
                    break;
                } else {
                    fprintf(stderr, "[Fatal] Failed to read, and not due to blocking\n");
                    do_connection_status(conn, CLOSE_STATE);
                    break;
                }
            } else if (retval == 0) {/* This client connection was closed */
                do_connection_status(conn, CLOSE_STATE);
                break;
            }
            
            conn->cRecvWhere += retval;
            conn->cRecvBytes -= retval;
            conn->lastTransportTime = time(NULL);
            break;
        }
        case IGNORE_STATE: {
            if (conn->cIgnoreBytes <= 0) {
                conn->crlf_bytes = 0;
                do_connection_status(conn, CRLF_STATE);
                break;
            }
            
            retval = read(conn->fd, conn->cIgnoreBuffer,
                    (conn->cIgnoreBytes > IGNORE_BUFF_SIZE ? IGNORE_BUFF_SIZE : conn->cIgnoreBytes));
            if (retval == -1) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    exit = 1;
                    break;
                } else {
                    fprintf(stderr, "[Fatal] Failed to read, and not due to blocking\n");
                    do_connection_status(conn, CLOSE_STATE);
                    break;
                }
            } else if (retval == 0) {
                do_connection_status(conn, CLOSE_STATE);
                break;
            }
            
            conn->cIgnoreBytes -= retval;
            conn->lastTransportTime = time(NULL);
            break;
        }
        case CRLF_STATE: {
            char crlf_buf[2];
            
            retval = read(conn->fd, crlf_buf, 2 - conn->crlf_bytes);
            if (retval == -1) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    exit = 1;
                    break;
                } else {
                    fprintf(stderr, "[Fatal] Failed to read, and not due to blocking\n");
                    do_connection_status(conn, CLOSE_STATE);
                    break;
                }
            } else if (retval == 0) {/* This client connection was closed */
                do_connection_status(conn, CLOSE_STATE);
                break;
            }
            
            conn->crlf_bytes += retval;
            conn->lastTransportTime = time(NULL);
            
            if (conn->crlf_bytes >= 2) {
                conn->crlf_bytes = 0;
                doSendReply(conn, "+FIN: OK", 1);
                break;
            }
        }
        case SEND_STATE: {
            if (conn->cSendBytes <= 0) {/* Each status would do it */
                aeDeleteFileEvent(server.eventLoop, conn->fd, AE_WRITABLE);
                do_connection_status(conn, RECV_STATE);/* back to recv state after send state */
                do_connection_send_empty(conn);
                do_connection_recv_empty(conn);
                do_connection_nrecvbuf_empty(conn);
                do_connection_nsendbuf_empty(conn);
                break;
            }
            
            retval = write(conn->fd, conn->cSendBuffer + conn->cSendWhere, conn->cSendBytes - conn->cSendWhere);
            if (retval == -1) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    exit = 1;
                    break;
                } else {
                    fprintf(stderr, "[Fatal] Failed to read, and not due to blocking\n");
                    do_connection_status(conn, CLOSE_STATE);
                    break;
                }
            } else if (retval == 0) {
                do_connection_status(conn, CLOSE_STATE);
                break;
            }
            
            conn->cSendWhere += retval;
            conn->cSendBytes -= retval;
            conn->lastTransportTime = time(NULL);
            break;
        }
        case MSEND_STATE: {
            void *item;
            
            if (conn->cSendBytes > 0) {
                retval = write(conn->fd, conn->mSendBuffer + conn->cSendWhere, conn->cSendBytes - conn->cSendWhere);
                if (retval == -1) {
                    if (errno == EAGAIN || errno == EWOULDBLOCK) {
                        exit = 1;
                        break;
                    } else {
                        fprintf(stderr, "[Fatal] Failed to read, and not due to blocking\n");
                        do_connection_status(conn, CLOSE_STATE);
                        break;
                    }
                } else if (retval == 0) {
                    do_connection_status(conn, CLOSE_STATE);
                    break;
                }
                
                conn->cSendWhere += retval;
                conn->cSendBytes -= retval;
                conn->lastTransportTime = time(NULL);
            }
            else
            {
                switch (conn->replyStage) {
                case 1:/* Get item data */
                    item = list_fetch_head(conn->replyList);
                    conn->mSendBuffer = (char *)((smq_block_t *)item)->block;
                    conn->cSendBytes = ((smq_block_t *)item)->blockSize + 2;/* require "\r\n" */
                    conn->cSendWhere = 0;
                    conn->replyStage = 2;
                    break;
                case 2:/* Remove item */
                    retval = list_remove_head(conn->replyList, &item);
                    doItemDecrRefcount(item);/* reback to slabs */
                    conn->mSendBuffer = NULL;
                    conn->cSendBytes = 0;
                    conn->cSendWhere = 0;
                    if (list_empty(conn->replyList)) {
                        conn->replyStage = 3;
                        break;
                    }
                case 0:/* Get item size */
                    item = list_fetch_head(conn->replyList);
                    if (!item) {
                        fprintf(stderr, "[Fatal] Reply list is empty, connection would be closed.\n");
                        do_connection_status(conn, CLOSE_STATE);
                        continue;
                    }
                    sprintf(conn->cSendBuffer, "$%d\r\n", ((smq_block_t *)item)->blockSize);
                    conn->mSendBuffer = conn->cSendBuffer;
                    conn->cSendBytes = strlen(conn->cSendBuffer);
                    conn->cSendWhere = 0;
                    conn->replyStage = 1;
                    break;
                case 3:/* Finish reply items */
                    conn->replyStage  = 0;
                    conn->mSendBuffer = NULL;
                    doSendReply(conn, "+END", 0);/* we don't create file event in this case */
                    break;
                }
            }
            break;
        }
        case CLOSE_STATE:
            clientCloseRemove(conn);
            exit = 1;
            break;
        default:
            smqLog(LOG_ERROR, "Oops, What is the state call [%d]?", conn->state);
            do_connection_status(conn, CLOSE_STATE);/* Undefine state, we close this connection. */
            break;
        }
    }
}

/* Disk store functions */
int diskStoreAction() {
    FILE *fp;
    char tmpfile[1024];
    smq_block_t *item;
    int eof_flag = 0;
    
    sprintf(tmpfile, "squirrel-tmp-%d.db", (int)getpid());
    fp = fopen(tmpfile, "wb");
    if (!fp) {
        smqLog(LOG_ERROR, "Failed saving the queue, %s", strerror(errno));
        return -1;
    }
    
    if (fwrite(RDB_HEADER_BLOCK, strlen(RDB_HEADER_BLOCK), 1, fp) == 0) goto ERROR_FLAG;
    while (list_remove_head(server.queue, (void **)&item) != -1) {
        if (fwrite(&(item->blockSize), sizeof(int), 1, fp) == 0) goto ERROR_FLAG;
        if (fwrite(item->block, item->blockSize, 1, fp) == 0) goto ERROR_FLAG;
    }
    if (fwrite(&eof_flag, sizeof(int), 1, fp) == 0) goto ERROR_FLAG;/* EOF flag */
    
    fflush(fp);
    fsync(fileno(fp));
    fclose(fp);
    
    if (access("squirrel.db", R_OK) == 0)
        unlink("squirrel.db");/* delete squirrel.rdb */
    if (rename(tmpfile, "squirrel.db") == -1) {
        smqLog(LOG_ERROR, "Error moving temp DB file on the final destination, %s", strerror(errno));
        unlink(tmpfile);
        return -1;
    }
    return 0;

ERROR_FLAG:
    fclose(fp);
    unlink(tmpfile);
    smqLog(LOG_ERROR, "Write error saving queue on disk: %s", strerror(errno));
    return -1;
}

int diskStoreBackground() {
    pid_t pid;
    
    if (server.bgSaveChildpid != -1 || !server.dirty)
        return -1;
    pid = fork();
    if (pid == 0) {/* child proccess */
        if (diskStoreAction() == -1) {
            exit(-1);
        } else {
            exit(0);
        }
    } else {
        if (pid == -1) {
            smqLog(LOG_ERROR, "Unable fork child proccess");
            return -1;
        }
        server.bgSaveChildpid = pid;
        server.dirty = 0;
        return 0;
    }
    return 0;
}

int tryLoadData() {
    FILE *fp;
    char rdbHeaderBlock[32];
    int blockSize;
    smq_block_t *item;
    int nodes = 0;
    
    if (access("squirrel.db", R_OK) != 0)
        return 0;
    fp = fopen("squirrel.db", "rb");
    if (!fp) {
        smqLog(LOG_ERROR, "Unable open date file to load");
        return -1;
    }
    
    if (fread(rdbHeaderBlock, strlen(RDB_HEADER_BLOCK), 1, fp) == 0) {
        fprintf(stderr, "Unable read data from data file.\n");
        goto ERROR_FLAG;
    }
    if (strncmp(rdbHeaderBlock, RDB_HEADER_BLOCK, strlen(RDB_HEADER_BLOCK))) {
        fprintf(stderr, "The data file header error.\n");
        goto ERROR_FLAG;
    }
    
    while (1) {
        if (fread(&blockSize, sizeof(int), 1, fp) == 0)
            goto ERROR_FLAG;
        if (blockSize == 0)
            break;
        item = doItemAlloc(blockSize);
        if (!item) {
            smqLog(LOG_ERROR, "Unable alloc item");
            goto ERROR_FLAG;
        }
        if (fread(item->block, blockSize, 1, fp) == 0)
            goto ERROR_FLAG;
        if (list_add_head(server.queue, item) == -1) {
            smqLog(LOG_ERROR, "Unable add item into the queue");
            goto ERROR_FLAG;
        }
        nodes++;
    }
    smqLog(LOG_DEBUG, "Load %d items from database file", nodes);
    
    fclose(fp);
    return 0;

ERROR_FLAG:
    fclose(fp);
    return -1;
}
/* End of disk store functions */

int serverCron(struct aeEventLoop *eventLoop, long long id, void *clientData) {
    int loops = server.cronLoops++;
    smq_client_t *conn, *next;
    time_t now;
    
    /* wait for child proccess to disk store */
    if (server.bgSaveChildpid != -1) {
        int statloc;
        pid_t pid;
        
        if ((pid = wait3(&statloc, WNOHANG, NULL)) != 0) {/* noblock wait */
            int exitcode = WEXITSTATUS(statloc);
            int bysignal = WIFSIGNALED(statloc);
            char tmpfile[256];
        
            if (!bysignal && exitcode == 0) {
                smqLog(LOG_DEBUG, "Background saving terminated with success");
                server.lastSaveTime = time(NULL);
            } else if (!bysignal && exitcode != 0) {
                smqLog(LOG_NOTICE, "Background saving error");
            } else {
                smqLog(LOG_NOTICE, "Background saving terminated by signal");
                sprintf(tmpfile, "squirrel-tmp-%d.rdb", server.bgSaveChildpid);
                unlink(tmpfile);
            }
            server.bgSaveChildpid = -1;
        }
    } else {
        now = time(NULL);
        if (now - server.lastSaveTime > server.secondsToSaveDisk &&
            server.dirty >= server.chagesToSaveDisk) {
            diskStoreBackground();
        }
    }
    
    smqLog(LOG_DEBUG, "The queue size: %d, total memory used: %dKB, connections: %d",
            list_size(server.queue), (server.memoryUsedTotal / 1024) + 1, server.connections);
    
    if (!(loops % 30)) {
        int howmanyZombie = 0;
        now = time(NULL);
        conn = server.clientListHead;
        while (conn) {/* foreach connections list */
            next = conn->next;
            if ((int)now - conn->lastTransportTime > server.clientExpiredTime) {
                clientCloseRemove(conn);
                howmanyZombie++;
            }
            conn = next;
        }
        howmanyZombie > 0 ? smqLog(LOG_DEBUG, "%ds zombie client connections were clean", howmanyZombie) : 0;
    }
    
    return server.cronLoops; /* How many seconds would cron loop */
}

void daemonize(void) {
    int fd;
    FILE *fp;

    if (fork() != 0) exit(0);
    
    setsid();

    if ((fd = open("/dev/null", O_RDWR, 0)) != -1)
    {
        dup2(fd, STDIN_FILENO);
        dup2(fd, STDOUT_FILENO);
        dup2(fd, STDERR_FILENO);
        if (fd > STDERR_FILENO) close(fd);
    }
}

void version() {
    printf("SquirrelMQ server version: %s\n", SQUIRREL_VERSION);
    printf("Please visit http://code.google.com/p/squirrel-message-queue/\n\n");
}

void usage(void) {
    printf("+--------------------------------------------------------+\n");
    printf("|             The Squirrel message queue                 |\n");
    printf("+--------------------------------------------------------+\n");
    printf("| Liexusong(c)2010-2011 Contact by liexusong@qq.com      |\n");
    printf("| Visit http://code.google.com/p/squirrel-message-queue/ |\n");
    printf("+--------------------------------------------------------+\n");
    printf("| -c <config>   load configure from the configure file   |\n");
    printf("| -v            print this current version and exit      |\n");
    printf("| -h            print this help and exit                 |\n");
    printf("+--------------------------------------------------------+\n\n");
    return;
}

/*===================== Lua functions and proccess ========================*/
static int smq_item_pop_head(lua_State *L) {
    smq_block_t *item;
    int retval;

    thread_lock_list();
    while (list_empty(server.queue))
        pthread_cond_wait(&listGlobalCondition, &listGlobalLock);
    retval = list_remove_head(server.queue, (void **)&item);
    server.dirty++;
    thread_unlock_list();
    
    if (retval == -1) {
        lua_pushboolean(L, 0);
        lua_pushnil(L);
    } else {
        lua_pushboolean(L, 1);
        lua_pushlstring(L, item->block, item->blockSize);
        doItemDecrRefcount(item);
    }
    return 2;
}

static int smq_item_pop_tail(lua_State *L) {
    smq_block_t *item;
    int retval;

    thread_lock_list();
    while (list_empty(server.queue))
        pthread_cond_wait(&listGlobalCondition, &listGlobalLock);
    retval = list_remove_tail(server.queue, (void **)&item);
    server.dirty++;
    thread_unlock_list();
    
    if (retval == -1) {
        lua_pushboolean(L, 0);
        lua_pushnil(L);
    } else {
        lua_pushboolean(L, 1);
        lua_pushlstring(L, item->block, item->blockSize);
        doItemDecrRefcount(item);
    }
    return 2;
}

static int smq_item_pop_index(lua_State *L) {
    smq_block_t *item;
    int retval;
    int index;

    index = luaL_checknumber(L, 1);
    thread_lock_list();
    while (list_empty(server.queue))
        pthread_cond_wait(&listGlobalCondition, &listGlobalLock);
    retval = list_remove_index(server.queue, index, (void **)&item);
    server.dirty++;
    thread_unlock_list();
    
    if (retval == -1) {
        lua_pushboolean(L, 0);
        lua_pushnil(L);
    } else {
        lua_pushboolean(L, 1);
        lua_pushlstring(L, item->block, item->blockSize);
        doItemDecrRefcount(item);
    }
    return 2;
}

static int smq_item_get_head(lua_State *L) {
    smq_block_t *item;

    thread_lock_list();
    while (list_empty(server.queue))
        pthread_cond_wait(&listGlobalCondition, &listGlobalLock);
    item = list_fetch_head(server.queue);
    thread_unlock_list();
    
    if (!item) {
        lua_pushboolean(L, 0);
        lua_pushnil(L);
    } else {
        lua_pushboolean(L, 1);
        lua_pushlstring(L, item->block, item->blockSize);
    }
    return 2;
}

static int smq_item_get_tail(lua_State *L) {
    smq_block_t *item;
    int retval;

    thread_lock_list();
    while (list_empty(server.queue))
        pthread_cond_wait(&listGlobalCondition, &listGlobalLock);
    item = list_fetch_tail(server.queue);
    thread_unlock_list();
    
    if (!item) {
        lua_pushboolean(L, 0);
        lua_pushnil(L);
    } else {
        lua_pushboolean(L, 1);
        lua_pushlstring(L, item->block, item->blockSize);
    }
    return 2;
}

static int smq_item_get_index(lua_State *L) {
    smq_block_t *item;
    int index;

    index = luaL_checknumber(L, 1);
    thread_lock_list();
    while (list_empty(server.queue))
        pthread_cond_wait(&listGlobalCondition, &listGlobalLock);
    item = list_fetch_index(server.queue, index);
    thread_unlock_list();
    
    if (!item) {
        lua_pushboolean(L, 0);
        lua_pushnil(L);
    } else {
        lua_pushboolean(L, 1);
        lua_pushlstring(L, item->block, item->blockSize);
    }
    return 2;
}

static int smq_queue_size(lua_State *L) {
    int size;
    thread_lock_list();
    size = list_size(server.queue);
    thread_unlock_list();
    lua_pushnumber(L, size);
    return 1;
}

static int smq_item_push_head(lua_State *L) {
    smq_block_t *item;
    const char *value = luaL_checkstring(L, 1);
    
    item = doItemAllocString(value);
    if (item) {
        thread_lock_list();
        list_add_head(server.queue, item);
        server.dirty++;
        thread_unlock_list();
        lua_pushboolean(L, 1);
    } else {
        lua_pushboolean(L, 0);
    }
    return 1;
}

static int smq_item_push_tail(lua_State *L) {
    smq_block_t *item;
    const char *value = luaL_checkstring(L, 1);
    
    item = doItemAllocString(value);
    if (item) {
        thread_lock_list();
        list_add_tail(server.queue, item);
        server.dirty++;
        thread_unlock_list();
        lua_pushboolean(L, 1);
    } else {
        lua_pushboolean(L, 0);
    }
    return 1;
}

static int smq_sleep(lua_State *L) {
    int sleep_sec, sleep_usec;
    struct timeval timeout;
    
    sleep_sec = luaL_checknumber(L, 1);
    sleep_usec = luaL_checknumber(L, 2);
    timeout.tv_sec = sleep_sec;
    timeout.tv_usec = sleep_usec;
    select(0, NULL, NULL, NULL, &timeout);
    return 0;
}

void *luaThreadFunction(void *arg) {
    smq_block_t *item;
    int retval;
    lua_State *L = luaL_newstate();/* open lua vm */
    luaL_openlibs(L);
    
    if (luaL_loadfile(L, server.luaFilePath) ||
        lua_pcall(L, 0, 0, 0)) {
        fprintf(stderr, "%s\n", lua_tostring(L, -1));
        exit(-1);
    }
    
    /* Load c functions for lua script */
    lua_pushcfunction(L, smq_item_pop_head);
    lua_setglobal(L, "item_pop_head");
    lua_pushcfunction(L, smq_item_pop_tail);
    lua_setglobal(L, "item_pop_tail");
    lua_pushcfunction(L, smq_item_pop_index);
    lua_setglobal(L, "item_pop_index");
    lua_pushcfunction(L, smq_item_get_head);
    lua_setglobal(L, "item_get_head");
    lua_pushcfunction(L, smq_item_get_tail);
    lua_setglobal(L, "item_get_tail");
    lua_pushcfunction(L, smq_item_get_index);
    lua_setglobal(L, "item_get_index");
    lua_pushcfunction(L, smq_item_push_head);
    lua_setglobal(L, "item_push_head");
    lua_pushcfunction(L, smq_item_push_tail);
    lua_setglobal(L, "item_push_tail");
    lua_pushcfunction(L, smq_queue_size);
    lua_setglobal(L, "queue_size");
    lua_pushcfunction(L, smq_sleep);
    lua_setglobal(L, "sleep");
    
    lua_getglobal(L, server.luaMainFunction);/* Get the main function */
    lua_pcall(L, 0, 0, 0);/* Call the main function */
    lua_close(L);
    pthread_exit(0);
}

int pidFileCreate() {
    char buff[16];
    FILE *handle;
    
    if (server.pidFilePath == NULL)
        return 0;
    sprintf(buff, "%d", getpid());
    handle = fopen(server.pidFilePath, "w");
    if (!handle) {
        fprintf(stderr, "[Fatal] Unable create pid file.\n");
        return -1;
    }
    fwrite(buff, strlen(buff), 1, handle);
    fclose(handle);
    return 0;
}

int fileExists(const char *filename) {
    struct stat buf;
    if (stat(filename, &buf) == ENOENT)
        return -1;
    return 0;
}

int main(int argc, char **argv) {
    int c;
    struct rlimit rlim;
    struct rlimit rlim_new;
    struct sigaction sa;
    pthread_t luaThreadId, acceptThreadId;
    char defalutConfigPath[1024] = "squirrel.conf";
    
    while ((c = getopt(argc, argv, "c:vh")) != -1)
    {
        switch (c)
        {
        case 'c':
            sprintf(defalutConfigPath, "%s", optarg);
            break;
        case 'v':
            version();
            exit(0);
        case 'h':
            usage();
            exit(0);
        default:
            fprintf(stderr, "[Fatal] Illegal argument `%c'\n", c);
            exit(-1);
        }
    }

    /* set max resource */
    if (getrlimit(RLIMIT_CORE, &rlim)==0)
    {
        rlim_new.rlim_cur = rlim_new.rlim_max = RLIM_INFINITY;
        if (setrlimit(RLIMIT_CORE, &rlim_new)!=0)
        {
            rlim_new.rlim_cur = rlim_new.rlim_max = rlim.rlim_max;
            (void) setrlimit(RLIMIT_CORE, &rlim_new);
        }
    }
       
    if ((getrlimit(RLIMIT_CORE, &rlim)!=0) || rlim.rlim_cur==0)
    {
        smqLog(LOG_ERROR, "Unable ensure corefile creation");
        exit(-1);
    }
    
    if (getrlimit(RLIMIT_NOFILE, &rlim) != 0)
    {
        smqLog(LOG_ERROR, "Unable getrlimit number of files");
        exit(-1);
    }
    else
    {
        int maxfiles = 1024;
        if (rlim.rlim_cur < maxfiles)
            rlim.rlim_cur = maxfiles + 3;
        if (rlim.rlim_max < rlim.rlim_cur)
            rlim.rlim_max = rlim.rlim_cur;
        if (setrlimit(RLIMIT_NOFILE, &rlim) != 0) {
            smqLog(LOG_ERROR, "Unable set rlimit for open files. Try running as root or requesting smaller maxconns value");
            exit(-1);
        }
    }
    
    /* ignore SIGPIPE signal */
    sa.sa_handler = SIG_IGN;
    sa.sa_flags = 0;
    if (sigemptyset(&sa.sa_mask) == -1 || sigaction(SIGPIPE, &sa, 0) == -1) {
        fprintf(stderr, "[Fatal] Unable ignore SIGPIPE.\n");
        exit(-1);
    }
    
    memset(&server, 0, sizeof(server));
    loadConfigFile(defalutConfigPath);
    loadConfig();
    initServer();/* init server */
    if (server.daemonize)
        daemonize();
    if (fileExists(server.pidFilePath)) {
        fprintf(stderr, "[Fatal] Pid file was exists, you must remove it first.\n");
        exit(-1);
    }
    if (pidFileCreate() != 0)
        exit(-1);
    slabs_init(server.memoryLimitUsed);/* init slabs lib */
    if (tryLoadData() == -1) { /* load data */
        fprintf(stderr, "[Fatal] Unable load the data from rdb file.\n");
        exit(-1);
    }
    
    if (pthread_create(&acceptThreadId, NULL, acceptThreadFunction, NULL) != 0) {
        fprintf(stderr, "[Fatal] Unable create accept thread\n");
        exit(-1);
    }
    
    if (pthread_create(&luaThreadId, NULL, luaThreadFunction, NULL) != 0) {
        fprintf(stderr, "[Fatal] Unable create lua cron thread\n");
        exit(-1);
    }
    
    aeMain(server.eventLoop);/* do event loop */
    
    if (server.pidFilePath != NULL)
        unlink(server.pidFilePath);
    exit(0);
}

int putf_command(smq_client_t *conn, token_t *tokens, int tokencount) {
    int bytes;
    int movbytes;
    int cmdlen;
    int retval;
    
    bytes = atoi(tokens[1].token);
    if (bytes > 0)
    {
        smq_block_t *item = doItemAlloc(bytes);
        cmdlen = strlen(conn->cRecvBuffer);
        
        movbytes = conn->cRecvWhere - cmdlen - 2;
        if (!item) {
            if (movbytes > 0) {
                int wouldmove = (movbytes >= bytes ? bytes : movbytes);
                bytes -= wouldmove;
                if (movbytes - wouldmove >= 2) {/* Finish receive data */
                    conn->crlf_bytes = 0;
                    doSendReply(conn, "+FIN: OK", 1);
                    return 0;
                }
            }
            conn->cIgnoreBytes = bytes;
            do_connection_status(conn, IGNORE_STATE);
            return -1;
        }
        
        conn->cRecvBytes = bytes;
        conn->cRecvWhere = 0;
        do_connection_status(conn, NRECV_STATE);
        
        if (movbytes > 0) {
            int wouldmove = (movbytes >= conn->cRecvBytes ? conn->cRecvBytes : movbytes);
            memcpy(item->block, conn->cRecvBuffer + cmdlen + 2, wouldmove);
            conn->cRecvBytes -= wouldmove;
            conn->cRecvWhere  = wouldmove;
            conn->mRecvBuffer = item->block;
            if (movbytes - wouldmove >= 2) {/* Finish receive data */
                conn->crlf_bytes = 0;
                doSendReply(conn, "+FIN: OK", 1);
            }
        }
        
        thread_lock_list();
        retval = list_add_head(server.queue, item);
        pthread_cond_signal(&listGlobalCondition);
        thread_unlock_list();
        
        if (retval == -1) {
            doSendReply(conn, "#ERR: Unable entry item into the message queue", 1);
            doItemDecrRefcount(item);
            return -1;
        }
        server.dirty++;
    } else {
        fprintf(stderr, "[Fatal] The data received is too short.\n");
        do_connection_status(conn, CLOSE_STATE);
        return -1;
    }
    return 0;
}

int putl_command(smq_client_t *conn, token_t *tokens, int tokencount) {
    int bytes;
    int movbytes;
    int cmdlen;
    int retval;
    
    bytes = atoi(tokens[1].token);
    if (bytes > 0)
    {
        smq_block_t *item = doItemAlloc(bytes);
        cmdlen = strlen(conn->cRecvBuffer);
        
        movbytes = conn->cRecvWhere - cmdlen - 2;
        if (!item) {
            if (movbytes > 0) {
                int wouldmove = (movbytes >= bytes ? bytes : movbytes);
                bytes -= wouldmove;
                if (movbytes - wouldmove >= 2) {/* Finish receive data */
                    conn->crlf_bytes = 0;
                    doSendReply(conn, "+FIN: OK", 1);
                    return 0;
                }
            }
            conn->cIgnoreBytes = bytes;
            do_connection_status(conn, IGNORE_STATE);
            return -1;
        }
        
        conn->cRecvBytes = bytes;
        conn->cRecvWhere = 0;
        do_connection_status(conn, NRECV_STATE);
        
        if (movbytes > 0) {
            int wouldmove = (movbytes >= conn->cRecvBytes ? conn->cRecvBytes : movbytes);
            memcpy(item->block, conn->cRecvBuffer + cmdlen + 2, wouldmove);
            conn->cRecvBytes -= wouldmove;
            conn->cRecvWhere  = wouldmove;
            conn->mRecvBuffer = item->block;
            if (movbytes - wouldmove >= 2) {/* Finish receive data */
                conn->crlf_bytes = 0;
                doSendReply(conn, "+FIN: OK", 1);
            }
        }
        
        thread_lock_list();
        retval = list_add_tail(server.queue, item);
        pthread_cond_signal(&listGlobalCondition);
        thread_unlock_list();
        
        if (retval == -1) {
            doSendReply(conn, "#ERR: Unable entry item into the message queue", 1);
            doItemDecrRefcount(item);
            return -1;
        }
        server.dirty++;
    } else {
        fprintf(stderr, "[Fatal] The data received is too short.\n");
        do_connection_status(conn, CLOSE_STATE);
        return -1;
    }
    return 0;
}

int popf_command(smq_client_t *conn, token_t *tokens, int tokencount) {
    void *item;
    int retval;
    
    if (list_empty(server.queue)) {
        doSendReply(conn, "#ERR: The message queue is empty", 1);
        return -1;
    }
    
    thread_lock_list();
    retval = list_remove_head(server.queue, &item);
    thread_unlock_list();
    
    if (retval == -1) {
        doSendReply(conn, "#ERR: Unable fetch item from the message queue", 1);
        return -1;
    } else {
        if (list_add_tail(conn->replyList, item) == -1) {
            doSendReply(conn, "#ERR: Unable entry item into reply list", 1);
            return -1;
        }
        list_add_tail(conn->replyList, makeNLObject());
        do_connection_status(conn, MSEND_STATE);
        do_connection_nsendbuf_empty(conn);
        do_connection_send_empty(conn);
        conn->replyStage = 0;
        aeCreateFileEvent(server.eventLoop, conn->fd, AE_WRITABLE, eventActionFunction, conn);
        server.dirty++;
        return 0;
    }
    return 0;
}

int popl_command(smq_client_t *conn, token_t *tokens, int tokencount) {
    void *item;
    int retval;
    
    if (list_empty(server.queue)) {
        doSendReply(conn, "#ERR: The message queue is empty", 1);
        return -1;
    }
    
    thread_lock_list();
    retval = list_remove_tail(server.queue, &item);
    thread_unlock_list();
    
    if (retval == -1) {
        doSendReply(conn, "#ERR: Unable fetch item from the message queue", 1);
        return -1;
    } else {
        if (list_add_tail(conn->replyList, item) == -1) {
            doSendReply(conn, "#ERR: Unable entry item into reply list", 1);
            return -1;
        }
        list_add_tail(conn->replyList, makeNLObject());
        do_connection_status(conn, MSEND_STATE);
        do_connection_nsendbuf_empty(conn);
        do_connection_send_empty(conn);
        conn->replyStage = 0;
        aeCreateFileEvent(server.eventLoop, conn->fd, AE_WRITABLE, eventActionFunction, conn);
        server.dirty++;
        return 0;
    }
    return 0;
}

int popp_command(smq_client_t *conn, token_t *tokens, int tokencount) {
    void *item;
    int retval;
    int position;
    
    if (list_empty(server.queue)) {
        doSendReply(conn, "#ERR: The message queue is empty", 1);
        return -1;
    }
    
    position = atoi(tokens[1].token);
    
    thread_lock_list();
    retval = list_remove_index(server.queue, position, &item);
    thread_unlock_list();
    
    if (retval == -1) {
        doSendReply(conn, "#ERR: Unable fetch item from the message queue", 1);
        return -1;
    } else {
        if (list_add_tail(conn->replyList, item) == -1) {
            doSendReply(conn, "#ERR: Unable entry item into reply list", 1);
            return -1;
        }
        list_add_tail(conn->replyList, makeNLObject());
        do_connection_status(conn, MSEND_STATE);
        do_connection_nsendbuf_empty(conn);
        do_connection_send_empty(conn);
        conn->replyStage = 0;
        aeCreateFileEvent(server.eventLoop, conn->fd, AE_WRITABLE, eventActionFunction, conn);
        server.dirty++;
        return 0;
    }
    return 0;
}

int pops_command(smq_client_t *conn, token_t *tokens, int tokencount) {
    void *item;
    int retval;
    int nums, i;
    
    if (list_empty(server.queue)) {
        doSendReply(conn, "#ERR: The message queue is empty", 1);
        return -1;
    }
    
    nums = atoi(tokens[1].token);
    
    thread_lock_list();
    for (i = 0; i < nums; i++) {
        item = list_fetch_index(server.queue, i);
        if (!item)
            break;
        doItemIncrRefcount(item);
        if (list_add_tail(conn->replyList, item) == -1) {
            doItemIncrRefcount(item);
            break;
        }
        list_add_tail(conn->replyList, makeNLObject());
    }
    thread_unlock_list();
    
    do_connection_status(conn, MSEND_STATE);
    do_connection_nsendbuf_empty(conn);
    do_connection_send_empty(conn);
    conn->replyStage = 0;
    
    aeCreateFileEvent(server.eventLoop, conn->fd, AE_WRITABLE, eventActionFunction, conn);
    return 0;
}

int getf_command(smq_client_t *conn, token_t *tokens, int tokencount) {
    void *item;
    
    if (list_empty(server.queue)) {
        doSendReply(conn, "#ERR: The message queue is empty", 1);
        return -1;
    }
    
    thread_lock_list();
    item = list_fetch_head(server.queue);
    thread_unlock_list();
    
    if (!item) {
        doSendReply(conn, "#ERR: Unable fetch item from the message queue", 1);
        return -1;
    } else {
        doItemIncrRefcount(item);
        if (list_add_tail(conn->replyList, item) == -1) {
            doItemDecrRefcount(item);
            doSendReply(conn, "#ERR: Unable entry item into reply list", 1);
            return -1;
        }
        list_add_tail(conn->replyList, makeNLObject());
        do_connection_status(conn, MSEND_STATE);
        do_connection_nsendbuf_empty(conn);
        do_connection_send_empty(conn);
        conn->replyStage = 0;
        aeCreateFileEvent(server.eventLoop, conn->fd, AE_WRITABLE, eventActionFunction, conn);
        return 0;
    }
    return 0;
}

int getl_command(smq_client_t *conn, token_t *tokens, int tokencount) {
    void *item;
    
    if (list_empty(server.queue)) {
        doSendReply(conn, "#ERR: The message queue is empty", 1);
        return -1;
    }
    
    thread_lock_list();
    item = list_fetch_tail(server.queue);
    thread_unlock_list();
    
    if (!item) {
        doSendReply(conn, "#ERR: Unable fetch item from the message queue", 1);
        return -1;
    } else {
        doItemIncrRefcount(item);
        if (list_add_tail(conn->replyList, item) == -1) {
            doItemDecrRefcount(item);
            doSendReply(conn, "#ERR: Unable entry item into reply list", 1);
            return -1;
        }
        list_add_tail(conn->replyList, makeNLObject());
        do_connection_status(conn, MSEND_STATE);
        do_connection_nsendbuf_empty(conn);
        do_connection_send_empty(conn);
        conn->replyStage = 0;
        aeCreateFileEvent(server.eventLoop, conn->fd, AE_WRITABLE, eventActionFunction, conn);
        return 0;
    }
    return 0;
}

int getp_command(smq_client_t *conn, token_t *tokens, int tokencount) {
    void *item;
    int position;
    
    if (list_empty(server.queue)) {
        doSendReply(conn, "#ERR: The message queue is empty", 1);
        return -1;
    }
    
    position = atoi(tokens[1].token);
    
    thread_lock_list();
    item = list_fetch_index(server.queue, position);
    thread_unlock_list();
    
    if (!item) {
        doSendReply(conn, "#ERR: Unable fetch item from the message queue", 1);
        return -1;
    } else {
        doItemIncrRefcount(item);
        if (list_add_tail(conn->replyList, item) == -1) {
            doItemDecrRefcount(item);
            doSendReply(conn, "#ERR: Unable entry item into reply list", 1);
            return -1;
        }
        list_add_tail(conn->replyList, makeNLObject());
        do_connection_status(conn, MSEND_STATE);
        do_connection_nsendbuf_empty(conn);
        do_connection_send_empty(conn);
        conn->replyStage = 0;
        aeCreateFileEvent(server.eventLoop, conn->fd, AE_WRITABLE, eventActionFunction, conn);
        return 0;
    }
    return 0;
}

int gets_command(smq_client_t *conn, token_t *tokens, int tokencount) {
    void *item;
    int nums, i;
    
    if (list_empty(server.queue)) {
        doSendReply(conn, "#ERR: The message queue is empty", 1);
        return -1;
    }
    
    nums = atoi(tokens[1].token);
    
    thread_lock_list();
    for (i = 0; i < nums; i++) {
        item = list_fetch_index(server.queue, i);
        if (!item)
            break;
        doItemIncrRefcount(item);
        if (list_add_tail(conn->replyList, item) == -1) {
            doItemIncrRefcount(item);
            break;
        }
        list_add_tail(conn->replyList, makeNLObject());
    }
    thread_unlock_list();
    
    do_connection_status(conn, MSEND_STATE);
    do_connection_nsendbuf_empty(conn);
    do_connection_send_empty(conn);
    conn->replyStage = 0;
    
    aeCreateFileEvent(server.eventLoop, conn->fd, AE_WRITABLE, eventActionFunction, conn);
    return 0;
}

int stat_command(smq_client_t *conn, token_t *tokens, int tokencount) {
    smq_block_t *item;
    char buffer[1024];
    int offset;
    int listSize;
    
    thread_lock_list();
    listSize = list_size(server.queue);
    thread_unlock_list();
    
    offset = sprintf(buffer, "[*] queue size: %d items\r\n", listSize);
    offset += sprintf(buffer + offset, "[*] memory used: %d bytes", server.memoryUsedTotal);
    item = doItemAlloc(offset);
    if (!item) {
        doSendReply(conn, "#ERR: Not enough memory alloc item", 1);
        return -1;
    }
    memcpy(item->block, buffer, offset);
    if (list_add_tail(conn->replyList, (void *)item) == -1) {
        doSendReply(conn, "#ERR: Unable entry item into reply list", 1);
        return -1;
    }
    list_add_tail(conn->replyList, makeNLObject());
    do_connection_status(conn, MSEND_STATE);
    do_connection_nsendbuf_empty(conn);
    do_connection_send_empty(conn);
    conn->replyStage = 0;
    return 0;
}

int slab_command(smq_client_t *conn, token_t *tokens, int tokencount) {
    smq_block_t *item;
    int infoLen;
    char *info;
    
    info = slabs_stats(&infoLen);
    if (info) {
        item = doItemAlloc(infoLen);
        if (!item) {
            doSendReply(conn, "#ERR: Not enough memory alloc item", 1);
            free(info);
            return -1;
        }
        memcpy(item->block, info, infoLen);
        if (list_add_tail(conn->replyList, (void *)item) == -1) {
            doSendReply(conn, "#ERR: Unable entry item into reply list", 1);
            return -1;
        }
        list_add_tail(conn->replyList, makeNLObject());
        do_connection_status(conn, MSEND_STATE);
        do_connection_nsendbuf_empty(conn);
        do_connection_send_empty(conn);
        conn->replyStage = 0;
        free(info);
        return 0;
    } else {
        doSendReply(conn, "#ERR: Not enough memory alloc item", 1);
        return -1;
    }
}

int size_command(smq_client_t *conn, token_t *tokens, int tokencount) {
    char buffer[256];
    smq_block_t *item;
    int slen;
    int listSize;
    
    thread_lock_list();
    listSize = list_size(server.queue);
    thread_unlock_list();
    
    slen = sprintf(buffer, "SIZE(%d)", listSize);
    item = doItemAlloc(slen);
    if (!item) {
        doSendReply(conn, "#ERR: Not enough memory alloc item", 1);
        return -1;
    }
    
    memcpy(item->block, buffer, slen);
    if (list_add_tail(conn->replyList, (void *)item) == -1) {
        doSendReply(conn, "#ERR: Unable entry item into reply list", 1);
        return -1;
    }
    list_add_tail(conn->replyList, makeNLObject());
    do_connection_status(conn, MSEND_STATE);
    do_connection_nsendbuf_empty(conn);
    do_connection_send_empty(conn);
    conn->replyStage = 0;
    return 0;
}

int close_command(smq_client_t *conn, token_t *tokens, int tokencount) {
    do_connection_status(conn, CLOSE_STATE);/* close this connection */
    return 0;
}

