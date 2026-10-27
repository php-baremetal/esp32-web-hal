/*
   +----------------------------------------------------------------------+
   | php-baremetal: esp32-web-hal                                         |
   +----------------------------------------------------------------------+
   | Copyright (c) 2026 Gianfrancesco Aurecchia                           |
   +----------------------------------------------------------------------+
   | This source file is subject to the MIT license, that is bundled with |
   | this package in the file LICENSE.                                    |
   +----------------------------------------------------------------------+
   | Authors: Gianfrancesco Aurecchia <gianfri@php-baremetal.com>         |
   +----------------------------------------------------------------------+
*/

/*
 * ESP32 implementation of the web HAL (bm_web_hal.h) over esp_http_server. Holds the one HTTP server,
 * the httpd-task request/WebSocket handlers, and the request/response rendezvous that bridges the httpd
 * task to the reactor. The neutral facade (php_web.c) is unaware of all of it: it only fills a
 * web_slot_t's response fields and calls bm_web_hal_response_ready().
 *
 * The httpd task runs on core 0 with its stack in internal RAM and never touches PHP: it only parses,
 * posts a slot to the PHP task (core 1) and waits for the reply, at most the configured time. Each request
 * has its own slot; its state (parked, done, abandoned) decides who frees it. WebSocket sends run on the
 * httpd task too (httpd_queue_work), where connections open and close, so a frame never reaches a socket
 * that now belongs to another client.
 */
#include "bm_web_hal.h"

#include "phpbm.h"             /* phpbm_request_timeout, phpbm_at_stop */
#include "event_bus_php.h"   /* bm_events_post_http / post_ws / ensure_queue / set_http_release */

#include "freertos/FreeRTOS.h"
#include "freertos/semphr.h"
#include "esp_http_server.h"
#include "esp_heap_caps.h"
#include "esp_log.h"
#include "esp_netif.h"
#include "lwip/sockets.h"   /* getpeername, select */
#include "lwip/inet.h"      /* inet_ntop */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <unistd.h>         /* close */

static const char *TAG = "web";

#define WS_POST_WAIT_MS   2000   /* how long the httpd task waits for a reactor-queue slot before dropping */
#define HTTPD_CORE        0      /* the PHP task runs on core 1 */
#define WEB_WAIT_SLICE_MS 1000   /* with no deadline, the wait re-checks the slot this often */
#define WS_MAX_CLIENTS    16
#define WEB_BODY_BASE_MS  10000   /* body read: 10 s plus 1 s per WEB_BODY_RATE bytes */
#define WEB_BODY_RATE     (16 * 1024)

enum { SLOT_PARKED, SLOT_DONE, SLOT_ABANDONED };

typedef struct {
    web_slot_t pub;   /* first: the reactor's handle is &pub */
    int        state;
} hal_slot_t;

static httpd_handle_t    s_httpd;        /* the one HTTP server, shared by http / ws */
static uint16_t          s_port;         /* its port, 0 while it is not running */
static bool              s_http_routes;  /* the HTTP catch-all route is registered */
static SemaphoreHandle_t s_slot_resp;    /* reactor -> httpd: a response is ready (re-checked) */
static portMUX_TYPE      s_slot_lock = portMUX_INITIALIZER_UNLOCKED;
static uint32_t          s_wait_s;       /* 0: no deadline (set by bm_web_hal_set_limits before the routes) */
static size_t            s_max_body;
static volatile bool     s_stopping;     /* the program stopped: every request gets 503 until the restart */

/* Open WebSocket connections: written on the httpd task only. An id is never reused. */
static struct { int fd; int32_t id; } s_ws[WS_MAX_CLIENTS];
static int32_t      s_ws_next = 1;
static volatile int s_ws_count;

/* The client's IP for the parked request's socket, into buf ("" if unavailable). */
static void web_peer_ip(httpd_req_t *req, char *buf, size_t sz)
{
    buf[0] = '\0';
    int fd = httpd_req_to_sockfd(req);
    if (fd < 0) {
        return;
    }
    struct sockaddr_in6 sa;
    socklen_t sl = sizeof sa;
    if (getpeername(fd, (struct sockaddr *) &sa, &sl) != 0) {
        return;
    }
    if (sa.sin6_family == AF_INET6) {
        inet_ntop(AF_INET6, &sa.sin6_addr, buf, sz);
        if (strncasecmp(buf, "::ffff:", 7) == 0) {      /* IPv4-mapped -> bare IPv4 (lwip prints FFFF) */
            memmove(buf, buf + 7, strlen(buf + 7) + 1);
        }
    } else {
        struct sockaddr_in *s4 = (struct sockaddr_in *) &sa;
        inet_ntop(AF_INET, &s4->sin_addr, buf, sz);
    }
}

static const char *web_method_str(int m)
{
    switch (m) {
        case HTTP_GET:     return "GET";
        case HTTP_POST:    return "POST";
        case HTTP_PUT:     return "PUT";
        case HTTP_PATCH:   return "PATCH";
        case HTTP_DELETE:  return "DELETE";
        case HTTP_HEAD:    return "HEAD";
        case HTTP_OPTIONS: return "OPTIONS";
        default:           return "GET";
    }
}

static const char *web_reason(int code)
{
    switch (code) {
        case 100: return "Continue";
        case 101: return "Switching Protocols";
        case 200: return "OK";
        case 201: return "Created";
        case 202: return "Accepted";
        case 203: return "Non-Authoritative Information";
        case 204: return "No Content";
        case 205: return "Reset Content";
        case 206: return "Partial Content";
        case 207: return "Multi-Status";
        case 300: return "Multiple Choices";
        case 301: return "Moved Permanently";
        case 302: return "Found";
        case 303: return "See Other";
        case 304: return "Not Modified";
        case 307: return "Temporary Redirect";
        case 308: return "Permanent Redirect";
        case 400: return "Bad Request";
        case 401: return "Unauthorized";
        case 402: return "Payment Required";
        case 403: return "Forbidden";
        case 404: return "Not Found";
        case 405: return "Method Not Allowed";
        case 406: return "Not Acceptable";
        case 407: return "Proxy Authentication Required";
        case 408: return "Request Timeout";
        case 409: return "Conflict";
        case 410: return "Gone";
        case 411: return "Length Required";
        case 412: return "Precondition Failed";
        case 413: return "Content Too Large";
        case 414: return "URI Too Long";
        case 415: return "Unsupported Media Type";
        case 416: return "Range Not Satisfiable";
        case 417: return "Expectation Failed";
        case 418: return "I'm a teapot";
        case 421: return "Misdirected Request";
        case 422: return "Unprocessable Content";
        case 423: return "Locked";
        case 424: return "Failed Dependency";
        case 425: return "Too Early";
        case 426: return "Upgrade Required";
        case 428: return "Precondition Required";
        case 429: return "Too Many Requests";
        case 431: return "Request Header Fields Too Large";
        case 451: return "Unavailable For Legal Reasons";
        case 500: return "Internal Server Error";
        case 501: return "Not Implemented";
        case 502: return "Bad Gateway";
        case 503: return "Service Unavailable";
        case 504: return "Gateway Timeout";
        case 505: return "HTTP Version Not Supported";
        case 507: return "Insufficient Storage";
        case 511: return "Network Authentication Required";
        default:  return "Unknown";
    }
}

/* ---- the slot ---------------------------------------------------------------------------------- */

static void web_slot_free(hal_slot_t *h)
{
    free(h->pub.body);
    free(h->pub.resp_body);
    free(h->pub.resp_hdrs);
    heap_caps_free(h);
}

bool bm_web_hal_slot_abandoned(const web_slot_t *slot)
{
    const hal_slot_t *h = (const hal_slot_t *) slot;
    taskENTER_CRITICAL(&s_slot_lock);
    bool abandoned = h->state == SLOT_ABANDONED;
    taskEXIT_CRITICAL(&s_slot_lock);
    return abandoned;
}

void bm_web_hal_response_ready(web_slot_t *slot)
{
    hal_slot_t *h = (hal_slot_t *) slot;
    taskENTER_CRITICAL(&s_slot_lock);
    bool abandoned = h->state == SLOT_ABANDONED;
    if (!abandoned) {
        h->state = SLOT_DONE;
    }
    taskEXIT_CRITICAL(&s_slot_lock);
    if (abandoned) {
        web_slot_free(h);              /* the server task already answered 503 and let go */
    } else {
        xSemaphoreGive(s_slot_resp);   /* the server task frees it after sending */
    }
}

/* ext-events: a request nobody takes is answered with `status` (503 when no HTTP handler is set). */
static void web_release(void *handle, int status)
{
    web_slot_t *s = handle;
    const char *reason = web_reason(status);
    s->status = status;
    snprintf(s->resp_ctype, sizeof s->resp_ctype, "text/plain; charset=UTF-8");
    s->resp_len = strlen(reason);
    s->resp_body = malloc(s->resp_len);
    if (s->resp_body) {
        memcpy(s->resp_body, reason, s->resp_len);
    } else {
        s->resp_len = 0;
    }
    bm_web_hal_response_ready(s);
}

/* Waits for the reactor's answer; false once the wait limit passed (the slot is then abandoned). A
 * give meant for an earlier slot only costs one more turn of the loop. */
static bool web_wait(hal_slot_t *h)
{
    TickType_t start = xTaskGetTickCount();
    TickType_t limit = pdMS_TO_TICKS(s_wait_s * 1000);
    for (;;) {
        TickType_t spent = xTaskGetTickCount() - start;
        taskENTER_CRITICAL(&s_slot_lock);
        int state = h->state;
        if (state == SLOT_PARKED && (s_stopping || (s_wait_s > 0 && spent >= limit))) {
            h->state = state = SLOT_ABANDONED;
        }
        taskEXIT_CRITICAL(&s_slot_lock);
        if (state != SLOT_PARKED) {
            return state == SLOT_DONE;
        }
        /* No deadline (request-timeout 0): a handler that never answers keeps the httpd task here. */
        TickType_t slice = pdMS_TO_TICKS(WEB_WAIT_SLICE_MS);
        xSemaphoreTake(s_slot_resp, s_wait_s > 0 && limit - spent < slice ? limit - spent : slice);
    }
}

/* ---- HTTP -------------------------------------------------------------------------------------- */

/* Copy n bytes of src into dst as a string, truncated to fit. */
static void web_copy(char *dst, size_t sz, const char *src, size_t n)
{
    if (n >= sz) {
        n = sz - 1;
    }
    memcpy(dst, src, n);
    dst[n] = '\0';
}

/* A plain-text answer from the server itself. With close, httpd drops the connection instead of
 * draining an unread body. */
static esp_err_t web_send_text(httpd_req_t *req, int status, bool close)
{
    char line[64];
    snprintf(line, sizeof line, "%d %s", status, web_reason(status));
    httpd_resp_set_status(req, line);
    httpd_resp_set_type(req, "text/plain; charset=UTF-8");
    if (close) {
        httpd_resp_set_hdr(req, "Connection", "close");
    }
    httpd_resp_send(req, line + 4, HTTPD_RESP_USE_STRLEN);
    return close ? ESP_FAIL : ESP_OK;
}

static bool web_send_all(httpd_req_t *req, const char *buf, size_t len)
{
    while (len > 0) {
        int r = httpd_send(req, buf, len);
        if (r <= 0) {
            return false;
        }
        buf += r;
        len -= (size_t) r;
    }
    return true;
}

/* The answer to HEAD, written by hand: httpd_resp_send always sends its buffer and derives
 * Content-Length from it. */
static esp_err_t web_send_head(httpd_req_t *req, const char *status, const web_slot_t *s)
{
    size_t size = strlen(status) + strlen(s->resp_ctype) + 96 + s->resp_hdrs_len + 4 * (size_t) s->n_resp_hdr;
    char *head = malloc(size);
    if (head == NULL) {
        return ESP_FAIL;
    }
    size_t n = (size_t) snprintf(head, size, "HTTP/1.1 %s\r\nContent-Type: %s\r\n", status, s->resp_ctype);
    const char *p = s->resp_hdrs;
    for (int i = 0; i < s->n_resp_hdr; i++) {
        const char *value = p + strlen(p) + 1;
        n += (size_t) snprintf(head + n, size - n, "%s: %s\r\n", p, value);
        p = value + strlen(value) + 1;
    }
    n += (size_t) snprintf(head + n, size - n, "Content-Length: %u\r\n\r\n", (unsigned) s->resp_len);
    bool sent = web_send_all(req, head, n);
    free(head);
    return sent ? ESP_OK : ESP_FAIL;
}

static esp_err_t web_send_reply(httpd_req_t *req, web_slot_t *s)
{
    int status = s->status < 100 || s->status > 599 ? 500 : s->status;
    char line[64];
    snprintf(line, sizeof line, "%d %s", status, web_reason(status));
    if (s->resp_ctype[0] == '\0') {
        snprintf(s->resp_ctype, sizeof s->resp_ctype, "text/html; charset=UTF-8");
    }
    if (req->method == HTTP_HEAD) {
        return web_send_head(req, line, s);
    }
    httpd_resp_set_status(req, line);
    httpd_resp_set_type(req, s->resp_ctype);
    const char *p = s->resp_hdrs;
    for (int i = 0; i < s->n_resp_hdr; i++) {
        const char *value = p + strlen(p) + 1;
        httpd_resp_set_hdr(req, p, value);   /* the strings live until the slot is freed */
        p = value + strlen(value) + 1;
    }
    return httpd_resp_send(req, s->resp_body ? s->resp_body : "", (ssize_t) s->resp_len);
}

/* Copies the wanted request headers into the slot; false if they do not fit (never within httpd's
 * own header limit, which is the size of hdr_buf). */
static bool web_read_headers(httpd_req_t *req, web_slot_t *s)
{
    static const char *want[] = {
        "Host", "Content-Type", "User-Agent", "Accept", "Cookie", "Authorization",
        "X-Requested-With", "Accept-Language",
    };
    size_t used = 0;
    s->n_hdr = 0;
    for (size_t i = 0; i < sizeof want / sizeof want[0] && s->n_hdr < WEB_MAX_HDRS; i++) {
        size_t len = httpd_req_get_hdr_value_len(req, want[i]);
        if (len == 0) {
            continue;
        }
        if (used + len + 1 > sizeof s->hdr_buf ||
            httpd_req_get_hdr_value_str(req, want[i], s->hdr_buf + used, len + 1) != ESP_OK) {
            return false;
        }
        s->hdr[s->n_hdr].name = want[i];
        s->hdr[s->n_hdr].value = s->hdr_buf + used;
        s->n_hdr++;
        used += len + 1;
    }
    return true;
}

/* Reads the whole body into the slot; 0 on success, else the error status. */
static int web_read_body(httpd_req_t *req, web_slot_t *s)
{
    if (req->content_len == 0) {
        return 0;
    }
    if (s_max_body > 0 && req->content_len > s_max_body) {
        return 413;
    }
    s->body = heap_caps_malloc(req->content_len, MALLOC_CAP_SPIRAM | MALLOC_CAP_8BIT);
    if (s->body == NULL) {
        return 413;
    }
    /* One recv timeout ends the read, and a wall-clock deadline stops a client that drips bytes
     * just fast enough to never time out. */
    TickType_t start = xTaskGetTickCount();
    uint64_t ms = WEB_BODY_BASE_MS + (uint64_t) req->content_len * 1000 / WEB_BODY_RATE;
    TickType_t limit = pdMS_TO_TICKS((uint32_t) (ms < 3600000 ? ms : 3600000));
    while (s->body_len < req->content_len) {
        int r = httpd_req_recv(req, s->body + s->body_len, req->content_len - s->body_len);
        if (r <= 0 || xTaskGetTickCount() - start > limit) {
            return r > 0 || r == HTTPD_SOCK_ERR_TIMEOUT ? 408 : 400;
        }
        s->body_len += (size_t) r;
    }
    return 0;
}

/* Runs on the httpd task: parse the request, hand it to the reactor, send the response it returns. */
static esp_err_t web_handle(httpd_req_t *req)
{
    if (s_stopping) {
        return web_send_text(req, 503, false);
    }
    hal_slot_t *h = heap_caps_calloc(1, sizeof *h, MALLOC_CAP_SPIRAM | MALLOC_CAP_8BIT);
    if (h == NULL) {
        return web_send_text(req, 503, false);
    }
    web_slot_t *s = &h->pub;
    s->method = web_method_str(req->method);

    const char *qm = strchr(req->uri, '?');
    web_copy(s->path, sizeof s->path, req->uri, qm ? (size_t) (qm - req->uri) : strlen(req->uri));
    web_copy(s->query, sizeof s->query, qm ? qm + 1 : "", qm ? strlen(qm + 1) : 0);
    if (!web_read_headers(req, s)) {
        web_slot_free(h);
        return web_send_text(req, 431, false);
    }
    web_peer_ip(req, s->ip, sizeof s->ip);
    int err = web_read_body(req, s);
    if (err != 0) {
        web_slot_free(h);
        return web_send_text(req, err, true);
    }

    /* Once the slot is abandoned the reactor may free it at any time: the log uses these copies. */
    const char *method = s->method;
    char path[sizeof s->path];
    memcpy(path, s->path, sizeof path);
    h->state = SLOT_PARKED;
    if (!bm_events_post_http(s)) {
        web_slot_free(h);
        return web_send_text(req, 503, false);   /* the reactor queue is full */
    }
    if (!web_wait(h)) {
        /* The reactor owns the slot now and frees it when it answers: no access to it from here on. */
        if (s_stopping) {
            ESP_LOGW(TAG, "%s %s: the program stopped, 503 sent", method, path);
        } else {
            ESP_LOGW(TAG, "%s %s: no answer within %u s, 503 sent", method, path, (unsigned) s_wait_s);
        }
        return web_send_text(req, 503, false);
    }
    esp_err_t e = web_send_reply(req, s);
    web_slot_free(h);
    return e;
}

/* ---- WebSocket ------------------------------------------------------------------------------------ */

/* httpd task: the id of the connection on fd, registering it on first sight (0 if the table is full). */
static int32_t ws_client(int fd)
{
    for (int i = 0; i < WS_MAX_CLIENTS; i++) {
        if (s_ws[i].id != 0 && s_ws[i].fd == fd) {
            return s_ws[i].id;
        }
    }
    for (int i = 0; i < WS_MAX_CLIENTS; i++) {
        if (s_ws[i].id == 0) {
            int32_t id = s_ws_next;
            s_ws_next = s_ws_next == INT32_MAX ? 1 : s_ws_next + 1;
            s_ws[i].fd = fd;
            s_ws[i].id = id;
            s_ws_count++;
            return id;
        }
    }
    return 0;
}

/* httpd task: every closed session passes here (with a close_fn set, the socket is ours to close). */
static void web_close(httpd_handle_t hd, int fd)
{
    (void) hd;
    for (int i = 0; i < WS_MAX_CLIENTS; i++) {
        if (s_ws[i].id != 0 && s_ws[i].fd == fd) {
            s_ws[i].id = 0;
            s_ws_count--;
        }
    }
    close(fd);
}

/* httpd task: read a frame and hand it to the reactor (a GET is the handshake: register the client). */
static esp_err_t web_ws_handle(httpd_req_t *req)
{
    if (req->method == HTTP_GET) {
        ws_client(httpd_req_to_sockfd(req));
        return ESP_OK;
    }
    httpd_ws_frame_t frame = { 0 };
    frame.type = HTTPD_WS_TYPE_TEXT;
    esp_err_t r = httpd_ws_recv_frame(req, &frame, 0);   /* length only */
    if (r != ESP_OK) {
        return r;
    }
    if (frame.len == 0) {
        return ESP_OK;
    }
    ws_msg_t *m = malloc(sizeof(ws_msg_t) + frame.len + 1);
    if (!m) {
        return ESP_ERR_NO_MEM;
    }
    frame.payload = (uint8_t *) m->data;
    r = httpd_ws_recv_frame(req, &frame, frame.len);
    if (r != ESP_OK) {
        free(m);
        return r;
    }
    m->data[frame.len] = '\0';
    m->len = frame.len;
    m->binary = (frame.type == HTTPD_WS_TYPE_BINARY);
    m->client = ws_client(httpd_req_to_sockfd(req));
    /* backpressure: block until the reactor has room (TCP slows the sender); drop only if it times out */
    if (!bm_events_post_ws(m, WS_POST_WAIT_MS)) {
        free(m);
    }
    return ESP_OK;
}

typedef struct {
    int32_t client;   /* 0: every client */
    size_t  len;
    char    data[];
} ws_job_t;

/* httpd task: send the frame to one open WebSocket fd if it can take it now (never blocks). */
static void ws_send_fd(int fd, ws_job_t *j)
{
    if (httpd_ws_get_fd_info(s_httpd, fd) != HTTPD_WS_CLIENT_WEBSOCKET) {
        return;
    }
    fd_set wf;
    FD_ZERO(&wf);
    FD_SET(fd, &wf);
    struct timeval z = { 0, 0 };
    if (select(fd + 1, NULL, &wf, NULL, &z) <= 0 || !FD_ISSET(fd, &wf)) {
        return;   /* not writable now -> drop this frame for this client */
    }
    httpd_ws_frame_t frame = {
        .type = HTTPD_WS_TYPE_TEXT,
        .payload = (uint8_t *) j->data,
        .len = j->len,
    };
    httpd_ws_send_frame_async(s_httpd, fd, &frame);
}

/* httpd task: the registry is current here, so a closed client's fd is never written to. */
static void ws_send_work(void *arg)
{
    ws_job_t *j = arg;
    for (int i = 0; i < WS_MAX_CLIENTS; i++) {
        if (s_ws[i].id != 0 && (j->client == 0 || s_ws[i].id == j->client)) {
            ws_send_fd(s_ws[i].fd, j);
        }
    }
    free(j);
}

static bool ws_queue(int32_t client, const char *data, size_t len)
{
    ws_job_t *j = heap_caps_malloc(sizeof(ws_job_t) + len, MALLOC_CAP_SPIRAM | MALLOC_CAP_8BIT);
    if (!j) {
        return false;
    }
    j->client = client;
    j->len = len;
    memcpy(j->data, data, len);
    if (httpd_queue_work(s_httpd, ws_send_work, j) != ESP_OK) {
        free(j);
        return false;
    }
    return true;
}

/* A hint read from the PHP task; the httpd task checks again before sending. */
static bool ws_known(int32_t client)
{
    for (int i = 0; i < WS_MAX_CLIENTS; i++) {
        if (*(volatile int32_t *) &s_ws[i].id == client) {
            return true;
        }
    }
    return false;
}

bool bm_web_hal_ws_send(int32_t client, const char *data, size_t len)
{
    if (!s_httpd || client <= 0 || !ws_known(client)) {
        return false;
    }
    return ws_queue(client, data, len);
}

int bm_web_hal_ws_broadcast(const char *data, size_t len)
{
    int n = s_ws_count;
    if (!s_httpd || n <= 0) {
        return 0;
    }
    return ws_queue(0, data, len) ? n : 0;
}

/* ---- server ------------------------------------------------------------------------------------- */

bool bm_web_hal_network_ready(void)
{
    return esp_netif_get_default_netif() != NULL;
}

void bm_web_hal_set_limits(uint32_t wait_s, size_t max_body)
{
    s_wait_s = wait_s;
    s_max_body = max_body;
}

uint32_t bm_web_hal_request_timeout(void)
{
    int t = phpbm_request_timeout();
    return t > 0 ? (uint32_t) t : 0;
}

/* phpbm_at_stop hook (PHP task): the program ended, so the request being waited on and every later one
 * is answered 503 by the httpd task instead of waiting for a reactor that no longer runs. */
static void web_stop(void)
{
    s_stopping = true;
    if (s_slot_resp) {
        xSemaphoreGive(s_slot_resp);
    }
}

bool bm_web_hal_start(uint16_t port)
{
    if (s_httpd) {
        return true;
    }
    bm_events_ensure_queue();
    if (!s_slot_resp) {
        s_slot_resp = xSemaphoreCreateBinary();
    }
    if (!s_slot_resp) {
        return false;
    }
    httpd_config_t cfg = HTTPD_DEFAULT_CONFIG();
    cfg.server_port = port;
    cfg.uri_match_fn = httpd_uri_match_wildcard;
    cfg.lru_purge_enable = true;
    cfg.stack_size = 8192;
    cfg.core_id = HTTPD_CORE;
    cfg.task_caps = MALLOC_CAP_INTERNAL | MALLOC_CAP_8BIT;
    cfg.max_uri_handlers = 12;
    cfg.max_req_hdr_len = WEB_HDR_BUF;   /* a longer header block gets 431 from httpd */
    cfg.max_resp_headers = WEB_MAX_RESP_HDRS + 4;
    cfg.send_wait_timeout = 2;   /* bound a stuck outbound send (default 10 s) -- a slow client is dropped */
    cfg.close_fn = web_close;
    if (httpd_start(&s_httpd, &cfg) != ESP_OK) {
        return false;
    }
    bm_events_set_http_release(web_release);
    phpbm_at_stop(web_stop);
    s_port = port;
    return true;
}

uint16_t bm_web_hal_port(void)
{
    return s_port;
}

bool bm_web_hal_register_http(void)
{
    if (!s_httpd) {
        return false;
    }
    static const httpd_method_t methods[] = {
        HTTP_GET, HTTP_POST, HTTP_PUT, HTTP_PATCH, HTTP_DELETE, HTTP_HEAD, HTTP_OPTIONS,
    };
    for (size_t i = 0; i < sizeof methods / sizeof methods[0]; i++) {
        httpd_uri_t u = { .uri = "/*", .method = methods[i], .handler = web_handle };
        httpd_register_uri_handler(s_httpd, &u);   /* registered after any ws path */
    }
    s_http_routes = true;
    ESP_LOGI(TAG, "HTTP routes registered");
    return true;
}

bool bm_web_hal_register_ws(const char *path)
{
    if (!s_httpd) {
        return false;
    }
    httpd_uri_t u = { .uri = path, .method = HTTP_GET, .handler = web_ws_handle, .is_websocket = true };
    if (httpd_register_uri_handler(s_httpd, &u) != ESP_OK) {
        return false;
    }
    if (s_http_routes) {
        /* move the http catch-all after this path (re-register) so the exact WS route matches first */
        httpd_unregister_uri_handler(s_httpd, "/*", HTTP_GET);
        httpd_uri_t g = { .uri = "/*", .method = HTTP_GET, .handler = web_handle };
        httpd_register_uri_handler(s_httpd, &g);
    }
    ESP_LOGI(TAG, "WebSocket endpoint at %s", path);
    return true;
}
