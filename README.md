# esp32-web-hal

The ESP32 side of [ext-web](package:php-baremetal/ext-web): the HTTP server over `esp_http_server`, the server
task that reads requests and WebSocket frames and hands them to the PHP reactor, and the rendezvous that sends
back the reply PHP fills in. It implements `bm_web_hal.h` from ext-web.

| Key | Value |
| --- | --- |
| Type | `bm-hal` |
| Platform | `esp32` (`extra.baremetal.platform`) |
| Provides | `php-baremetal/web-hal` |
| ESP-IDF components | `esp_http_server`, `esp_netif`, `phpbm` (the SDK host component) |
| sdkconfig | `CONFIG_HTTPD_WS_SUPPORT=y` (WebSocket support in `esp_http_server`) |

## Install

Projects do not require it. A project requires `php-baremetal/ext-web`, and conductor adds
`php-baremetal/esp32-web-hal` because the project's SDK (`"sdk": "esp32@^1.0"`) has the platform `esp32`:

```bash
composer require php-baremetal/ext-web
```

## Behaviour on ESP32

**The server task.** One `esp_http_server` instance, started by the first `serve_http()` or `serve_ws()`:
task on core 0, 8 KB stack in internal RAM, LRU purge of idle sockets when the socket table is full, 12 URI
handler slots, request header block up to 2048 bytes, up to 20 response headers, a 2 s limit on a stuck
outbound send. The task never calls PHP: it parks each request in a slot (in PSRAM), posts the slot to the
event queue of ext-events and waits for the reply.

**Requests.** Methods `GET`, `POST`, `PUT`, `PATCH`, `DELETE`, `HEAD` and `OPTIONS` go to one catch-all route
(`/*`). The server captures eight headers (`Host`, `Content-Type`, `User-Agent`, `Accept`, `Cookie`,
`Authorization`, `X-Requested-With`, `Accept-Language`), the client IP, and the whole body (into PSRAM).

| Situation | Answer from the server task |
| --- | --- |
| Header block over 2048 bytes | 431 |
| Body over `post_max_size`, or no memory for it | 413 |
| Body not received within 10 s plus 1 s per 16 KB | 408 (400 on a socket error) |
| Event queue full, or no memory for the slot | 503 |
| No reply within `request-timeout` + 5 s | 503, logged as `no answer within .. s, 503 sent` |
| The program stopped | 503, logged as `the program stopped, 503 sent` |

**Waiting and ownership.** Each request slot is parked, done or abandoned. If the deadline passes first, the
server task answers 503 and abandons the slot; when PHP later reaches it, ext-web skips the handler and the
slot is freed then. Whoever finishes last frees it, so a late reply never touches freed memory. With
`request-timeout` 0 there is no deadline.

**Stop.** The HAL registers a stop hook with the SDK host (`phpbm_at_stop()`). When the PHP program ends
(fatal error, `exit()`, `sys_restart()`), the request being waited on and every later one get 503 until the
board restarts.

**WebSocket.** The handshake registers the connection in a table of 16 with an id that is never reused.
Inbound frames are copied and posted to the event queue, waiting up to 2 s for room. Outbound frames
(`Message::reply()`, `ws_broadcast()`) are queued to the server task with `httpd_queue_work`, so they are
written on the task that opens and closes connections; a client whose socket is not writable at that moment
is skipped for that frame.

**Network.** `serve_http()` needs a default network interface: the wired network the SDK brings up with
`"network": "on"`, or the Wi-Fi interface `ext-wifi` creates.

## License

MIT.
