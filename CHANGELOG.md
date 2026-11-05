# Changelog

## 1.0.3

- Fixed: the request slot, the request body and the queued WebSocket frames were allocated only in PSRAM. On a
  board with 2 MB of PSRAM the Wi-Fi buffers can leave less than one slot free, and every request got 503. They
  now fall back to internal RAM.
- Changed: a 503 for lack of memory or for a full event queue now logs a warning with the path, like the 503
  after the time limit already did.

## 1.0.2

- Docs: the README gave the 20 response header slots of httpd as the limit; PHP can set 16 through ext-web,
  and the other 4 are for the HAL's own headers.

## 1.0.1

- Changed: the author's contact email.
- Docs: projects use `"sdk": "esp32@^2.0"`.

## 1.0.0

Initial release.
