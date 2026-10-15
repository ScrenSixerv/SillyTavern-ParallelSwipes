# Parallel Swipes for SillyTavern

Generate several alternative replies (swipes) **at the same time** instead of one after another.

Many chat completion providers no longer accept `n > 1` ("Multiple swipes per generation"), so SillyTavern's built-in Multi Swipe fails with errors like `Invalid n value (currently only n = 1 is supported)`. This extension brings that feature back. When SillyTavern sends its reply request, the extension sends N-1 identical copies in parallel. Once SillyTavern's own reply finishes, it adds the other results to the same message as swipes. Switch between them with the left and right arrows as usual.

Total wait time is roughly the time of the slowest request, not the sum of all of them.

> The extension's interface (settings panel, menu entry, notifications) is in Simplified Chinese.

## Features

- **Same context.** Each parallel request uses an exact copy of the request body SillyTavern builds: same prompt, preset, world info, and connection settings.
- **Native-looking swipes.** Results go through the same cleanup SillyTavern applies to normal replies (stopping strings, regex scripts, whitespace trimming, reasoning parsing), so they look like any other swipe.
- **Adjustable at any time.** Change the count from the wand menu, the settings panel, or the `/pswipe` slash command.
- **Progress bar with a stop button.** SillyTavern's own stop button also stops the parallel requests.
- **Resilient:**
  - Only SillyTavern's own reply requests are copied. Requests from other extensions or scripts (for example image prompt generators using `generateRaw`) are left alone.
  - Rate limits (429), gateway errors, and "service too busy" responses are retried once. This can be turned off.
  - A configurable timeout gives up on any request that stops sending content, so the bar never hangs forever.
  - If SillyTavern's own reply fails or comes back empty, the first parallel result replaces it instead of leaving a blank swipe.
  - If you switch chats or delete or change the message while requests are running, the results are discarded rather than written to the wrong place.
  - While the extension is enabled, the built-in `n` parameter is removed from requests so it can no longer cause `Bad Request` errors.

## Requirements

- SillyTavern with the **Chat Completion** API. Tested on SillyTavern 1.19.0 with a custom OpenAI-compatible endpoint.
- A provider that allows several concurrent requests with your key. If it doesn't, extra requests are rejected (see Troubleshooting).

## Installation

1. In SillyTavern, open **Extensions** (the stacked-blocks icon) and click **Install extension**.
2. Paste this repository's URL:
   ```
   https://github.com/ScrenSixerv/SillyTavern-ParallelSwipes
   ```
3. Reload the page. A **并行备选回复** section appears in the extension settings.

Manual install: copy this folder into `data/<your-user>/extensions/` (or `public/scripts/extensions/third-party/` for all users) and reload SillyTavern.

## Usage

The count is the **total** number of replies per generation, **including SillyTavern's own**. For example, `3` means SillyTavern's reply plus 2 parallel ones.

| Where | How |
| --- | --- |
| Wand menu | Click **并行回复：N 条** and enter a number. `1` turns it off. |
| Slash command | `/pswipe 3` sets the count to 3. `/pswipe off` turns it off. `/pswipe` alone returns the current count (`0` = off). |
| Extension settings | **启用** (enable), **每次生成的回复总数** (total replies, 1–10), **右滑生成新回复时也并行** (also run in parallel when swiping right), **遇到限流/网关错误自动重试一次** (retry once on rate limit or gateway errors), **多少秒没收到内容就放弃这条** (timeout in seconds, 15–600, default 90). |

It runs on normal sends, regenerate, and swipe-right on the last reply.

**Billing:** every parallel request is a full, separately billed request. A count of 3 costs about three times as much as a normal generation.

### Deleting a swipe you don't want

SillyTavern can already do this for the last message. Either use `/delswipe 2` (1-based swipe number), or open the message editor, click the trash icon, and choose **Delete Swipe**. Choosing **Delete Message** removes the whole message.

## Limitations

- Group chats, Continue, Impersonate, and quiet or background generations are not handled. They generate normally.
- Text Completion, NovelAI, KoboldAI, and other non-Chat-Completion APIs are not supported.
- Swipes are added after SillyTavern's own reply finishes and the UI is idle, so a parallel reply that finishes early still waits for that one.
- Tool calling is not replayed in the parallel requests.

## Troubleshooting

- **"Service is too busy" / 503 / 429 errors on the extra replies.** Your provider is limiting concurrent requests. Lower the count, try again later, or use a different provider.
- **Nothing streams, even SillyTavern's own reply.** That is the provider, not this extension. Run `/pswipe off` and send a message to check. The extension gives up on stuck parallel requests after the timeout, but SillyTavern's own request has to be stopped with its stop button.
- **No swipes were added.** Open the browser console (F12) and look for lines starting with `[并行备选]`. They explain why each request failed.

## How it works

1. On `GENERATION_STARTED`, the extension notes that a native `normal`, `regenerate`, or `swipe` generation is starting.
2. On `CHAT_COMPLETION_SETTINGS_READY`, it checks that the request belongs to that generation, then copies the final request body (registered last, after other listeners have modified it).
3. It sends the copies to SillyTavern's own backend endpoint `/api/backends/chat-completions/generate`. Your API keys stay on the server and never touch the extension.
4. Each response (streaming or not) is parsed and cleaned the way SillyTavern cleans a normal reply.
5. When everything has settled and SillyTavern is idle, the results are appended to the target message's `swipes` and `swipe_info`, and the chat is saved.

## License

[MIT](LICENSE)
