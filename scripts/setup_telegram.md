# Telegram Alert Setup

1. Create a bot with BotFather and copy the bot token.
2. Send one message to the bot from the target Telegram chat.
3. Open `https://api.telegram.org/bot<token>/getUpdates` and copy the chat id.
4. Add both values to `docker/.env`:

```env
TELEGRAM_BOT_TOKEN=...
TELEGRAM_CHAT_ID=...
```

5. Recreate the Airflow services:

```bash
cd docker
docker compose up -d --build
```

Failure callbacks, silent-skip warnings, and `catch_up` summaries use these
credentials. Messages are sent as plain text.
