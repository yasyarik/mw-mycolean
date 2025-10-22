# MW Mycolean — минимальный приёмник `orders/create`

## Установка
```bash
cd mw-mycolean
npm i
```

## Конфиг
Скопируй `.env.example` в `.env` и вставь секрет из Shopify (Settings → Notifications → Webhooks → Reveal secret).
```bash
cp .env.example .env
```

## Запуск
```bash
npm start
```

Проверь:
- `http://localhost:8080/health` → ok

## Публичный URL для вебхука
Пример с cloudflared:
```bash
cloudflared tunnel --url http://localhost:8080
```
URL вида `https://XXXX.trycloudflare.com`.

## Вебхук в Shopify
Settings → Notifications → Webhooks → Create webhook:
- Event: Order creation
- Format: JSON
- URL: `https://XXXX.trycloudflare.com/webhooks/orders-create`

Нажми **Send test notification**. В терминале:
- `skip ...` — заказ без метки
- `MATCH ... { theme: 'preview-...', debug: true|false }` — наш заказ с меткой
