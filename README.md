# YP_KAFKA_PRACTICE_2
Репо для сдачи Практической работы 2 по курсу ЯП Kafka


# Структуры данных
## Топики:
* registered_users - информация о залогинившихся пользователях (пока не использую)
* blocked_users - заблокированые пользователи
* blocked_words - заблокированые слова
* messages - "сырые" сообщения пользователей
* filtered_messages - обработанные сообщения пользователей


# Содержится два приложения:
1) user_messenger - реализует работу через консоль:
- комнады пользвателей для входа, блокировки/разблокировки слов и пользователей, отправки сообщений
- выводит в консоль сообщения из топика filtered_messages
2) message_processor - обрабатывает топик messages и помещает обработанные сообщения в топик filtered_messages
- маскирует заблокированные слова
- отправляет сообщения в топик filtered_messages если пользователь не заблокировал доставку

Для того чтобы пользователи могли блокировать и разблокировать слова и других пользователей
используется поле event - BLOCK|UNBLOCK

# Как запустить проект:
## Запуск контейнеров
make up-infra
make init-topics
make up
make load-blocked-words

## Тестирование
docker compose run --rm -it user_cli

### Вход
login(kate)
### Блокировка пользователя
block_user_for_me(anna)
### Разблокировка пользователя
### Блокировка слова
block_word(anna_bubu)
### Разблокировка слова
unblock_word(anna_bubu)
send_message(anna,hello)
### Отправка сообщения пользователю
send_message(mary, hello spam )
### Выход из пользователя
logout()

# Кейсы для тестирования

## Блокировка и разблокировка слов:
Пример работы из docker compose run --rm -it user_cli

CLI app ready. Enter commands like: login(alice), logout(), send_message(bob,Hi)
> login(kate)
Logged in as: kate
> send_message(anna, Hi Anna this is spam for you)
Message sent to anna
> 
📩 [04:38:09] kate → anna: Hi Anna this is *** for you
block_word(kuku)
> send_message(bob, kuku)
Message sent to bob
> 
📩 [04:38:32] kate → bob: ***
unblock_word(kuku)
> send_message(bob, kuku boby)
Message sent to bob
> 
📩 [04:38:52] kate → bob: kuku boby


## Блокировка и разблокировка пользователей:
Пример работы из docker compose run --rm -it user_cli
Здесь пользователь mary блокирует пользователя kate и сообщения от kate к mary не доходят
После разблокировки сообщения доходят

> login(mary)
Logged in as: mary
> block_user_for_me(kate)
> send_message(kate, hi you blocked)
Message sent to kate
> 
📩 [04:41:11] mary → kate: hi you blocked
logout()
Logged out.
✅ Подтверждено: event=None | топик=messages | partition=2 | offset=3
> login(kate)
Logged in as: kate
> send_message(mary, hi too)
Message sent to mary
> 

--- Разблокировка
> login(mary)
Logged in as: mary
> unblock_user_for_me(kate)
logout()
Logged out.
> login(kate)
Logged in as: kate
> send_message(mary, wtf)
Message sent to mary
> 
📩 [04:45:45] kate → mary: wtf
