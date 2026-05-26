        # kafka — Rebalance: eager vs cooperative

        Homework-шаблон для урока **l2_rebalance_protocols** (Rebalance: eager vs cooperative) на платформе Vibe Learn.

        ## Что делать

        Реализуй на Go симуляцию двух режимов consumer group и измерь downtime при rolling restart.

**Часть 1 — Eager режим (baseline):**
- Запусти consumer group из 6 consumers, `partition.assignment.strategy` дефолтный (RangeAssignor / eager).
- Симулируй rolling restart: по очереди останови и перезапусти каждый consumer.
- Измерь суммарный downtime (время, за которое ни один consumer не читает хотя бы одну конкретную партицию).
- Залогируй время каждого rebalance.

**Часть 2 — Production-grade режим:**
- Та же группа, но с `CooperativeStickyAssignor` + `group.instance.id=consumer-{index}` + `session.timeout.ms=45s`.
- Проведи тот же rolling restart.
- Измерь суммарный downtime.

**CI assertion:**
- downtime(cooperative+sticky+static) < downtime(eager) × 0.10
- То есть cooperative режим должен давать < 10% downtime относительно eager baseline.

## Контекст (из transfer-задачи урока)

У тебя consumer group из **50 consumers**, которая обрабатывает критический топик с платёжными событиями.
Деплой через Kubernetes — pod recreated по одному (rolling deploy, maxUnavailable=1).
Каждое пересоздание пода занимает примерно 30 секунд.

**Проблема:** каждый раз, когда pod пересоздаётся, группа уходит в rebalance и отстаёт на **30 секунд** (consumer lag).
За время деплоя 50 подов суммарный lag = 50 × 30s = **25 минут накопленного lag'а**.
SLA: lag должен восстанавливаться за ≤ 5 минут после каждого деплоя.

## Recap из урока

- **Eager rebalance (≤ 2.3)** — stop-the-world: все consumers revoke все партиции перед новым assignment. Для группы из 50+ consumers это секунды–минуты полной паузы чтения.
- **Cooperative rebalance (2.4+)** — инкрементальный двухраундовый протокол: revoke только перемещаемые партиции. Большинство consumers не прерываются. Включается через `partition.assignment.strategy=CooperativeStickyAssignor`.
- **Sticky assignor** минимизирует движения партиций между rebalance'ами, сохраняя cache locality. В связке с cooperative — оптимальный production выбор.
- **Static membership** (`group.instance.id`) защищает от rebalance при временных разрывах (pod restart, rolling deploy): coordinator ждёт `session.timeout.ms` перед объявлением rebalance. Если consumer вернулся раньше с тем же ID — rebalance не происходит.
- **Дефолт (`RangeAssignor` без `group.instance.id`)** — anti-pattern для большой production group: каждый pod restart = полный eager rebalance = lag spike. Мигрируй на `CooperativeStickyAssignor` + `group.instance.id` + адекватный `session.timeout.ms`.

        ## Как работать

        1. Платформа Vibe Learn создаёт копию этого репо в твоём GitHub-аккаунте по клику «Начать домашку» на странице урока (через GitHub `/generate`, codecrafters-pattern).
        2. Склонируй копию локально, реализуй TODO в `main.go`, прогони тесты, запушь.
        3. CI (`.github/workflows/ci.yml`) запускает `go vet` + `go test ./...` на каждый push. Платформа слушает результат через webhook от GitHub Actions и обновляет статус домашки на странице урока.

        ## Локальное окружение

        - Go 1.22+
        - Docker + docker-compose — `docker compose -f docker-compose.yml up -d` поднимает 3-нодовый Kafka cluster на портах 9092/9093/9094, использовать в тестах через bootstrap `localhost:9092,localhost:9093,localhost:9094`.

        ## Запуск

        ```bash
        # Поднять локальный Kafka
        docker compose up -d

        # Прогнать тесты (часть из них стартует свой ephemeral testcontainers cluster, часть использует docker-compose выше)
        go test ./...

        # Запустить main (печатает marker; замени stub на реализацию)
        go run .
        ```

        ## Заметка автора

        Это baseline-шаблон, сгенерированный платформой. Бизнес-сущность задачи (что конкретно реализовать в `main.go`, какие тесты сделать строгими) расширяется по ходу итераций — параллельно с углублением теории урока.
