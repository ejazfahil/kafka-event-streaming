# Kafka Topic Conventions 2024-02-22

## Naming: `<domain>.<entity>.<event>`
- `payments.transaction.created`
- `users.profile.updated`
- `inventory.item.depleted`

## Partitioning
- Key = user_id (co-locate user events)
- Partitions: 12 (3× consumer count)

## Retention
- Main topics: 7 days
- DLQ: 30 days (for replay)
