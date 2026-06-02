---
title: "Scheduling"
weight: 6
---

## Schedules

### `Every(d time.Duration) Schedule`

Creates a schedule that runs at fixed intervals.

### `Daily(hour, minute int) Schedule`

Creates a schedule that runs daily at the specified time (UTC).

### `Weekly(day time.Weekday, hour, minute int) Schedule`

Creates a schedule that runs weekly on the specified day and time (UTC).

### `Cron(expr string) Schedule`

Creates a schedule from a cron expression.
