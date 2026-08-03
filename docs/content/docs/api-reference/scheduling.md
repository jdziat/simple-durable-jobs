---
title: "Scheduling"
weight: 7
---

## Schedules

### `Every(d time.Duration) Schedule`

Creates a schedule that runs at fixed intervals.

### `Daily(hour, minute int) Schedule`

Creates a schedule that runs daily at the specified time (UTC).

### `Weekly(day time.Weekday, hour, minute int) Schedule`

Creates a schedule that runs weekly on the specified day and time (UTC).

### `DailyIn(loc *time.Location, hour, minute int) Schedule`

`Daily` with the hour and minute interpreted in `loc`, honouring that location's
DST rules — `DailyIn(newYork, 9, 0)` fires at 09:00 New York time year-round, not
at a fixed UTC offset. `loc` must be non-nil.

Exactly one fire per calendar day at both DST edges. The fire is the **earliest
instant on that calendar day whose clock in `loc` has reached `hour:minute`**:

- the reading exists — the usual case — and the fire is at it;
- the reading does not exist because the clock jumped over it (spring forward),
  and the fire is at the instant of the jump: `DailyIn(newYork, 2, 30)` fires at
  03:00 EDT on a US spring-forward day;
- the reading exists twice (fall back), and the **first** occurrence fires.

`Next` always returns an instant strictly after its argument, so a schedule can
never stall. A calendar day that cannot hold the fire at all — a location can skip
the tail of a day, or a whole day when it crosses the date line — is skipped
rather than pushed onto the next day.

`Daily(h, m)` is `DailyIn(time.UTC, h, m)`, and UTC has no DST, so none of the
above changes what a `Daily` schedule does.

### `WeeklyIn(loc *time.Location, day time.Weekday, hour, minute int) Schedule`

`Weekly` with the day, hour and minute interpreted in `loc`. The fire always lands
on the requested **calendar weekday** in `loc`; the DST notes on `DailyIn` apply.
`loc` must be non-nil.

### `Cron(expr string) (Schedule, error)`

Creates a schedule from a cron expression. It returns an error for an invalid
expression, so it cannot be used inline where a `Schedule` is expected:

```go
sched, err := jobs.Cron("0 */2 * * *")
if err != nil {
    return err
}
```

The default zone is **UTC**, not the host's local zone, so the same expression
fires at the same instant on every node in a fleet. A `CRON_TZ=Area/City ` or
`TZ=Area/City ` prefix on the expression is honoured.

### `MustCron(expr string) Schedule`

The same, but panics on an invalid expression. Use it for expressions fixed at
compile time, where it can be passed inline.

### `CronIn(loc *time.Location, expr string) (Schedule, error)`

Interprets the expression in `loc` rather than UTC — note `loc` comes **first**.
`loc` must be non-nil. `MustCronIn(loc, expr)` is the panicking variant.
