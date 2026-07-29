// Command upgradematrix seeds or checks a scheduled-fire cursor database, so a
// released binary and the working tree's binary can be pointed at the same file.
//
// It is built from BOTH trees by scripts/upgrade-matrix.sh, so it must compile
// against the older tag too: keep it to long-stable API only.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/schedule"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/storage"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// families covers both clock-face lineages. Daily and Weekly pin UTC, so their
// cursors are UTC-faced. Every and Cron derive from the seed anchor (time.Now(),
// local) and their Next() preserves that location, so theirs carry the host
// offset. A fix that forces either face stalls the other lineage.
func families() map[string]schedule.Schedule {
	f := map[string]schedule.Schedule{
		"every":       schedule.Every(time.Hour),
		"daily":       schedule.Daily(9, 0),
		"weekly":      schedule.Weekly(time.Monday, 9, 0),
		"crossface":   schedule.Every(time.Hour),
		"seeded-only": schedule.Daily(9, 0),
	}
	c, err := schedule.Cron("0 * * * *")
	if err != nil {
		// Do NOT silently drop the family. Omitting it lets the matrix report
		// success while never testing that schedule shape at all, which is exactly
		// the way this harness was blind to a predicate regression before.
		fmt.Fprintf(os.Stderr, "upgradematrix: cron schedule failed to parse: %v\n", err)
		os.Exit(1)
	}
	f["cron"] = c
	return f
}

func order() []string {
	return []string{"cron", "crossface", "daily", "every", "seeded-only", "weekly"}
}

// crossZone is a fixed offset deliberately unlike any host zone the matrix runs
// under, and deliberately NOT a whole hour.
//
// The "crossface" family models what CronIn / DailyIn / WeeklyIn and a CRON_TZ=
// prefix produce: a boundary rendered in the SCHEDULE's location rather than the
// cursor's. Without it this whole harness is blind to the bug it exists for —
// every other family's Next() preserves the anchor's location, so its boundary
// always shares its cursor's clock face and even a raw lexical comparison
// succeeds. Measured: reverting the predicate to the pre-wave form still reported
// "OK" for all four of the original families.
//
// It is built with time.FixedZone rather than the new *In constructors on purpose:
// this file is compiled against the BASELINE tag too, which does not have them.
var crossZone = time.FixedZone("probe+0530", 5*3600+30*60)

func main() {
	if len(os.Args) < 3 {
		fmt.Fprintln(os.Stderr, "usage: upgradematrix <db> seed|check")
		os.Exit(2)
	}
	db, err := gorm.Open(sqlite.Open(os.Args[1]), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		panic(err)
	}
	st := storage.NewGormStorage(db)
	ctx := context.Background()
	if err := st.Migrate(ctx); err != nil {
		fmt.Printf("MIGRATE FAILED: %v\n", err)
		os.Exit(1)
	}

	switch os.Args[2] {
	case "seed":
		for _, name := range order() {
			s, ok := families()[name]
			if !ok {
				continue
			}
			anchor, err := st.SeedScheduledFire(ctx, name, time.Now())
			if err != nil {
				panic(err)
			}
			// Seed and first fire always go in on the HOST's face, as a released
			// binary would write them.
			//
			// EXCEPT "seeded-only", which is left at the raw seed. That is the state
			// a schedule is in between first registration and its first fire, and it
			// is the one the claim overwrites everywhere else: the seed is written on
			// the HOST's face from time.Now(), while a UTC-pinned family's first
			// boundary arrives on UTC. Every other family here destroys that state
			// immediately by claiming, which is exactly how this harness came to be
			// blind to a whole predicate regression.
			if name == "seeded-only" {
				continue
			}
			boundary := s.Next(anchor)
			won, err := st.ClaimScheduledFire(ctx, name, boundary)
			if err != nil {
				panic(err)
			}
			if !won {
				// The baseline could not claim a boundary genuinely LATER than its
				// own anchor. That is not a harness failure — it is the released
				// version demonstrating the very bug this matrix exists to check, so
				// it is reported rather than hidden or treated as fatal. Measured
				// under TZ=Asia/Tokyo: anchor 11:33+09:00 (02:33 UTC), boundary
				// 09:00Z, six hours later, rejected by the baseline's lexical text
				// comparison.
				//
				// The row is left at the anchor, which is itself a legitimate upgrade
				// fixture (the same shape as "seeded-only"), and the check phase then
				// verifies HEAD can move it.
				fmt.Printf("%-11s BASELINE COULD NOT CLAIM (pre-existing bug) anchor=%s boundary=%s\n",
					name, anchor.Format("15:04:05Z07:00"), boundary.Format("15:04:05Z07:00"))
			}
		}
		var rows []struct{ Name, V string }
		db.Raw(`SELECT name, CAST(last_fire_at AS TEXT) AS v FROM scheduled_fires ORDER BY name`).Scan(&rows)
		for _, r := range rows {
			fmt.Printf("%-8s %s\n", r.Name, r.V)
		}

	case "check":
		stalled := 0
		for _, name := range order() {
			s, ok := families()[name]
			if !ok {
				continue
			}
			cur, found, err := st.GetScheduledFireTime(ctx, name)
			if err != nil || !found {
				fmt.Printf("%-8s NO CURSOR (err=%v)\n", name, err)
				stalled++
				continue
			}
			next := s.Next(cur)
			if name == "crossface" {
				// Same instant, different clock face — the shape a schedule with an
				// explicit location produces against a cursor written on another.
				next = next.In(crossZone)
			}
			won, err := st.ClaimScheduledFire(ctx, name, next)
			if err != nil {
				panic(err)
			}
			status := "OK"
			if !won {
				status = "*** STALLED ***"
				stalled++
			}
			fmt.Printf("%-8s cursor=%s next=%s claimed=%v %s\n",
				name, cur.Format("15:04:05Z07:00"), next.Format("15:04:05Z07:00"), won, status)
		}
		if stalled > 0 {
			os.Exit(1)
		}
	}
}
