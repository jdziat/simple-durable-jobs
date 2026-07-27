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
		"every":  schedule.Every(time.Hour),
		"daily":  schedule.Daily(9, 0),
		"weekly": schedule.Weekly(time.Monday, 9, 0),
	}
	if c, err := schedule.Cron("0 * * * *"); err == nil {
		f["cron"] = c
	}
	return f
}

func order() []string { return []string{"cron", "daily", "every", "weekly"} }

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
			if _, err := st.ClaimScheduledFire(ctx, name, s.Next(anchor)); err != nil {
				panic(err)
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
