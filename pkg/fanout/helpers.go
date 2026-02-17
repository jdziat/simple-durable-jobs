package fanout

// Values extracts values from successful results.
func Values[T any](results []Result[T]) []T {
	values := make([]T, 0, len(results))
	for _, r := range results {
		if r.Err == nil {
			values = append(values, r.Value)
		}
	}
	return values
}

// Partition splits results into successes and failures.
func Partition[T any](results []Result[T]) ([]T, []error) {
	successes := make([]T, 0)
	failures := make([]error, 0)
	for _, r := range results {
		if r.Err == nil {
			successes = append(successes, r.Value)
		} else {
			failures = append(failures, r.Err)
		}
	}
	return successes, failures
}

// AllSucceeded checks if all results succeeded.
func AllSucceeded[T any](results []Result[T]) bool {
	for _, r := range results {
		if r.Err != nil {
			return false
		}
	}
	return true
}

// SuccessCount returns the number of successful results.
func SuccessCount[T any](results []Result[T]) int {
	count := 0
	for _, r := range results {
		if r.Err == nil {
			count++
		}
	}
	return count
}
