package fanout

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValues(t *testing.T) {
	results := []Result[int]{
		{Index: 0, Value: 10},
		{Index: 1, Err: errors.New("failed")},
		{Index: 2, Value: 30},
	}
	values := Values(results)
	assert.Equal(t, []int{10, 30}, values)
}

func TestValues_Empty(t *testing.T) {
	results := []Result[int]{}
	values := Values(results)
	assert.Empty(t, values)
}

func TestValues_AllFailed(t *testing.T) {
	results := []Result[int]{
		{Index: 0, Err: errors.New("err1")},
		{Index: 1, Err: errors.New("err2")},
	}
	values := Values(results)
	assert.Empty(t, values)
}

func TestPartition(t *testing.T) {
	results := []Result[string]{
		{Index: 0, Value: "a"},
		{Index: 1, Err: errors.New("err1")},
		{Index: 2, Value: "c"},
		{Index: 3, Err: errors.New("err2")},
	}
	successes, failures := Partition(results)
	assert.Equal(t, []string{"a", "c"}, successes)
	assert.Len(t, failures, 2)
}

func TestPartition_AllSuccess(t *testing.T) {
	results := []Result[string]{
		{Index: 0, Value: "a"},
		{Index: 1, Value: "b"},
	}
	successes, failures := Partition(results)
	assert.Equal(t, []string{"a", "b"}, successes)
	assert.Empty(t, failures)
}

func TestPartition_AllFailed(t *testing.T) {
	results := []Result[string]{
		{Index: 0, Err: errors.New("err1")},
		{Index: 1, Err: errors.New("err2")},
	}
	successes, failures := Partition(results)
	assert.Empty(t, successes)
	assert.Len(t, failures, 2)
}

func TestAllSucceeded_AllSuccess(t *testing.T) {
	results := []Result[int]{{Value: 1}, {Value: 2}}
	assert.True(t, AllSucceeded(results))
}

func TestAllSucceeded_HasFailure(t *testing.T) {
	results := []Result[int]{{Value: 1}, {Err: errors.New("fail")}}
	assert.False(t, AllSucceeded(results))
}

func TestAllSucceeded_Empty(t *testing.T) {
	results := []Result[int]{}
	assert.True(t, AllSucceeded(results))
}

func TestSuccessCount(t *testing.T) {
	results := []Result[int]{
		{Value: 1},
		{Err: errors.New("fail")},
		{Value: 3},
		{Value: 4},
	}
	assert.Equal(t, 3, SuccessCount(results))
}

func TestSuccessCount_Empty(t *testing.T) {
	results := []Result[int]{}
	assert.Equal(t, 0, SuccessCount(results))
}

func TestSuccessCount_AllFailed(t *testing.T) {
	results := []Result[int]{
		{Err: errors.New("fail1")},
		{Err: errors.New("fail2")},
	}
	assert.Equal(t, 0, SuccessCount(results))
}
