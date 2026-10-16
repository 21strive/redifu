package redifu

import (
	"context"
	"os"
	"strconv"
	"testing"

	"github.com/redis/go-redis/v9"
)

// clientFactory hands a test an empty Redis to work against. The scenarios that exercise
// redifu's Lua scripts take one, so that they run on miniredis with every `go test` and
// on a real server on request.
type clientFactory func(t *testing.T) redis.UniversalClient

// miniClient is the default: a fresh in-memory miniredis per test.
func miniClient(t *testing.T) redis.UniversalClient {
	t.Helper()
	_, client := newTestRedis(t)
	return client
}

// realClient connects to the server named by REDIFU_REDIS_ADDR and flushes the database
// named by REDIFU_REDIS_DB (15 when unset) before handing it over. Without the variable
// the test is skipped.
//
// miniredis runs scripts on gopher-lua, which differs from the Lua 5.1 inside Redis
// where it matters here: Redis compares Lua strings with strcoll, following the server's
// LC_COLLATE. Run the suite against a real server — ideally one started under a locale
// other than C — whenever a script changes:
//
//	LC_ALL=en_US.utf8 redis-server --port 6399 --save '' &
//	REDIFU_REDIS_ADDR=127.0.0.1:6399 go test -run TestRealRedis -v ./...
//
// Never point it at a server holding data you care about: every scenario flushes that
// database.
func realClient(t *testing.T) redis.UniversalClient {
	t.Helper()

	addr := os.Getenv("REDIFU_REDIS_ADDR")
	if addr == "" {
		t.Skip("REDIFU_REDIS_ADDR is not set")
	}
	db := 15
	if raw := os.Getenv("REDIFU_REDIS_DB"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil {
			t.Fatalf("REDIFU_REDIS_DB: %v", err)
		}
		db = parsed
	}

	client := redis.NewClient(&redis.Options{Addr: addr, DB: db})
	t.Cleanup(func() { _ = client.Close() })
	if err := client.FlushDB(context.Background()).Err(); err != nil {
		t.Fatalf("FLUSHDB on %s/%d: %v", addr, db, err)
	}
	return client
}

// TestRealRedis runs the script-dependent scenarios against a real server.
func TestRealRedis(t *testing.T) {
	if os.Getenv("REDIFU_REDIS_ADDR") == "" {
		t.Skip("REDIFU_REDIS_ADDR is not set")
	}

	scenarios := map[string]func(*testing.T, clientFactory){
		"FetchFromWalksEveryPageOnce":                        scenarioFetchFromWalksEveryPageOnce,
		"FetchFromOrdersTiesByMemberByteWise":                scenarioFetchFromOrdersTiesByMemberByteWise,
		"FetchFromContinuesAfterARemovedCursor":              scenarioFetchFromContinuesAfterARemovedCursor,
		"FetchFromInsideATieGroupWithoutItsMember":           scenarioFetchFromInsideATieGroupWithoutItsMember,
		"FetchFromWithAScoreOnlyCursor":                      scenarioFetchFromWithAScoreOnlyCursor,
		"FetchFromAtTheEndOfTheIndex":                        scenarioFetchFromAtTheEndOfTheIndex,
		"NextCursorAgreesAcrossFetchPaths":                   scenarioNextCursorAgreesAcrossFetchPaths,
		"FetchFromReportsDanglingMembersAndHasMoreFromIndex": scenarioFetchFromReportsDanglingMembersAndHasMoreFromTheIndex,
		"FetchFromWalksAThousandTies":                        scenarioFetchFromWalksAThousandTies,
		"TailIsTheLastMemberInTheTimelinesOrder":             scenarioTailIsTheLastMemberInTheTimelinesOrder,
		"TailOfAnEmptyIndex":                                 scenarioTailOfAnEmptyIndex,
		"DocumentedTimelineFlowWalksEveryRowOnce":            scenarioDocumentedTimelineFlowWalksEveryRowOnce,
		"DocumentedTimelineFlowWithRandIdCursors":            scenarioDocumentedTimelineFlowWithRandIdCursors,
		"DocumentedTimelineFlowWithANewItemMidScroll":        scenarioDocumentedTimelineFlowWithANewItemMidScroll,
		"DocumentedTimelineFlowSurvivesARemovalMidScroll":    scenarioDocumentedTimelineFlowSurvivesARemovalMidScroll,
		"DocumentedTimelineFlowRecoversAnIndexLostMidScroll": scenarioDocumentedTimelineFlowRecoversAnIndexLostMidScroll,
		"ReachedEnd":                                      scenarioReachedEnd,
		"TimelineIngestDescending":                        scenarioTimelineIngestDescending,
		"TimelineIngestAscending":                         scenarioTimelineIngestAscending,
		"TimelineIngestIntoAnEmptyIndex":                  scenarioTimelineIngestIntoAnEmptyIndex,
		"AnOldItemJoinsAFullySeededTimeline":              scenarioAnOldItemJoinsAFullySeededTimeline,
		"AnOldItemWaitsForTheTopUpOfAPartialTimeline":     scenarioAnOldItemWaitsForTheTopUpOfAPartialTimeline,
		"SortedRankRange":                                 scenarioSortedRankRange,
		"SortedScoreRangeWithLimit":                       scenarioSortedScoreRangeWithLimit,
		"SortedExecReportCountsDanglingMembers":           scenarioSortedExecReportCountsDanglingMembers,
		"SortedExecReportCountsOnlyTheRangeItRead":        scenarioSortedExecReportCountsOnlyTheRangeItRead,
		"TimeSeriesTakesTheFirstItemOfAnEmptySeededRange": scenarioTimeSeriesTakesTheFirstItemOfAnEmptySeededRange,
		"TimeSeriesAddSegmentAlignsTheTwoTTLs":            scenarioTimeSeriesAddSegmentAlignsTheTwoTTLs,
	}

	for name, scenario := range scenarios {
		t.Run(name, func(t *testing.T) { scenario(t, realClient) })
	}
}
