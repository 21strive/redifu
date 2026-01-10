package redifu

import (
	"context"
	"database/sql"
	"errors"
	"github.com/21strive/item"
	"github.com/redis/go-redis/v9"
	"strconv"
	"time"
)

type RowScanner[T SQLItemBlueprint] func(row *sql.Row) (T, error)

type RowsScanner[T SQLItemBlueprint] func(rows *sql.Rows) (T, error)
type RowsScannerWithRelation[T SQLItemBlueprint] func(rows *sql.Rows, relation map[string]Relation) (T, error)

type TimelineSeeder[T SQLItemBlueprint] struct {
	db               *sql.DB
	redis            redis.UniversalClient
	baseClient       *Base[T]
	paginationClient *Timeline[T]
	scoringField     string
}

func NewTimelineSeeder[T SQLItemBlueprint](redis redis.UniversalClient, db *sql.DB, baseClient *Base[T], paginateClient *Timeline[T]) *TimelineSeeder[T] {
	return &TimelineSeeder[T]{
		redis:            redis,
		db:               db,
		baseClient:       baseClient,
		paginationClient: paginateClient,
	}
}

func (s *TimelineSeeder[T]) SetSortingReference(scoringField string) {
	s.scoringField = scoringField
}

func (s *TimelineSeeder[T]) FindOne(rowQuery string, rowScanner RowScanner[T], queryArgs []interface{}) (T, error) {
	var item T
	if s.db == nil {
		return item, NoDatabaseProvided
	}

	if rowQuery == "" || rowScanner == nil {
		return item, QueryOrScannerNotConfigured
	}

	row := s.db.QueryRowContext(context.TODO(), rowQuery, queryArgs...)

	item, err := rowScanner(row)
	if err != nil {
		if err == sql.ErrNoRows {
			return item, DocumentOrReferencesNotFound
		}
		return item, err
	}

	return item, nil
}

func (s *TimelineSeeder[T]) SeedOne(rowQuery string, rowScanner RowScanner[T], queryArgs []interface{}) error {
	item, err := s.FindOne(rowQuery, rowScanner, queryArgs)
	if err != nil {
		return err
	}

	pipeCtx := context.Background()
	pipe := s.redis.Pipeline()
	s.baseClient.Set(pipe, pipeCtx, item)
	_, err = pipe.Exec(pipeCtx)

	return err
}

func (s *TimelineSeeder[T]) SeedPartial(rowQuery string, firstPageQuery string, nextPageQuery string, queryArgs []interface{}, subtraction int64, lastRandId string, paginateParams []string, rowScanner RowScanner[T], rowsScanner RowsScanner[T]) error {
	return s.partialSeed(rowQuery, firstPageQuery, nextPageQuery, queryArgs, subtraction, lastRandId, paginateParams, rowScanner, func(rows *sql.Rows) (T, error) {
		return rowsScanner(rows)
	})
}

func (s *TimelineSeeder[T]) SeedPartialWithRelation(rowQuery string, firstPageQuery string, nextPageQuery string, queryArgs []interface{}, subtraction int64, lastRandId string, paginateParams []string, rowScanner RowScanner[T], rowsScannerWithJoin RowsScannerWithRelation[T]) error {
	return s.partialSeed(rowQuery, firstPageQuery, nextPageQuery, queryArgs, subtraction, lastRandId, paginateParams, rowScanner, func(rows *sql.Rows) (T, error) {
		return rowsScannerWithJoin(rows, s.paginationClient.relation)
	})
}

func (s *TimelineSeeder[T]) partialSeed(rowQuery string, firstPageQuery string, nextPageQuery string, queryArgs []interface{}, subtraction int64, lastRandId string, paginateParams []string, rowScanner RowScanner[T], scanFunc func(*sql.Rows) (T, error)) error {
	var firstPage bool
	var queryToUse string

	if s.db == nil {
		return NoDatabaseProvided
	}

	if lastRandId == "" {
		firstPage = true
		queryToUse = firstPageQuery
	} else {
		reference, errFindReference := s.FindOne(rowQuery, rowScanner, []interface{}{lastRandId})
		if errFindReference != nil {
			if errors.Is(errFindReference, sql.ErrNoRows) {
				return DocumentOrReferencesNotFound
			}
			return errFindReference
		} else {
			firstPage = false
			queryToUse = nextPageQuery
			if s.scoringField != "" {
				queryArgs = append(queryArgs, getFieldValue(reference, s.scoringField))
			} else {
				queryArgs = append(queryArgs, reference.GetCreatedAt())
			}
		}
	}

	var limit int64
	if subtraction > 0 {
		limit = s.paginationClient.GetItemPerPage() - subtraction
	} else {
		limit = s.paginationClient.GetItemPerPage()
	}
	queryToUse = queryToUse + ` LIMIT ` + strconv.FormatInt(limit, 10)

	rows, err := s.db.QueryContext(context.TODO(), queryToUse, queryArgs...)
	if err != nil {
		return err
	}
	defer rows.Close()

	var counterLoop int64 = 0

	// pipeline preparation
	pipeCtx := context.Background()
	pipeline := s.redis.Pipeline()

	for rows.Next() {
		item, err := scanFunc(rows)
		if err != nil {
			return err
		}

		s.baseClient.Set(pipeline, pipeCtx, item)
		s.paginationClient.IngestItem(pipeline, pipeCtx, item, paginateParams, true)
		counterLoop++
	}
	if err = rows.Err(); err != nil {
		return err
	}

	if firstPage && counterLoop == 0 {
		s.paginationClient.SetBlankPage(pipeline, pipeCtx, paginateParams)
	} else if firstPage && counterLoop > 0 && counterLoop < s.paginationClient.GetItemPerPage() {
		s.paginationClient.SetFirstPage(pipeline, pipeCtx, paginateParams)
	} else if !firstPage && subtraction+counterLoop < s.paginationClient.GetItemPerPage() {
		s.paginationClient.SetLastPage(pipeline, pipeCtx, paginateParams)
	}

	if firstPage {
		s.paginationClient.SetExpiration(pipeline, pipeCtx, paginateParams)
	}

	// pipeline execution
	_, err = pipeline.Exec(context.TODO())
	if err != nil {
		return err
	}

	return nil
}

type timelineSeedBuilder[T item.Blueprint] struct {
	timelineSeeder *Timeline[T]
	queryArgs      []interface{}
	keyParams      []string
}

type SortedSeeder[T SQLItemBlueprint] struct {
	db           *sql.DB
	redis        redis.UniversalClient
	baseClient   *Base[T]
	sortedClient *Sorted[T]
}

func NewSortedSeeder[T SQLItemBlueprint](
	redis redis.UniversalClient,
	db *sql.DB,
	baseClient *Base[T],
	sortedClient *Sorted[T],
) *SortedSeeder[T] {
	return &SortedSeeder[T]{
		redis:        redis,
		db:           db,
		baseClient:   baseClient,
		sortedClient: sortedClient,
	}
}

func (s *SortedSeeder[T]) Seed(query string, rowsScanner RowsScanner[T], args []interface{}, keyParam []string) error {
	return s.runSeed(query, args, keyParam, func(rows *sql.Rows) (T, error) {
		return rowsScanner(rows)
	})
}

func (s *SortedSeeder[T]) SeedWithRelation(query string, rowsScanner RowsScannerWithRelation[T], args []interface{}, keyParam []string) error {
	return s.runSeed(query, args, keyParam, func(rows *sql.Rows) (T, error) {
		return rowsScanner(rows, s.sortedClient.relation)
	})
}

func (s *SortedSeeder[T]) runSeed(
	query string,
	args []interface{},
	keyParam []string,
	scanFunc func(*sql.Rows) (T, error),
) error {
	if s.db == nil {
		return NoDatabaseProvided
	}

	if scanFunc == nil {
		return QueryOrScannerNotConfigured
	}

	rows, err := s.db.QueryContext(context.TODO(), query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	var counterLoop int64

	// pipeline preparation
	pipeCtx := context.Background()
	pipeline := s.redis.Pipeline()

	for rows.Next() {
		item, errScan := scanFunc(rows)
		if errScan != nil {
			return errScan
		}

		s.baseClient.Set(pipeline, pipeCtx, item)
		s.sortedClient.IngestItem(pipeline, pipeCtx, item, keyParam, true)
		counterLoop++
	}
	if err = rows.Err(); err != nil {
		return err
	}

	if counterLoop == 0 {
		s.sortedClient.SetBlankPage(pipeline, pipeCtx, keyParam)
	} else {
		s.sortedClient.SetExpiration(pipeline, pipeCtx, keyParam)
	}

	_, errPipe := pipeline.Exec(pipeCtx)
	return errPipe
}

type PageSeeder[T SQLItemBlueprint] struct {
	redis      redis.UniversalClient
	db         *sql.DB
	baseClient *Base[T]
	pageClient *Page[T]
}

func NewPageSeeder[T SQLItemBlueprint](
	redisClient redis.UniversalClient,
	db *sql.DB,
	baseClient *Base[T],
	pageClient *Page[T],
) *PageSeeder[T] {
	return &PageSeeder[T]{
		redis:      redisClient,
		db:         db,
		baseClient: baseClient,
		pageClient: pageClient,
	}
}

func (p *PageSeeder[T]) Seed(query string, page int64, rowsScanner RowsScanner[T], args []interface{}, keyParam []string) error {
	offset := (page - 1) * p.pageClient.itemPerPage
	adjustedQuery := query + " LIMIT " + strconv.FormatInt(p.pageClient.itemPerPage, 10) + " OFFSET " + strconv.FormatInt(offset, 10)
	return p.runSeed(adjustedQuery, args, page, keyParam, func(rows *sql.Rows) (T, error) { return rowsScanner(rows) })
}

func (p *PageSeeder[T]) SeedWithRelation(query string, page int64, rowsScanner RowsScannerWithRelation[T], args []interface{}, keyParam []string) error {
	offset := (page - 1) * p.pageClient.itemPerPage
	adjustedQuery := query + " LIMIT " + strconv.FormatInt(p.pageClient.itemPerPage, 10) + " OFFSET " + strconv.FormatInt(offset, 10)
	return p.runSeed(adjustedQuery, args, page, keyParam, func(rows *sql.Rows) (T, error) {
		return rowsScanner(rows, p.pageClient.relation)
	})
}

func (p *PageSeeder[T]) runSeed(
	query string,
	args []interface{},
	page int64,
	keyParam []string,
	scanFunc func(*sql.Rows) (T, error),
) error {
	if p.db == nil {
		return NoDatabaseProvided
	}

	if scanFunc == nil {
		return QueryOrScannerNotConfigured
	}

	rows, err := p.db.QueryContext(context.TODO(), query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	var counterLoop int64

	// pipeline preparation
	pipeCtx := context.Background()
	pipeline := p.redis.Pipeline()

	for rows.Next() {
		item, errScan := scanFunc(rows)
		if errScan != nil {
			return errScan
		}

		p.baseClient.Set(pipeline, pipeCtx, item)
		p.pageClient.IngestItem(pipeline, pipeCtx, item, page, keyParam)
		counterLoop++
	}
	if err = rows.Err(); err != nil {
		return err
	}

	if counterLoop == 0 {
		p.pageClient.SetBlankPage(pipeline, pipeCtx, page, keyParam)
	} else {
		p.pageClient.SetExpiration(pipeline, pipeCtx, page, keyParam)
	}

	p.pageClient.AddPage(pipeline, pipeCtx, page, keyParam)

	_, errPipe := pipeline.Exec(pipeCtx)
	return errPipe
}

type TimeSeriesSeeder[T SQLItemBlueprint] struct {
	db               *sql.DB
	redis            redis.UniversalClient
	baseClient       *Base[T]
	timeSeriesClient *TimeSeries[T]
}

func NewTimeSeriesSeeder[T SQLItemBlueprint](
	redisClient redis.UniversalClient,
	db *sql.DB,
	baseClient *Base[T],
	timeSeriesClient *TimeSeries[T],
) *TimeSeriesSeeder[T] {
	return &TimeSeriesSeeder[T]{
		redis:            redisClient,
		db:               db,
		baseClient:       baseClient,
		timeSeriesClient: timeSeriesClient,
	}
}

// let user append both lowerbound and upperbound as argument.
func (s *TimeSeriesSeeder[T]) Seed(query string, queryArgs []interface{}, lowerbound time.Time, upperbound time.Time, keyParam []string, rowsScanner RowsScanner[T]) error {
	gaps, errFindGaps := s.timeSeriesClient.FindGap(lowerbound, upperbound, keyParam)
	if errFindGaps != nil {
		return errFindGaps
	}

	if gaps != nil {
		for _, gap := range gaps {
			lowerGap := time.UnixMilli(gap[0]).UTC()
			upperGap := time.UnixMilli(gap[1]).UTC()

			return s.runSeed(query, queryArgs, lowerGap, upperGap, keyParam, rowsScanner)
		}
	}

	return s.runSeed(query, queryArgs, lowerbound, upperbound, keyParam, rowsScanner)
}

func (s *TimeSeriesSeeder[T]) runSeed(query string, queryArgs []interface{}, lowerGap time.Time, upperGap time.Time, keyParam []string, rowsScanner RowsScanner[T]) error {
	queryArgs = append(queryArgs, lowerGap, upperGap)

	rows, err := s.db.QueryContext(context.TODO(), query, queryArgs...)
	if err != nil {
		return err
	}
	defer rows.Close()

	pipeCtx := context.Background()
	pipeline := s.redis.Pipeline()

	isSortedExists := s.timeSeriesClient.Count(keyParam) > 0
	var counterLoop int64
	for rows.Next() {
		item, errScan := rowsScanner(rows)
		if errScan != nil {
			return errScan
		}

		s.baseClient.Set(pipeline, pipeCtx, item)
		err := s.timeSeriesClient.IngestItem(pipeline, pipeCtx, item, keyParam)
		if err != nil {
			return err
		}
		counterLoop++
	}

	// only set expiration in the first seed attempt
	if counterLoop > 0 && !isSortedExists {
		s.timeSeriesClient.SetExpiration(pipeline, pipeCtx, keyParam)
	}

	s.timeSeriesClient.AddPage(pipeline, pipeCtx, lowerGap, upperGap, keyParam)
	_, errPipe := pipeline.Exec(pipeCtx)
	return errPipe
}
