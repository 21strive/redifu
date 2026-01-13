package redifu

import (
	"context"
	"database/sql"
	"errors"
	"github.com/redis/go-redis/v9"
	"strconv"
	"time"
)

type RowScanner[T SQLItemBlueprint] func(row *sql.Row) (T, error)

type RowsScanner[T SQLItemBlueprint] func(rows *sql.Rows) (T, error)
type RowsScannerWithRelation[T SQLItemBlueprint] func(ctx context.Context, rows *sql.Rows, relation map[string]Relation) (T, error)

type TimelineSeeder[T SQLItemBlueprint] struct {
	db             *sql.DB
	queryBuilder   *Builder
	redis          redis.UniversalClient
	baseClient     *Base[T]
	timelineClient *Timeline[T]
	scoringField   string
}

func NewTimelineSeeder[T SQLItemBlueprint](redis redis.UniversalClient, db *sql.DB, baseClient *Base[T], timelineClient *Timeline[T]) *TimelineSeeder[T] {
	return &TimelineSeeder[T]{
		redis:          redis,
		db:             db,
		baseClient:     baseClient,
		timelineClient: timelineClient,
	}
}

func (s *TimelineSeeder[T]) SetSortingReference(scoringField string) {
	s.scoringField = scoringField
}

func (s *TimelineSeeder[T]) findOne(ctx context.Context, rowQuery string, rowScanner RowScanner[T], randid string) (T, error) {
	var item T
	if s.db == nil {
		return item, NoDatabaseProvided
	}

	if rowQuery == "" || rowScanner == nil {
		return item, QueryOrScannerNotConfigured
	}

	row := s.db.QueryRowContext(ctx, rowQuery, randid)

	item, err := rowScanner(row)
	if err != nil {
		if err == sql.ErrNoRows {
			return item, DocumentOrReferencesNotFound
		}
		return item, err
	}

	return item, nil
}

func (s *TimelineSeeder[T]) Seed(
	ctx context.Context,
	subtraction int64,
	lastRandId string,
	queryBuilder *Builder,
) *timelineSeedBuilder[T] {
	return &timelineSeedBuilder[T]{
		mainCtx:        ctx,
		timelineSeeder: s,
		subtraction:    subtraction,
		lastRandId:     lastRandId,
		queryBuilder:   queryBuilder,
	}
}

func (s *TimelineSeeder[T]) runSeed(
	ctx context.Context,
	rowQuery string,
	firstPageQuery string,
	nextPageQuery string,
	queryArgs []interface{},
	subtraction int64,
	lastRandId string,
	rowScanner RowScanner[T],
	scanFunc func(*sql.Rows) (T, error),
	keyParams ...string) error {
	var firstPage bool
	var queryToUse string

	if s.db == nil {
		return NoDatabaseProvided
	}

	if lastRandId == "" {
		firstPage = true
		queryToUse = firstPageQuery
	} else {
		reference, errFindReference := s.findOne(ctx, rowQuery, rowScanner, lastRandId)
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
		limit = s.timelineClient.GetItemPerPage() - subtraction
	} else {
		limit = s.timelineClient.GetItemPerPage()
	}
	queryToUse = queryToUse + ` LIMIT ` + strconv.FormatInt(limit, 10)

	rows, err := s.db.QueryContext(ctx, queryToUse, queryArgs...)
	if err != nil {
		return err
	}
	defer rows.Close()

	var counterLoop int64 = 0

	// pipeline preparation
	pipeline := s.redis.Pipeline()

	for rows.Next() {
		item, err := scanFunc(rows)
		if err != nil {
			return err
		}

		s.baseClient.Set(ctx, pipeline, item)
		s.timelineClient.IngestItem(ctx, pipeline, item, true, keyParams...)
		counterLoop++
	}
	if err = rows.Err(); err != nil {
		return err
	}

	if firstPage && counterLoop == 0 {
		s.timelineClient.SetBlankPage(ctx, pipeline, keyParams...)
	} else if firstPage && counterLoop > 0 && counterLoop < s.timelineClient.GetItemPerPage() {
		s.timelineClient.SetFirstPage(ctx, pipeline, keyParams...)
	} else if !firstPage && subtraction+counterLoop < s.timelineClient.GetItemPerPage() {
		s.timelineClient.SetLastPage(ctx, pipeline, keyParams...)
	}

	if firstPage {
		s.timelineClient.SetExpiration(ctx, pipeline, keyParams...)
	}

	// pipeline execution
	_, err = pipeline.Exec(context.TODO())
	if err != nil {
		return err
	}

	return nil
}

type timelineSeedBuilder[T SQLItemBlueprint] struct {
	mainCtx        context.Context
	timelineSeeder *TimelineSeeder[T]
	queryBuilder   *Builder
	subtraction    int64
	lastRandId     string
	queryArgs      []interface{}
	keyParams      []string
}

func (t *timelineSeedBuilder[T]) WithQueryArgs(queryArgs ...interface{}) *timelineSeedBuilder[T] {
	t.queryArgs = queryArgs
	return t
}

func (t *timelineSeedBuilder[T]) WithParams(keyParams ...string) *timelineSeedBuilder[T] {
	t.keyParams = keyParams
	return t
}

func (t *timelineSeedBuilder[T]) Exec(rowScanner RowScanner[T], rowsScanner RowsScanner[T]) error {
	scanFunc := func(rows *sql.Rows) (T, error) {
		return rowsScanner(rows)
	}

	return t.timelineSeeder.runSeed(
		t.mainCtx,
		t.queryBuilder.Row("randid"),
		t.queryBuilder.Base(),
		t.queryBuilder.WithCursor(),
		t.queryArgs,
		t.subtraction,
		t.lastRandId,
		rowScanner,
		scanFunc,
		t.keyParams...)
}

func (t *timelineSeedBuilder[T]) ExecWithRelation(rowScanner RowScanner[T], rowsScanner RowsScannerWithRelation[T]) error {
	scanFunc := func(rows *sql.Rows) (T, error) {
		return rowsScanner(t.mainCtx, rows, t.timelineSeeder.timelineClient.relation)
	}

	return t.timelineSeeder.runSeed(
		t.mainCtx,
		t.queryBuilder.Row("randid"),
		t.queryBuilder.Base(),
		t.queryBuilder.WithCursor(),
		t.queryArgs,
		t.subtraction,
		t.lastRandId,
		rowScanner,
		scanFunc,
		t.keyParams...)
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

func (s *SortedSeeder[T]) Seed(ctx context.Context, queryBuilder *Builder) *sortedSeedBuilder[T] {
	return &sortedSeedBuilder[T]{
		mainCtx:      ctx,
		sortedSeeder: s,
		query:        queryBuilder.Base(),
	}
}

func (s *SortedSeeder[T]) runSeed(
	ctx context.Context,
	query string,
	args []interface{},
	scanFunc func(*sql.Rows) (T, error),
	keyParams ...string,
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
	pipeline := s.redis.Pipeline()

	for rows.Next() {
		item, errScan := scanFunc(rows)
		if errScan != nil {
			return errScan
		}

		s.baseClient.Set(ctx, pipeline, item)
		s.sortedClient.IngestItem(ctx, pipeline, item, true, keyParams...)
		counterLoop++
	}
	if err = rows.Err(); err != nil {
		return err
	}

	if counterLoop == 0 {
		s.sortedClient.SetBlankPage(ctx, pipeline, keyParams...)
	} else {
		s.sortedClient.SetExpiration(ctx, pipeline, keyParams...)
	}

	_, errPipe := pipeline.Exec(ctx)
	return errPipe
}

type sortedSeedBuilder[T SQLItemBlueprint] struct {
	mainCtx      context.Context
	sortedSeeder *SortedSeeder[T]
	query        string
	queryArgs    []interface{}
	keyParams    []string
}

func (t *sortedSeedBuilder[T]) WithQueryArgs(queryArgs ...interface{}) *sortedSeedBuilder[T] {
	t.queryArgs = queryArgs
	return t
}

func (t *sortedSeedBuilder[T]) WithParams(keyParams ...string) *sortedSeedBuilder[T] {
	t.keyParams = keyParams
	return t
}

func (t *sortedSeedBuilder[T]) Exec(rowsScanner RowsScanner[T]) error {
	scanFunc := func(rows *sql.Rows) (T, error) {
		return rowsScanner(rows)
	}

	return t.sortedSeeder.runSeed(
		t.mainCtx,
		t.query,
		t.queryArgs,
		scanFunc,
		t.keyParams...,
	)
}

func (t *sortedSeedBuilder[T]) ExecWithRelation(rowsScanner RowsScannerWithRelation[T]) error {
	scanFunc := func(rows *sql.Rows) (T, error) {
		return rowsScanner(t.mainCtx, rows, t.sortedSeeder.sortedClient.relation)
	}

	return t.sortedSeeder.runSeed(
		t.mainCtx,
		t.query,
		t.queryArgs,
		scanFunc,
		t.keyParams...,
	)
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

func (p *PageSeeder[T]) Seed(ctx context.Context, page int64, query *Builder) *pageSeederBuilder[T] {
	offset := (page - 1) * p.pageClient.itemPerPage
	adjustedQuery := query.Base() + " LIMIT " + strconv.FormatInt(p.pageClient.itemPerPage, 10) + " OFFSET " + strconv.FormatInt(offset, 10)

	return &pageSeederBuilder[T]{
		mainCtx:       ctx,
		pageSeeder:    p,
		page:          page,
		adjustedQuery: adjustedQuery,
	}
}

func (p *PageSeeder[T]) runSeed(
	ctx context.Context,
	query string,
	args []interface{},
	page int64,
	scanFunc func(*sql.Rows) (T, error),
	keyParams ...string,
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

		p.baseClient.Set(ctx, pipeline, item)
		p.pageClient.IngestItem(ctx, pipeline, item, page, keyParams...)
		counterLoop++
	}
	if err = rows.Err(); err != nil {
		return err
	}

	if counterLoop == 0 {
		p.pageClient.SetBlankPage(ctx, pipeline, page, keyParams...)
	} else {
		p.pageClient.SetExpiration(ctx, pipeline, page, keyParams...)
	}

	p.pageClient.AddPage(ctx, pipeline, page, keyParams...)

	_, errPipe := pipeline.Exec(pipeCtx)
	return errPipe
}

type pageSeederBuilder[T SQLItemBlueprint] struct {
	mainCtx       context.Context
	pageSeeder    *PageSeeder[T]
	page          int64
	adjustedQuery string
	queryArgs     []interface{}
	keyParams     []string
}

func (t *pageSeederBuilder[T]) WithQueryArgs(queryArgs ...interface{}) *pageSeederBuilder[T] {
	t.queryArgs = queryArgs
	return t
}

func (t *pageSeederBuilder[T]) WithParams(keyParams ...string) *pageSeederBuilder[T] {
	t.keyParams = keyParams
	return t
}

func (t *pageSeederBuilder[T]) Exec(rowsScanner RowsScanner[T]) error {
	scanFunc := func(rows *sql.Rows) (T, error) {
		return rowsScanner(rows)
	}

	return t.pageSeeder.runSeed(
		t.mainCtx,
		t.adjustedQuery,
		t.queryArgs,
		t.page,
		scanFunc,
		t.keyParams...)
}

func (t *pageSeederBuilder[T]) ExecWithRelation(rowsScanner RowsScannerWithRelation[T]) error {
	scanFunc := func(rows *sql.Rows) (T, error) {
		return rowsScanner(t.mainCtx, rows, t.pageSeeder.pageClient.relation)
	}

	return t.pageSeeder.runSeed(
		t.mainCtx,
		t.adjustedQuery,
		t.queryArgs,
		t.page,
		scanFunc,
		t.keyParams...)
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

func (s *TimeSeriesSeeder[T]) Seed(
	ctx context.Context,
	query *Builder,
	lowerbound time.Time,
	upperbound time.Time) *timeSeriesBuilder[T] {
	return &timeSeriesBuilder[T]{
		mainCtx:          ctx,
		timeSeriesSeeder: s,
		query:            query.Base(),
		lowerBound:       lowerbound,
		upperBound:       upperbound,
	}
}

func (s *TimeSeriesSeeder[T]) runSeed(
	ctx context.Context,
	query string,
	queryArgs []interface{},
	lowerGap time.Time,
	upperGap time.Time,
	scanFunc func(*sql.Rows) (T, error),
	keyParams ...string) error {
	queryArgs = append(queryArgs, lowerGap, upperGap)

	rows, err := s.db.QueryContext(context.TODO(), query, queryArgs...)
	if err != nil {
		return err
	}
	defer rows.Close()

	pipeCtx := context.Background()
	pipeline := s.redis.Pipeline()

	isSortedExists := s.timeSeriesClient.Count(ctx, keyParams...) > 0
	var counterLoop int64
	for rows.Next() {
		item, errScan := scanFunc(rows)
		if errScan != nil {
			return errScan
		}

		s.baseClient.Set(ctx, pipeline, item)
		err := s.timeSeriesClient.IngestItem(ctx, pipeline, item, keyParams...)
		if err != nil {
			return err
		}
		counterLoop++
	}

	// only set expiration in the first seed attempt
	if counterLoop > 0 && !isSortedExists {
		s.timeSeriesClient.SetExpiration(ctx, pipeline, keyParams...)
	}

	s.timeSeriesClient.AddPage(ctx, pipeline, lowerGap, upperGap, keyParams...)
	_, errPipe := pipeline.Exec(pipeCtx)
	return errPipe
}

type timeSeriesBuilder[T SQLItemBlueprint] struct {
	mainCtx          context.Context
	timeSeriesSeeder *TimeSeriesSeeder[T]
	query            string
	queryArgs        []interface{}
	keyParams        []string
	lowerBound       time.Time
	upperBound       time.Time
}

func (t *timeSeriesBuilder[T]) WithQueryArgs(queryArgs ...interface{}) *timeSeriesBuilder[T] {
	t.queryArgs = queryArgs
	return t
}

func (t *timeSeriesBuilder[T]) WithParams(keyParams ...string) *timeSeriesBuilder[T] {
	t.keyParams = keyParams
	return t
}

func (t *timeSeriesBuilder[T]) ValidateRange() error {
	gaps, errFindGaps := t.timeSeriesSeeder.timeSeriesClient.FindGap(
		t.mainCtx,
		t.lowerBound, t.upperBound, t.keyParams...)
	if errFindGaps != nil {
		return errFindGaps
	}

	if gaps != nil {
		for _, gap := range gaps {
			t.lowerBound = time.UnixMilli(gap[0]).UTC()
			t.upperBound = time.UnixMilli(gap[1]).UTC()
		}
	}

	return nil
}

func (t *timeSeriesBuilder[T]) Exec(rowsScanner RowsScanner[T]) error {
	scanFunc := func(rows *sql.Rows) (T, error) {
		return rowsScanner(rows)
	}

	t.ValidateRange()
	return t.timeSeriesSeeder.runSeed(
		t.mainCtx,
		t.query,
		t.queryArgs,
		t.lowerBound,
		t.upperBound, scanFunc, t.keyParams...)
}

func (t *timeSeriesBuilder[T]) ExecWithRelation(rowsScanner RowsScannerWithRelation[T]) error {
	scanFunc := func(rows *sql.Rows) (T, error) {
		return rowsScanner(t.mainCtx, rows, t.timeSeriesSeeder.timeSeriesClient.sorted.relation)
	}

	t.ValidateRange()
	return t.timeSeriesSeeder.runSeed(
		t.mainCtx,
		t.query,
		t.queryArgs,
		t.lowerBound,
		t.upperBound, scanFunc, t.keyParams...)
}
