package redifu

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strconv"
)

type RowScanner[T SQLItemBlueprint] func(row *sql.Row) (T, error)

type RowsScanner[T SQLItemBlueprint] func(rows *sql.Rows) (T, error)
type RowsScannerWithRelation[T SQLItemBlueprint] func(rows *sql.Rows, relation map[string]Relation) (T, error)

type TimelineSeeder[T SQLItemBlueprint] struct {
	db               *sql.DB
	baseClient       *Base[T]
	paginationClient *Timeline[T]
	scoringField     string
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

	return s.baseClient.Set(item)
}

func (s *TimelineSeeder[T]) SeedPartial(rowQuery string, firstPageQuery string, nextPageQuery string, rowScanner RowScanner[T], rowsScanner RowsScanner[T], queryArgs []interface{}, subtraction int64, lastRandId string, paginateParams []string) error {
	return s.partialSeed(rowQuery, firstPageQuery, nextPageQuery, rowScanner, queryArgs, subtraction, lastRandId, paginateParams, func(rows *sql.Rows) (T, error) {
		return rowsScanner(rows)
	})
}

func (s *TimelineSeeder[T]) SeedPartialWithRelation(rowQuery string, firstPageQuery string, nextPageQuery string, rowScanner RowScanner[T], rowsScannerWithJoin RowsScannerWithRelation[T], queryArgs []interface{}, subtraction int64, lastRandId string, paginateParams []string) error {
	return s.partialSeed(rowQuery, firstPageQuery, nextPageQuery, rowScanner, queryArgs, subtraction, lastRandId, paginateParams, func(rows *sql.Rows) (T, error) {
		return rowsScannerWithJoin(rows, s.paginationClient.relation)
	})
}

func (s *TimelineSeeder[T]) partialSeed(rowQuery string, firstPageQuery string, nextPageQuery string, rowScanner RowScanner[T], queryArgs []interface{}, subtraction int64, lastRandId string, paginateParams []string, scanFunc func(*sql.Rows) (T, error)) error {
	var firstPage bool
	var queryToUse string

	if s.db == nil {
		return NoDatabaseProvided
	}

	if lastRandId == "" {
		firstPage = true
		queryToUse = firstPageQuery
	} else {
		reference, err := s.FindOne(rowQuery, rowScanner, []interface{}{lastRandId})
		if err != nil {
			return DocumentOrReferencesNotFound
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
	for rows.Next() {
		item, err := scanFunc(rows)
		if err != nil {
			log.Printf("rowscanner: %s", err)
			continue
		}

		s.baseClient.Set(item)
		s.paginationClient.IngestItem(item, paginateParams, true)
		counterLoop++
	}

	if firstPage && counterLoop == 0 {
		s.paginationClient.SetBlankPage(paginateParams)
	} else if firstPage && counterLoop > 0 && counterLoop < s.paginationClient.GetItemPerPage() {
		s.paginationClient.SetFirstPage(paginateParams)
	} else if !firstPage && subtraction+counterLoop < s.paginationClient.GetItemPerPage() {
		s.paginationClient.SetLastPage(paginateParams)
	}

	return nil
}

func NewTimelineSeeder[T SQLItemBlueprint](db *sql.DB, baseClient *Base[T], paginateClient *Timeline[T]) *TimelineSeeder[T] {
	return &TimelineSeeder[T]{
		db:               db,
		baseClient:       baseClient,
		paginationClient: paginateClient,
	}
}

type SortedSeeder[T SQLItemBlueprint] struct {
	db           *sql.DB
	baseClient   *Base[T]
	sortedClient *Sorted[T]
	scoringField string
}

func (s *SortedSeeder[T]) Seed(query string, rowsScanner RowsScanner[T], args []interface{}, keyParam []string) error {
	return s.runSeed(query, args, keyParam, func(rows *sql.Rows) (T, error) {
		return rowsScanner(rows)
	})
}

func (s *SortedSeeder[T]) SeedPage(query string, rowsScanner RowsScanner[T], args []interface{}, keyParam []string, page int64, itemPerPage int64) error {
	offset := (page - 1) * itemPerPage
	adjustedQuery := query + "LIMIT " + strconv.FormatInt(page, 10) + " OFFSET " + strconv.FormatInt(offset, 10)
	return s.Seed(adjustedQuery, rowsScanner, args, keyParam)
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
	for rows.Next() {
		item, err := scanFunc(rows)
		if err != nil {
			fmt.Printf("rowscanner: %s", err)
			continue
		}

		s.baseClient.Set(item)
		s.sortedClient.IngestItem(item, keyParam, true)
		counterLoop++
	}

	if counterLoop == 0 {
		s.sortedClient.SetBlankPage(keyParam)
	}

	return nil
}

func NewSortedSeeder[T SQLItemBlueprint](
	db *sql.DB,
	baseClient *Base[T],
	sortedClient *Sorted[T],
) *SortedSeeder[T] {
	return &SortedSeeder[T]{
		db:           db,
		baseClient:   baseClient,
		sortedClient: sortedClient,
	}
}
