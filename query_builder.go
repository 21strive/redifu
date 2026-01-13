package redifu

import (
	"fmt"
	"strings"
)

const (
	Between          = "between"
	LowerThan        = "lt"
	GreaterThan      = "gt"
	LowerThanEqual   = "lte"
	GreaterThanEqual = "gte"
	Equal            = "eq"
)

type Builder struct {
	table      string
	alias      string
	selectCols string
	joins      []string
	orderCol   string
	orderDir   string
	conditions []CursorCondition
}

type CursorCondition struct {
	Column   string
	Operator string // "between", "lt", "gt", "lte", "gte", "<", ">", "<=", ">="
}

func NewQuery(table string, alias ...string) *Builder {
	b := &Builder{
		table:      table,
		orderDir:   "DESC",
		joins:      []string{},
		conditions: []CursorCondition{},
	}

	if len(alias) > 0 {
		b.alias = alias[0]
	}

	return b
}

func (b *Builder) Select(cols string) *Builder {
	b.selectCols = cols
	return b
}

func (b *Builder) LeftJoin(table string, aliasOrCondition string, condition ...string) *Builder {
	var join string
	if len(condition) > 0 {
		join = fmt.Sprintf("LEFT JOIN %s %s ON %s", table, aliasOrCondition, condition[0])
	} else {
		join = fmt.Sprintf("LEFT JOIN %s ON %s", table, aliasOrCondition)
	}
	b.joins = append(b.joins, join)
	return b
}

func (b *Builder) InnerJoin(table string, aliasOrCondition string, condition ...string) *Builder {
	var join string
	if len(condition) > 0 {
		join = fmt.Sprintf("INNER JOIN %s %s ON %s", table, aliasOrCondition, condition[0])
	} else {
		join = fmt.Sprintf("INNER JOIN %s ON %s", table, aliasOrCondition)
	}
	b.joins = append(b.joins, join)
	return b
}

func (b *Builder) OrderBy(col, direction string) *Builder {
	b.orderCol = col
	b.orderDir = direction
	return b
}

func (b *Builder) Where(column, operator string) *Builder {
	b.conditions = append(b.conditions, CursorCondition{
		Column:   column,
		Operator: operator,
	})
	return b
}

func (b *Builder) tableRef() string {
	if b.alias != "" {
		return fmt.Sprintf("%s %s", b.table, b.alias)
	}
	return b.table
}

func (b *Builder) Row(idCol string) string {
	return fmt.Sprintf("SELECT * FROM %s WHERE %s = $1", b.table, idCol)
}

func (b *Builder) Base() string {
	var query strings.Builder
	query.WriteString(b.selectCols)
	query.WriteString("\nFROM ")
	query.WriteString(b.tableRef())

	for _, join := range b.joins {
		query.WriteString("\n")
		query.WriteString(join)
	}

	if b.orderCol != "" {
		query.WriteString("\nORDER BY ")
		query.WriteString(b.orderCol)
		query.WriteString(" ")
		query.WriteString(b.orderDir)
	}

	return query.String()
}

func (b *Builder) WithCursor() string {
	var query strings.Builder
	query.WriteString(b.selectCols)
	query.WriteString("\nFROM ")
	query.WriteString(b.tableRef())

	for _, join := range b.joins {
		query.WriteString("\n")
		query.WriteString(join)
	}

	if len(b.conditions) > 0 {
		query.WriteString("\nWHERE ")

		paramIdx := 1
		for i, cond := range b.conditions {
			if i > 0 {
				query.WriteString(" AND ")
			}

			switch cond.Operator {
			case "between":
				query.WriteString(cond.Column)
				query.WriteString(" BETWEEN $")
				query.WriteString(fmt.Sprintf("%d", paramIdx))
				query.WriteString(" AND $")
				query.WriteString(fmt.Sprintf("%d", paramIdx+1))
				paramIdx += 2
			case "gt", ">":
				query.WriteString(cond.Column)
				query.WriteString(" > $")
				query.WriteString(fmt.Sprintf("%d", paramIdx))
				paramIdx++
			case "lt", "<":
				query.WriteString(cond.Column)
				query.WriteString(" < $")
				query.WriteString(fmt.Sprintf("%d", paramIdx))
				paramIdx++
			case "gte", ">=":
				query.WriteString(cond.Column)
				query.WriteString(" >= $")
				query.WriteString(fmt.Sprintf("%d", paramIdx))
				paramIdx++
			case "lte", "<=":
				query.WriteString(cond.Column)
				query.WriteString(" <= $")
				query.WriteString(fmt.Sprintf("%d", paramIdx))
				paramIdx++
			case "eq", "=":
				query.WriteString(cond.Column)
				query.WriteString(" = $")
				query.WriteString(fmt.Sprintf("%d", paramIdx))
				paramIdx++
			}
		}
	} else {
		// Default behavior based on order column and direction
		operator := "<"
		if b.orderDir == "ASC" {
			operator = ">"
		}
		query.WriteString("\nWHERE ")
		query.WriteString(b.orderCol)
		query.WriteString(" ")
		query.WriteString(operator)
		query.WriteString(" $1")
	}

	query.WriteString("\nORDER BY ")
	query.WriteString(b.orderCol)
	query.WriteString(" ")
	query.WriteString(b.orderDir)

	return query.String()
}
