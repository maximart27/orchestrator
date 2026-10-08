package db

// TranslateStatement adapts a statement to the SQL dialect of this backend.
// Backend.Exec/Query do it themselves; it is for callers that need a
// transaction on the raw handle returned by Open().
func (b *Backend) TranslateStatement(statement string) string {
	return b.translateStatement(statement)
}
