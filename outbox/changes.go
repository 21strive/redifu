package outbox

// ChangeKeys lists the keys the changes of one table touch, each once and in the order of its first
// change (oldest first), and the first change of each key. That change's Old row is the row as it
// stood before the batch — what Redis holds, since every earlier change was applied — so it names
// the indexes the entity has to leave. A handler reloads the current rows of the keys through
// batch.Tx and compares each with the old row of its first change.
func ChangeKeys(changes []Change) ([]string, map[string]Change) {
	keys := make([]string, 0, len(changes))
	first := make(map[string]Change, len(changes))
	for _, change := range changes {
		if _, seen := first[change.Key]; seen {
			continue
		}
		first[change.Key] = change
		keys = append(keys, change.Key)
	}
	return keys, first
}
