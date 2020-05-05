package db

// CommitError is used to signal an error when trying to fast forward.
type CommitError struct {
	error
}

// NewCommitError creates a new CommitError.
func NewCommitError(err error) CommitError {
	return CommitError{err}
}
