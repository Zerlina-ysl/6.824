package raft

type status int

const (
	Success status = iota

	TermError // 请求参数中的term小于当前term
	AppendEntriesLastLogIndexError
)
