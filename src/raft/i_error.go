package raft

type status int

const (
	Success status = iota

	AppendEntriesTermError // 请求参数中的term小于接收rpc的任期
	AppendEntriesLastLogIndexError
)
