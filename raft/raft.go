// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"math/rand"
	"sort"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes   map[uint64]bool
	rejects map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout     int
	thisElectionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	prs := make(map[uint64]*Progress, len(c.peers))
	for _, id := range c.peers {
		prs[id] = &Progress{}
	}
	hardState, _, _ := c.Storage.InitialState()
	log := newLog(c.Storage)
	log.committed = hardState.Commit
	r := &Raft{
		id:               c.ID,
		Term:             hardState.Term,
		Vote:             hardState.Vote,
		RaftLog:          log,
		State:            StateFollower,
		electionTimeout:  c.ElectionTick,
		heartbeatTimeout: c.HeartbeatTick,
		Prs:              prs,
	}
	r.startNewTimers()
	return r
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	peerProgress := r.Prs[to]
	logTerm, _ := r.RaftLog.Term(peerProgress.Match)

	var entriesToSend []*pb.Entry
	for i := peerProgress.Match; i < uint64(len(r.RaftLog.entries)); i++ {
		entriesToSend = append(entriesToSend, &r.RaftLog.entries[i])
	}

	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		LogTerm: logTerm,
		Index:   peerProgress.Match,
		Entries: entriesToSend,
		Commit:  r.RaftLog.committed,
	})
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeats() {
	if r.State == StateLeader {
		for _, id := range r.Others() {
			r.sendHeartbeat(id)
		}
	}
}

func (r *Raft) sendHeartbeat(to uint64) {
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		From:    r.id,
		To:      to,
		Term:    r.Term,
	})
}

func (r *Raft) sendRequestVote(to uint64) {
	li := r.RaftLog.LastIndex()
	logTerm, _ := r.RaftLog.Term(li)
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		LogTerm: logTerm,
		Index:   li,
	})
}

func (r *Raft) sendRequestVoteResponse(to uint64, reject bool) {
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Reject:  reject,
	})
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	switch r.State {
	case StateLeader:
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			r.Step(pb.Message{
				MsgType: pb.MessageType_MsgBeat,
				From:    r.id,
				To:      r.id,
				Term:    r.Term,
			})
		}

	case StateFollower:
		fallthrough
	case StateCandidate:
		r.electionElapsed++
		if r.electionElapsed >= r.thisElectionTimeout {
			r.electionElapsed = 0
			r.Step(pb.Message{MsgType: pb.MessageType_MsgHup, From: r.id, To: r.id})
		}
	}
}

func (r *Raft) startNewTimers() {
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.thisElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	r.State = StateFollower
	r.Term = term
	r.Lead = lead
	r.startNewTimers()
	// Your Code Here (2A).
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	if r.State != StateLeader {
		r.State = StateCandidate
		r.Term++
		r.votes = map[uint64]bool{}
		r.rejects = map[uint64]bool{}
		r.GotVote(r.id, false)
		r.startNewTimers()
	}
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	r.State = StateLeader
	r.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		From:    r.id,
		To:      r.id,
		Term:    r.Term,
		Entries: []*pb.Entry{{}},
	})
	r.startNewTimers()
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	if m.Term > r.Term {
		r.becomeFollower(m.Term, m.From)
		r.Vote = None
	}

	switch m.MsgType {
	case pb.MessageType_MsgHup:
		// electionTimeout reached
		if r.State == StateLeader {
			return nil
		}
		r.becomeCandidate()
		for _, id := range r.Others() {
			r.sendRequestVote(id)
		}
	case pb.MessageType_MsgBeat:
		r.sendHeartbeats()
	case pb.MessageType_MsgRequestVote:
		li := r.RaftLog.LastIndex()
		lt, _ := r.RaftLog.Term(li)
		if lt > m.LogTerm || (lt == m.LogTerm && li > m.Index) {
			// reject
			r.sendRequestVoteResponse(m.From, true)
			return nil
		}
		if r.State != StateLeader {
			r.Lead = None
		}
		// have we voted?
		if r.Vote == None {
			r.Vote = m.From
		}
		r.sendRequestVoteResponse(m.From, m.From != r.Vote)
	case pb.MessageType_MsgRequestVoteResponse:
		if r.State == StateCandidate && !r.votes[m.From] && !r.rejects[m.From] {
			r.GotVote(m.From, m.Reject)
		}
	case pb.MessageType_MsgHeartbeat:
		if m.Term >= r.Term {
			r.becomeFollower(m.Term, m.From)
		}
		r.sendHeartbeatResponse(m)
	case pb.MessageType_MsgHeartbeatResponse:
		if r.State == StateLeader {
			if m.Index != r.RaftLog.LastIndex() {
				r.sendAppend(m.From)
			}
		}
	case pb.MessageType_MsgPropose:
		r.handlePropose(m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendEntriesResponse(m)
	}
	return nil
}

func (r *Raft) GotVote(from uint64, reject bool) {
	if !reject {
		r.votes[from] = true
		if len(r.votes)*2 > len(r.Prs) {
			r.becomeLeader()
		}
	} else {
		r.rejects[from] = true
		if len(r.rejects)*2 >= len(r.Prs) {
			r.becomeFollower(r.Term, None)
		}
	}
}

func (r *Raft) handlePropose(m pb.Message) {
	// add to uncommited
	if r.State != StateLeader {
		return
	}

	for _, entry := range m.Entries {
		entry.Index = r.RaftLog.LastIndex() + 1
		entry.Term = r.Term
		r.RaftLog.entries = append(r.RaftLog.entries, *entry)
	}

	for _, id := range r.Others() {
		r.sendAppend(id)
	}
	// update my progress
	li := r.RaftLog.LastIndex()
	r.Progress().Match = li
	r.Progress().Next = li + 1
	if len(r.Prs) == 1 {
		r.RaftLog.committed = li
	}
}

func (r *Raft) sendAppendEntriesResponse(m pb.Message, reject bool, lastIndex uint64) {
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      m.From,
		From:    m.To,
		Term:    r.Term,
		Reject:  reject,
		Index:   lastIndex,
	})
}

func (r *Raft) sendHeartbeatResponse(m pb.Message) {
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      m.From,
		From:    m.To,
		Term:    r.Term,
		Index:   r.RaftLog.LastIndex(),
	})
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	if m.Term >= r.Term {
		r.becomeFollower(m.Term, m.From)
	} else {
		return
	}
	if logTerm, _ := r.RaftLog.Term(m.Index); logTerm != m.LogTerm {
		r.sendAppendEntriesResponse(m, true, 0)
		return
	}
	for _, entry := range m.Entries {
		// truncate own log
		if logTerm, _ := r.RaftLog.Term(entry.Index); logTerm != 0 {
			if logTerm != entry.Term {
				r.RaftLog.entries = r.RaftLog.entries[:entry.Index-1]
				r.RaftLog.stabled = entry.Index - 1
			} else {
				continue
			}
		}
		r.RaftLog.entries = append(r.RaftLog.entries, *entry)
	}
	li := m.Index + uint64(len(m.Entries))
	r.RaftLog.committed = min(m.Commit, li)
	r.sendAppendEntriesResponse(m, false, li)

}

func (r *Raft) handleAppendEntriesResponse(m pb.Message) {
	if r.State != StateLeader {
		return
	}
	resort := r.Prs[m.From].Match <= r.RaftLog.committed && m.Index > r.RaftLog.committed
	r.Prs[m.From].Match = m.Index
	r.Prs[m.From].Next = m.Index + 1
	if resort {
		match := make(uint64Slice, len(r.Prs))
		j := 0
		for i := range r.Prs {
			match[j] = r.Prs[i].Match
			j++
		}
		sort.Sort(match)
		mid := match[(len(r.Prs)-1)/2]
		if term, _ := r.RaftLog.Term(mid); term == r.Term {
			r.RaftLog.committed = mid
			r.Step(pb.Message{
				MsgType: pb.MessageType_MsgPropose,
			})
		}
	}
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}

func (r *Raft) Progress() *Progress {
	return r.Prs[r.id]
}

func (r *Raft) Others() []uint64 {
	result := []uint64{}
	for id, _ := range r.Prs {
		if id != r.id {
			result = append(result, id)
		}
	}
	return result
}
