package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comMents below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreeMent on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the saMe server.
//

import "sync"
import "labrpc"

// import "bytes"
// import "encoding/gob"
import "time"
import "math/rand"

//
// as each Raft peer becoMes aware that Successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the saMe server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type LogEntry struct {
	Term    int
	Command interface{}
}

//
// A Go object impleMenting a single Raft peer.
//
type Raft struct {
	mu                sync.Mutex
	peers             []*labrpc.ClientEnd
	persister         *Persister
	me                int // index into peers[]
	Timeout           time.Duration
	HeartbeatTimeout  time.Duration
	State             int // Folllower is 0, Candidate is 1, and Leader is 2.
	CurrentTerm       int
	VotedFor          int
	LogEntries        []LogEntry // Entries are indexed by Log Index. Each LogEntry consists of a Term and a Command. This makes things much simpler.
	Election_Timer    *time.Ticker
	Heartbeat_Timer   *time.Ticker
	ElectionResetTime time.Time
	Election_Started  bool
	CommitIndex       int
	LastApplied       int

	VoteCount  int
	NextIndex  []int // For each server, index of the next log entry to send to that server. It is initialized to leader last log index + 1
	MatchIndex []int // For each server, index of the highest log entry known to  be replicated on server (initiailized to 0, increases monotonically)

}

type AppendEntryArgs struct {
	LeaderTerm   int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntryReply struct {
	TermRply    int
	SuccessRply bool
}

// Handler for AppendEntry
func (rf *Raft) SendAppendEntry(server int, args AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	return ok
}

func (rf *Raft) AppendEntry(args AppendEntryArgs, reply *AppendEntryReply) {
	if args.LeaderTerm < rf.CurrentTerm {
		print("Leader Term is ", args.LeaderTerm, " Follower term is ", rf.CurrentTerm, "\n")
		reply.TermRply = rf.CurrentTerm
		reply.SuccessRply = false

		return
	}
	if args.LeaderTerm > rf.CurrentTerm {
		print("[", rf.CurrentTerm, "] ", rf.me, " REC HRTBT FROM ", args.LeaderId, "\n")
		rf.Follow(args.LeaderTerm)

	}
	if args.LeaderTerm == rf.CurrentTerm {
		if rf.State != 0 {
			rf.Follow(args.LeaderTerm)
		}
		reply.SuccessRply = true
	}
	reply.TermRply = rf.CurrentTerm
	return

}

func (rf *Raft) Heartbeat_Ticker() {
	rf.Heartbeat_Timer = time.NewTicker(time.Duration(int64(time.Millisecond) * 50))
	defer rf.Heartbeat_Timer.Stop()
	for {
		rf.do_Heartbeat()
		<-rf.Heartbeat_Timer.C
		if rf.State != 2 {
			print(rf.me, " is not leader anymore. Stopping heartbeats.\n")
			return
		}
	}
}

func (rf *Raft) do_Heartbeat() {
	var start_term = rf.CurrentTerm
	for i := range rf.peers {

		if rf.me != i {
			go rf.send_Heartbeat(i, start_term)
		}
	}

}

func (rf *Raft) send_Heartbeat(i int, SavedTerm int) {
	args := AppendEntryArgs{}
	args.LeaderTerm = SavedTerm
	args.LeaderId = rf.me
	args.PrevLogIndex = rf.LastApplied
	args.PrevLogTerm = -1 // For now this is ok, since we are not writing any logs
	args.Entries = rf.LogEntries
	args.LeaderCommit = rf.CommitIndex
	reply := AppendEntryReply{}
	if rf.State != 2 {
		return
	}
	ok := rf.SendAppendEntry(i, args, &reply)
	print("[", rf.CurrentTerm, "]", "[HEARTBEAT] FROM ", rf.me, " SENT TO ", i, " and reply was ", ok, " Their reply.term is ", reply.TermRply, "\n")

	if reply.TermRply > SavedTerm {
		print("Stepping down from leadership ", rf.me, "->", i, ":", reply.TermRply, ">", rf.CurrentTerm, "\n")
		print("<", rf.me, ">", " State: ", rf.State, "\n")
		rf.Follow(reply.TermRply)
		return
	}

}

// return CurrentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.
	term = rf.CurrentTerm
	isleader = rf.State == 2

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.CurrentTerm)
	// e.Encode(rf.VotedFor)
	// e.Encode(rf.LogEntries)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.CurrentTerm)
	// d.Decode(&rf.VotedFor)
	// d.Decode(&rf.LogEntries)
}

//
// example RequestVote RPC arguMents structure.
//
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	CurrentTerm int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {

	// var LastLogIndex = len(rf.LogEntries)
	// var LastLogTerm int
	// if len(rf.LogEntries) == 0 {
	// 	LastLogTerm = -1
	// }else{
	// 	LastLogTerm = rf.LogEntries[(args.LastLogIndex-1)].Term
	// }
	if args.Term > rf.CurrentTerm {
		rf.Follow(args.Term)

	}
	if (rf.VotedFor == -1 || rf.VotedFor == args.CandidateId) && rf.CurrentTerm == args.Term {
		reply.VoteGranted = true
		print(rf.me, " votes for ", args.CandidateId, "\n", rf.me, " term is ", args.Term, "\n")
		rf.VotedFor = args.CandidateId
		rf.ElectionResetTime = time.Now()

	} else {
		reply.VoteGranted = false
	}
	reply.CurrentTerm = rf.CurrentTerm
	return
}

func (rf *Raft) Follow(term int) {
	rf.State = 0
	rf.VotedFor = -1
	rf.VoteCount = 0
	rf.CurrentTerm = term
	rf.ElectionResetTime = time.Now()
	go rf.Election_Ticker()
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguMents in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the saMe as the types of the arguMents declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field naMes in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreeMent on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreeMent and return imMediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := (rf.CommitIndex + 1)
	term := (rf.CurrentTerm)
	isLeader := (rf.State == 2)

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the saMe order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg Messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//

func (rf *Raft) send_VoteRequest(i int, term int) {
	args := RequestVoteArgs{}
	args.Term = term
	args.CandidateId = rf.me
	args.LastLogIndex = len(rf.LogEntries)

	var majority int
	if len(rf.peers)%2 == 0 {
		majority = len(rf.peers) / 2
	} else {
		majority = len(rf.peers)/2 + 1
	}

	// SEND VOTE REQUEST
	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(i, args, &reply)
	print("SENT [VOTE REQUEST] FROM ", rf.me, " TO ", i, "\n")
	if rf.State != 1 {
		return
	}
	if ok == true {

		print("RECV [VOTE REQUEST REPLY] FROM ", i, " TO ", rf.me, " [", term, ", ", reply.CurrentTerm, "]", "\n")
		if reply.CurrentTerm > term {
			rf.Follow(reply.CurrentTerm)
			return
		} else if reply.CurrentTerm == term {

			if reply.VoteGranted == true {

				rf.VoteCount++
				if rf.VoteCount >= majority {
					rf.State = 2
					rf.VoteCount = 0
					rf.VotedFor = -1
					rf.Election_Started = false

					rf.CurrentTerm = term
					print("Leader is ", rf.me, " with term = ", rf.CurrentTerm, "\n")
					print(rf.me, " is sending heartbeats... \n")

					go rf.Heartbeat_Ticker()
					return
				}
			}
		}

	}
	if reply.VoteGranted == false {
		if reply.CurrentTerm > rf.CurrentTerm {
			rf.Follow(reply.CurrentTerm)

			return
		}

	}

	// if VoteCount >= majority{
	// 	rf.State = 2

	// 	// Removed break statement and suddenly all is well. need to change this perhaps?
	// }

	return
}

func (rf *Raft) do_Election() {
	rf.Election_Started = true
	rf.CurrentTerm = rf.CurrentTerm + 1
	rf.State = 1
	rf.VotedFor = rf.me
	rf.VoteCount = 1
	rf.ElectionResetTime = time.Now()

	// Timeout = time.Duration(int64(150)+rand.Int63n(300)) * time.Millisecond

	print("Server ", rf.me, " times out ", "\n")
	print("[ELECTION] Candidate is ", rf.me, " for term = ", rf.CurrentTerm, "\n")

	for i := range rf.peers {
		if i != rf.me {
			go rf.send_VoteRequest(i, rf.CurrentTerm)
		}

	}

	go rf.Election_Ticker()

}
func (rf *Raft) Election_Ticker() {
	var start_term = rf.CurrentTerm
	var timeout_interval = time.Duration(time.Duration(150+rand.Intn(1000)) * time.Millisecond)
	rf.Election_Timer = time.NewTicker(time.Duration(int64(time.Millisecond) * 10))
	defer rf.Election_Timer.Stop()

	for {

		<-rf.Election_Timer.C
		if rf.State != 1 && rf.State != 0 {
			return
		}
		if start_term != rf.CurrentTerm {
			return
		}
		if time.Since(rf.ElectionResetTime) > timeout_interval {
			print("Attempting election with ", rf.me, " and in state: ", rf.State, " and Term:", rf.CurrentTerm, "\n")
			rf.do_Election()
			return
		}
	}
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.Election_Started = false
	// Your initialization code here.
	rf.State = 0 // Folllower is 0, Candidate is 1, and Leader is 2.
	rf.CurrentTerm = -1
	rf.VotedFor = -1
	rf.VoteCount = 0
	// LogEntries	[][]int
	rand.Seed(time.Now().UnixNano())

	rand.Seed(time.Now().UnixNano())
	rf.HeartbeatTimeout = time.Duration(int64(time.Millisecond) * int64(int64(300)+rand.Int63n(100)))
	rf.CommitIndex = -1
	rf.LastApplied = -1
	for i := 0; i < len(peers); i++ {
		rf.NextIndex = append(rf.NextIndex, 1)
		rf.MatchIndex = append(rf.MatchIndex, 0)
	}
	rf.ElectionResetTime = time.Now()
	go rf.Election_Ticker()

	// NextIndex []int // to be reinitialized after reelection
	// MatchIndex	[]int // to be reinitialized after reelection
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	print("make is done ", me, "\n")
	return rf
}
