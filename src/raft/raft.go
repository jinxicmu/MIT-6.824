package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import "labrpc"
import "math/rand"
import "time"
import "fmt"
// import "bytes"
// import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
 currentTerm int
 votedFor int
 leaderId int
 alreadyVoted bool
 heartbeatReceived bool
 receivedVotes int
 switchToFollowerChan chan bool
 switchToCandidateChan chan bool
 switchToLeaderChan chan bool
 state State
}

type State int

const (
        LEADER State = iota
        FOLLOWER
        CANDIDATE
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.state == LEADER

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term int
	CandidateId int

	// Your data here (2A, 2B).
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term int
	VoteGranted bool
	// Your data here (2A).
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2A, 2B).
	// Vote granted

	// --> refuse
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	if (args.Term == rf.currentTerm) && rf.alreadyVoted {
		// --> refuse
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	// args.term > rf.currentTerm
	// OR agrs.term == rf.currentTerm && !rf.alreadyVoted
	// --> vote
	reply.VoteGranted = true
	rf.alreadyVoted = true
	rf.votedFor = args.CandidateId
	fmt.Printf("[%d] args.Term:%d rf.currentTerm:%d rf.votedFor:%d\n", rf.me, args.Term, rf.currentTerm, rf.votedFor)
	rf.currentTerm = args.Term
	return
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, wg *sync.WaitGroup) {
	defer wg.Done()

	reply := &RequestVoteReply{}
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		fmt.Printf("[%d] Hear back after sendRequestVote - RequestVoteReply.VoteGranted:%t\n", rf.me, reply.VoteGranted)
		if reply.VoteGranted{
			rf.receivedVotes++
		} else {
			rf.currentTerm = findMax(rf.currentTerm, reply.Term)
		}
	}
}

type AppendEntriesRequest struct {
	Term int
	LeaderId int
}

type AppendEntriesReply struct {
	Term int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesRequest, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term >= rf.currentTerm {
		// fmt.Printf("[%d] AppendEntries-received heartbeat\n", rf.me)
		rf.switchToFollowerChan<-true
		rf.currentTerm = args.Term
		rf.leaderId = args.LeaderId
		reply.Success = true
	} else {
		// follower has higher term, will step to candidate
		rf.leaderId = -1
		if rf.getServerState == FOLLOWER {
			rf.switchToCandidateChan<-true
	  }
		reply.Success = false
		reply.Term = rf.currentTerm
	}
	return
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesRequest) {
	reply := &AppendEntriesReply{}
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok && !reply.Success {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		rf.currentTerm = findMax(rf.currentTerm, reply.Term)
	}
}

func (rf *Raft) broadcastHeartbeats() {
	rf.mu.Lock()
	request := &AppendEntriesRequest{}
	request.Term = rf.currentTerm
	request.LeaderId = rf.me
	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go rf.sendAppendEntries(i, request)
	}
}

func (rf *Raft) startNewElectionRound() {
	request := &RequestVoteArgs{}

	rf.mu.Lock()
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.alreadyVoted = true
	rf.receivedVotes = 0
	request.Term = rf.currentTerm
	request.CandidateId = rf.me
	rf.mu.Unlock()

	var wg sync.WaitGroup
	wg.Add(len(rf.peers) - 1)
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go rf.sendRequestVote(i, request, &wg)
	}
	wg.Wait()

	// while waiting, a candidate may turn into a follower already
	if rf.getServerState() == FOLLOWER {
		return
	}

	// become leader
	fmt.Printf("[%d] receivedVotes:%d\n", rf.me, rf.receivedVotes)
	if rf.receivedVotes >= len(rf.peers)/2 {
		rf.switchToLeaderChan<-true
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


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

func findMax(a int, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}

func getRandomTimeOut(min int, max int) time.Duration {
	return time.Duration(min + rand.Intn(max-min)) * time.Millisecond
}

func(rf *Raft) getServerState() State{
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//

func(rf *Raft) serverRunning() {
	for {
		state := rf.getServerState()
		switch state {
		case LEADER:
			go rf.broadcastHeartbeats()
			select {
			case <-time.After(100 * time.Millisecond):
			case <-rf.switchToFollowerChan:
				rf.changeStateTo(FOLLOWER)
			case <-rf.switchToCandidateChan:
			case <-rf.switchToLeaderChan:
			}
		case FOLLOWER:
			select {
			case <-time.After(getRandomTimeOut(250, 500)):
				rf.changeStateTo(CANDIDATE)
			case <-rf.switchToFollowerChan:
			case <-rf.switchToCandidateChan:
				rf.changeStateTo(CANDIDATE)
			case <-rf.switchToLeaderChan:
			}
		case CANDIDATE:
			go rf.startNewElectionRound()
			select {
			case <-time.After(getRandomTimeOut(250, 500)):
			case <-rf.switchToFollowerChan:
				rf.changeStateTo(FOLLOWER)
			case <-rf.switchToLeaderChan:
				rf.changeStateTo(LEADER)
			case <-rf.switchToCandidateChan:
			}
		}
	}
}

func(rf *Raft) changeStateTo(toState State) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = toState
	rf.alreadyVoted = false
	switch toState {
	case LEADER:
		rf.leaderId = rf.me
		fmt.Printf("[%d] switchToLeader\n", rf.me)
	case FOLLOWER:
		fmt.Printf("[%d] switchToFollower\n", rf.me)
	case CANDIDATE:
		rf.leaderId = -1
		fmt.Printf("[%d] switchToCandidate\n", rf.me)
	}
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.leaderId = -1
	rf.switchToFollowerChan = make(chan bool)
	rf.switchToCandidateChan = make(chan bool)
	rf.switchToLeaderChan = make(chan bool)
	rf.state = FOLLOWER
	// Your initialization code here (2A, 2B, 2C).
	go rf.serverRunning()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
