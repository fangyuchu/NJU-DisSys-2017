package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (Index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"math/rand"
	"sync"
	"time"
)
import "labrpc"

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
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // Index into peers[]

	/******************************************************************************************************************/
	//persistent state
	CurrentTerm int
	Votedfor    int //-1表示未投票
	Logs        []*Log
	Identity    int //0表示follower，1表示candidate，2表示leader
	//volatile state on all servers
	CommitIndex int
	LastApplied int
	//volatile state on leaders
	NextIndex  []*int
	MatchIndex []*int
	//modified state
	ReqFromLeader    chan bool //是否收到leader的定时消息
	VoteResultComing chan bool //选举结果是否出来
	Voted            chan bool //设置容量大小为numpeers，通道中有消息表示有得到投票
	/******************************************************************************************************************/

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

/******************************************************************************************************************/
type Log struct {
	Index int
	Term  int
	Load  string
}

/******************************************************************************************************************/

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	/******************************************************************************************************************/
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
	/******************************************************************************************************************/
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	/******************************************************************************************************************/
	CurrentTerm   int
	VoteGranted   int  //0表示未回复，1表示拒绝，2表示同意
	CurrentLeader bool //回复者当前是否为leader
	/******************************************************************************************************************/
}

/******************************************************************************************************************/
type AppendEntries struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Logs         []*Log
	LeaderCommit int
}
type AppendEntriesReply struct {
	Term    int
	Success int //0代表未回复，1代表拒绝请求，2代表成功
}

/******************************************************************************************************************/

// return CurrentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var Term int
	var isleader bool
	// Your code here.
	Term = rf.CurrentTerm
	isleader = rf.Identity == 2
	return Term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	//TODO:not completed
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	//rf.persister.SaveRaftState(data)
	/******************************************************************************************************************/
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.Votedfor)
	e.Encode(rf.Identity)
	var logNum = len(rf.Logs)
	e.Encode(logNum)
	for i := 0; i < logNum; i = i + 1 {
		e.Encode(rf.Logs[i].Term)
		e.Encode(rf.Logs[i].Index)
		e.Encode(rf.Logs[i].Load)
	}
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	/******************************************************************************************************************/
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	//TODO:not completed
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	/******************************************************************************************************************/
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(rf.CurrentTerm)
	d.Decode(rf.Votedfor)
	var num int
	d.Decode(num)
	for i := 0; i < num; i++ {
		var Term, Index int
		var Load string
		d.Decode(Term)
		d.Decode(Index)
		d.Decode(Load)
		rf.Logs = append(rf.Logs, &Log{Term: Term, Index: Index, Load: Load})
	}

	/******************************************************************************************************************/
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	/******************************************************************************************************************/
	fmt.Println(rf.me, "开始给", args.CandidateId, "投票")
	fmt.Print("此时，投票者", rf.me, "的term为", rf.CurrentTerm)
	fmt.Println("，而候选者", args.CandidateId, "的term为", args.Term)
	if args.Term < rf.CurrentTerm {
		//candidate的term落后,拒绝
		reply.CurrentTerm = rf.CurrentTerm
		reply.VoteGranted = 1
		return
	}
	if args.Term == rf.CurrentTerm && rf.Identity == 2 {
		//rf已经是leader，candidate的term也不比他大，拒绝
		reply.VoteGranted = 1
		reply.CurrentLeader = true
		return
	}
	if args.Term > rf.CurrentTerm {
		//收到投票请求的服务器发现对方term比自己高，则更新term
		rf.CurrentTerm = args.Term
		rf.Votedfor = -1 //rf是follower时也需要重置
		if rf.Identity == 2 {
			//若自己是leader，则退为follower
			rf.BecomeFollower()
		}
		if rf.Identity == 1 {
			//若自己是candidate，则停止选举并退回到follower状态
			rf.VoteResultComing <- true
			rf.BecomeFollower()
		}
	}
	if rf.Votedfor != -1 {
		//已经投过票了,拒绝
		fmt.Println(rf.me, "给", rf.Votedfor, "投过票，所以拒绝了", args.CandidateId)
		reply.VoteGranted = 1
		return
	}
	if args.LastLogTerm < rf.Logs[len(rf.Logs)-1].Term {
		//log的term不够新,拒绝
		reply.VoteGranted = 1
		return
	} else if args.LastLogTerm == rf.Logs[len(rf.Logs)-1].Term && args.LastLogIndex < rf.Logs[len(rf.Logs)-1].Index {
		//log的index不够新,拒绝
		reply.VoteGranted = 1
		return
	}
	fmt.Println(rf.me, "投给了", args.CandidateId)
	//rf退回follower状态
	reply.VoteGranted = 2
	rf.Votedfor = args.CandidateId

	/******************************************************************************************************************/
}

//
// example code to send a RequestVote RPC to a server.
// server is the Index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	defer func() { //防止Voted已经被关闭的情况
		if err := recover(); err != nil {
		}
	}()
	rf.Voted <- true
	return ok
}

func (rf *Raft) sendRequestReply(server int, args AppendEntries, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.Reply", args, reply)
	return ok
}
func (rf *Raft) Reply(args AppendEntries, reply *AppendEntriesReply) {
	//follower收到leader发来的append entry请求后进行回复
	rf.ReqFromLeader <- true
	if args.Term < rf.CurrentTerm {
		//leader的term小，拒绝
		fmt.Println(rf.me, "拒绝了", args.LeaderID, "的心跳包")
		reply.Success = 1
		reply.Term = rf.CurrentTerm
		return
	}
	if args.Logs == nil {
		fmt.Println(rf.me, "处理了", args.LeaderID, "的心跳包")
		reply.Success = 2
		if rf.Identity == 1 { //在竞选时发现合法leader发来的包，就自动退回成follower
			rf.VoteResultComing <- true
			rf.BecomeFollower()
		}
		return
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
// the first return value is the Index that the command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	Index := -1
	Term := -1
	isLeader := true

	return Index, Term, isLeader
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
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.Identity = 0 //开始时都是candidate
	rf.CurrentTerm = 0
	rf.Votedfor = -1
	rf.Logs = append(rf.Logs, &Log{0, 0, "start"})
	rf.NextIndex = nil
	rf.MatchIndex = nil
	rf.readPersist(persister.ReadRaftState())
	rf.ReqFromLeader = make(chan bool, 1)
	rf.VoteResultComing = make(chan bool, 1)
	rf.Voted = make(chan bool, len(peers))
	go rf.startWorking()
	// Your initialization code here.

	// initialize from state persisted before a crash

	//rf.readPersist(persister.ReadRaftState())

	return rf
}

/******************************************************************************************************************/
func (rf *Raft) startElect() {
	rf.CurrentTerm += 1 //Term+1
	randNum := rand.Intn(150)
	electionTimeout := time.Duration(randNum+150) * time.Millisecond
	var numPeers int
	numPeers = len(rf.peers)
	reply := make([]RequestVoteReply, numPeers)
	for i := 0; i < numPeers; i++ {
		Term := rf.CurrentTerm
		CandidateId := rf.me
		LastLogIndex := rf.Logs[len(rf.Logs)-1].Index
		LastLogTerm := rf.Logs[len(rf.Logs)-1].Term
		req := RequestVoteArgs{Term, CandidateId, LastLogIndex, LastLogTerm}
		go rf.sendRequestVote(i, req, &reply[i])
	}
	go rf.checkIFElected(reply, numPeers)

	select {
	case <-rf.VoteResultComing:
		return
	case <-time.After(electionTimeout):
		fmt.Println(rf.me, "未收到足够选票，超时")
		rf.BecomeCandidate() //重置各项指标
	}
}
func (rf *Raft) checkIFElected(reply []RequestVoteReply, numPeers int) {
	var yes, no int
	var time int
	for <-rf.Voted {
		time++
		//fmt.Print(rf.me," result:",time,"       ")
		for i := 0; i < numPeers; i++ {
			//fmt.Print(reply[i].VoteGranted)
			if reply[i].VoteGranted == 2 {
				yes++
			} else if reply[i].VoteGranted == 1 {
				no++
			}
		}
		if no >= (int)(numPeers/2)+1 {
			fmt.Println(rf.me, "fail in election")
			rf.BecomeFollower()
			rf.VoteResultComing <- true
			return
		}
		if yes >= (int)(numPeers/2)+1 {
			//竞选成功
			fmt.Println(rf.me, "成为leader")
			rf.BecomeLeader()
			rf.VoteResultComing <- true
			return
		}
		yes = 0
		no = 0
	}
}

//func(rf *Raft)checkVoteResult(reply *RequestVoteReply,yes *int,no *int){
//	fmt.Println(rf.me,"wait for one reply")
//	<-reply.Voted
//	fmt.Println(rf.me,"receive one reply")
//	if reply.VoteGranted==2{
//		*yes++
//	}else if reply.VoteGranted==1{
//		*no++
//		if reply.CurrentTerm > rf.CurrentTerm {
//			//candidate发现自己的term较小，则更新term并退回至follower
//			rf.CurrentTerm = reply.CurrentTerm
//			rf.BecomeFollower()
//			rf.VoteResultComing<-true
//			return
//		}
//		if reply.CurrentLeader && reply.CurrentTerm == rf.CurrentTerm {
//			//已有leader存在，且term不比自己小
//			rf.BecomeFollower()
//			rf.VoteResultComing<-true
//			return
//		}
//	}
//
//}
func (rf *Raft) startWorking() {
	for true {
		if rf.Identity == 2 {
			//leader，发心跳包
			//暂定50ms发送一次
			time.Sleep(25 * time.Millisecond)
			reply := make([]AppendEntriesReply, len(rf.peers))
			for server := 0; server < len(rf.peers); server++ {
				//fmt.Println(rf.me,"给",server,"发心跳包")
				currentTerm := rf.CurrentTerm
				leaderId := rf.me
				prevLogIndex := rf.Logs[len(rf.Logs)-1].Index
				prevLogTerm := rf.Logs[len(rf.Logs)-1].Term
				var l []*Log
				l = nil
				leaderCommit := rf.CommitIndex
				args := AppendEntries{currentTerm, leaderId, prevLogIndex,
					prevLogTerm, l, leaderCommit}
				go rf.sendRequestReply(server, args, &reply[server])
			}
			//TODO: wait until receiving all replys?
			time.Sleep(25 * time.Millisecond)
			for server := 0; server < len(rf.peers); server++ {
				if reply[server].Success == 1 {
					//有follower的term更大，leader退回到follower状态
					fmt.Println("leader", rf.me, "发现自己的term过小，退回到follower")
					rf.CurrentTerm = reply[server].Term
					rf.BecomeFollower()
				}
			}
		} else if rf.Identity == 1 {
			//candidate,竞选
			fmt.Println(rf.me, "开始竞选")
			rf.startElect()
		} else if rf.Identity == 0 {
			select {
			case <-rf.ReqFromLeader:
			case <-time.After(500 * time.Millisecond):
				//150ms没消息就成为候选者
				fmt.Println(rf.me, "没收到心跳包，成为竞选者")
				rf.BecomeCandidate()
			}
		}
	}

}

func (rf *Raft) BecomeFollower() {
	rf.Identity = 0
	rf.Votedfor = -1
	rf.NextIndex = nil
	rf.MatchIndex = nil
	defer func() { //防止由Leader转为Follower时的重复关闭通道的情况
		if err := recover(); err != nil {
		}
	}()
	close(rf.Voted)
}
func (rf *Raft) BecomeLeader() {
	rf.Identity = 2
	rf.Votedfor = -1
	for i := 0; i < len(rf.peers); i++ {
		nextInd := rf.Logs[len(rf.Logs)-1].Index + 1
		matchInd := 0
		rf.NextIndex = append(rf.NextIndex, &nextInd)
		rf.MatchIndex = append(rf.MatchIndex, &matchInd)
	}
	close(rf.Voted)
}
func (rf *Raft) BecomeCandidate() {
	rf.Identity = 1
	rf.Votedfor = -1
	rf.NextIndex = nil
	rf.MatchIndex = nil
	rf.Voted = make(chan bool, len(rf.peers))
}
