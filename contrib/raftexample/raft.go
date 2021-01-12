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

package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"

	"go.etcd.io/etcd/etcdserver/api/rafthttp"
	"go.etcd.io/etcd/etcdserver/api/snap"
	stats "go.etcd.io/etcd/etcdserver/api/v2stats"
	"go.etcd.io/etcd/pkg/fileutil"
	"go.etcd.io/etcd/pkg/types"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
	"go.etcd.io/etcd/wal"
	"go.etcd.io/etcd/wal/walpb"

	"go.uber.org/zap"
)

// A key-value stream backed by raft
type raftNode struct {
	proposeC <-chan string // proposed messages (k,v)
	// HTTP PUT 请求表示添加键值对数据，当收到HTTP PUT 请求时， httpKVAPI 会将请求中的键值信息通过proposeC通道传递给raftNode 实例进行处理。
	confChangeC <-chan raftpb.ConfChange // proposed cluster config changes
	//HTTP POST 请求表示集群节点修改的请求，当收到POST 请求时， httpKVAPI 会通过confChangeC 通道将修改的节点ID 传递给raftNode实例进行处理。
	commitC chan<- *string // entries committed to log (k,v)
	//raftNode 会将etcd-raft模块返回的待应用Entry记录（封装在Ready 实例中)写入commitC通道，另一方面，kvstore 会从commitC 通道中读取这些待应用的Entry 记录井保存其中的键值对信息。
	errorC chan<- error // errors from raft session
	//当etcd-raft模块关闭或是出现异常的时候，会通过errorC通道将该信息通知上层模块。

	id    int      // client ID for raft session
	peers []string // raft peer URLs
	//当前集群中所有节点的地址，当前节点会通过该字段中保存的地址向集群中其他节点发送消息。
	join bool // node is joining an existing cluster
	//当前节点是否为后续加入到一个集群的节点
	waldir      string                 // 存放WAL 日志文件的目录。
	snapdir     string                 // 存放快照文件的目录
	getSnapshot func() ([]byte, error) //用于获取快照数据的函数，在raft-example 示例中
	//该函数会调用kvstore.getSnapshot（）方法获取kvstore.kvStore 字段的数据
	lastIndex uint64 // index of log at start
	//当回放WAL 日志结束之后，会使用使用该字段记录最后一 条Entry 记录的索引值。

	confState     raftpb.ConfState //用于记录当前的集群状态，该状态就是从前面介 绍的node . confstatec 通道中获取的。
	snapshotIndex uint64           //保存当前快照的相关元数据，即快照所包含的最后一 条Entry 记录的索引值。
	appliedIndex  uint64           //保存上层模块己应用的位置，即己应用的最后一条Entry 记录的索引值。

	// raft backing for the commit/error channel
	node        raft.Node
	raftStorage *raft.MemoryStorage //前面已经介绍过Storage 接口及其具体实现
	//MemoryStorage ，这里不再重复介绍。在raft-example 示例中，该MemoryStorage 实例
	//与底层raftLog.storage 字段指向了同一个实例
	wal *wal.WAL //负责WAL 日志的管理。当节点收到一条Entry 记录时，首先
	//会将其保存到raftLog. unstable 中，之后会将其封装到Ready 实例中并交给上层模块发
	//送给集群中的其他节点，并完成持久化。在raftexample 示例中， Entry 记录的持久化
	//是将其写入raftLog.storage 中。在持久化之前， Entry 记录还会被写入WAL 日志文件
	//中，这样就可以保证这些Entry 记录不会丢失。WAL 日志文件是顺序写入的，所以其
	//写入性能不会影响节点的整体性能。

	snapshotter *snap.Snapshotter //负责管理快照数据， etcd-raft 模块并没有完
	//成快照数据的管理，而是将其独立成一个单独的模块，在后面有单独的章节来介绍相关模块。
	snapshotterReady chan *snap.Snapshotter // signals when snapshotter is ready
	//主要用于初始化的过程中监昕snapshotter 实例是否创建完成， snapshotter 负责管理etcd-raft模块产生的快照数据，

	snapCount uint64 //两次生成快照之间间隔的Entry 记录数，即当前节点每处
	//理一定数量的Entry 记录，就要触发一次快照数据的创建。每次生成快照时，即可释
	//放掉一定量的WAL 日志及raftLog 中保存的Entry 记录，从而避免大量Entry记录带来
	//的内存压力及大量的WAL 日志文件带来的磁盘压力； 另外，定期创建快照也能减少
	//了节点重启时回放的WAL 日志数量，加速了启动时间。
	transport *rafthttp.Transport //节点待发送的消息只是记录到了raft.msgs 中，etcd-raft模块并没有提供网络层的实现，
	//而由上层模块决定两个节点之间如何通信。这样就为网络层的实现提供了更大的灵活性，
	//例如，如果两个节点在同一台服务器中，我们完全可以使用共享内存方式实现两个节点的通信，并不一定非要通过网络设备完成
	stopc     chan struct{} // signals proposal channel closed
	httpstopc chan struct{} // signals http server to shutdown
	httpdonec chan struct{} // signals http server shutdown complete
}

var defaultSnapshotCount uint64 = 10000

// newRaftNode initiates a raft instance and returns a committed log entry
// channel and error channel. Proposals for log updates are sent over the
// provided the proposal channel. All log entries are replayed over the
// commit channel, followed by a nil message (to indicate the channel is
// current), then new log entries. To shutdown, close proposeC and read errorC.
func newRaftNode(id int, peers []string, join bool, getSnapshot func() ([]byte, error), proposeC <-chan string,
	confChangeC <-chan raftpb.ConfChange) (<-chan *string, <-chan error, <-chan *snap.Snapshotter) {

	commitC := make(chan *string)
	errorC := make(chan error)

	rc := &raftNode{
		proposeC:    proposeC,
		confChangeC: confChangeC,
		commitC:     commitC,
		errorC:      errorC,
		id:          id,
		peers:       peers,
		join:        join,
		waldir:      fmt.Sprintf("raftexample-%d", id),
		snapdir:     fmt.Sprintf("raftexample-%d-snap", id),
		getSnapshot: getSnapshot,
		snapCount:   defaultSnapshotCount,
		stopc:       make(chan struct{}),
		httpstopc:   make(chan struct{}),
		httpdonec:   make(chan struct{}),

		snapshotterReady: make(chan *snap.Snapshotter, 1),
		// rest of structure populated after WAL replay
	}
	go rc.startRaft()
	return commitC, errorC, rc.snapshotterReady
}

func (rc *raftNode) saveSnap(snap raftpb.Snapshot) error {
	// must save the snapshot index to the WAL before saving the
	// snapshot to maintain the invariant that we only Open the
	// wal at previously-saved snapshot indexes.
	// 快照元数据
	walSnap := walpb.Snapshot{
		Index: snap.Metadata.Index,
		Term:  snap.Metadata.Term,
	}
	//WAL 会将上述快照的元数据信息封装成一条日志记录下来
	if err := rc.wal.SaveSnapshot(walSnap); err != nil {
		return err
	}
	if err := rc.snapshotter.SaveSnap(snap); err != nil {
		return err
	}
	//根据快照的元数据信息，释放一些无用的WAL 日志文件的句柄
	return rc.wal.ReleaseLockTo(snap.Metadata.Index)
}

//过滤待应用的entry
func (rc *raftNode) entriesToApply(ents []raftpb.Entry) (nents []raftpb.Entry) {
	if len(ents) == 0 {
		return ents
	}
	firstIdx := ents[0].Index
	if firstIdx > rc.appliedIndex+1 {
		log.Fatalf("first index of committed entry[%d] should <= progress.appliedIndex[%d]+1", firstIdx, rc.appliedIndex)
	}

	//过滤掉已经应用过的entry
	if rc.appliedIndex-firstIdx+1 < uint64(len(ents)) {
		nents = ents[rc.appliedIndex-firstIdx+1:]
	}
	return nents
}

// publishEntries writes committed log entries to commit channel and returns
// whether all entries could be published.
//raftNode 会将所有待应用记录写入commitC 通道中。后续kvstore 就可以读取commitC 通道并保存相应的键值对数据
func (rc *raftNode) publishEntries(ents []raftpb.Entry) bool {
	for i := range ents {
		switch ents[i].Type {
		case raftpb.EntryNormal:
			if len(ents[i].Data) == 0 {
				// 如果Entry 记录的Data 为空， 则直接忽略该条Entry 记录
				break
			}
			s := string(ents[i].Data)
			select {
			//将数据写入commitC 通道， kvstore 会读取从其中读取并记录相应的KV 值
			case rc.commitC <- &s:
			case <-rc.stopc:
				return false
			}

		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange //将EntryConfChange 类型的记录封装成ConfChange
			cc.Unmarshal(ents[i].Data)
			//将ConfChange 实例传入底层的etcd - raft 组件
			rc.confState = *rc.node.ApplyConfChange(cc)

			//除了etcd-raft 组件中需要创建（或删除）对应的Progress 实例，网络层也需要做出相应的调整，即添加（或删除）相应的Peer 实例
			switch cc.Type {
			case raftpb.ConfChangeAddNode:
				if len(cc.Context) > 0 {
					rc.transport.AddPeer(types.ID(cc.NodeID), []string{string(cc.Context)})
				}
			case raftpb.ConfChangeRemoveNode:
				if cc.NodeID == uint64(rc.id) {
					log.Println("I've been removed from the cluster! Shutting down.")
					return false
				}
				rc.transport.RemovePeer(types.ID(cc.NodeID))
			}
		}

		// after commit, update appliedIndex
		//处理完成之后，更新raftNode 记录的已应用位置，该值在过滤：已应用记录的entriesToApply ()
		//方法及后面即将介绍的maybeTriggerSnapshot()方法中都有使用
		rc.appliedIndex = ents[i].Index

		// special nil commit to signal replay has finished
		//此次应用的是否为重放的Entry 记录，如果是，且重放完成，则使用commitC通道通知kvstore
		if ents[i].Index == rc.lastIndex {
			select {
			case rc.commitC <- nil:
			case <-rc.stopc:
				return false
			}
		}
	}
	return true
}

//读取快照文件?
func (rc *raftNode) loadSnapshot() *raftpb.Snapshot {
	snapshot, err := rc.snapshotter.Load()
	if err != nil && err != snap.ErrNoSnapshot {
		log.Fatalf("raftexample: error loading snapshot (%v)", err)
	}
	return snapshot
}

// openWAL returns a WAL ready for reading.
func (rc *raftNode) openWAL(snapshot *raftpb.Snapshot) *wal.WAL {
	if !wal.Exist(rc.waldir) {
		if err := os.Mkdir(rc.waldir, 0750); err != nil {
			log.Fatalf("raftexample: cannot create dir for wal (%v)", err)
		}

		w, err := wal.Create(zap.NewExample(), rc.waldir, nil)
		if err != nil {
			log.Fatalf("raftexample: create wal error (%v)", err)
		}
		w.Close()
	}

	walsnap := walpb.Snapshot{}
	if snapshot != nil {
		walsnap.Index, walsnap.Term = snapshot.Metadata.Index, snapshot.Metadata.Term
	}
	log.Printf("loading WAL at term %d and index %d", walsnap.Term, walsnap.Index)
	w, err := wal.Open(zap.NewExample(), rc.waldir, walsnap)
	if err != nil {
		log.Fatalf("raftexample: error loading wal (%v)", err)
	}

	return w
}

// replayWAL replays WAL entries into the raft instance.
func (rc *raftNode) replayWAL() *wal.WAL {
	log.Printf("replaying WAL of member %d", rc.id)
	snapshot := rc.loadSnapshot()
	w := rc.openWAL(snapshot)
	_, st, ents, err := w.ReadAll() //读取快照数据之后的全部WAL 日志数据，并获取状态信息
	if err != nil {
		log.Fatalf("raftexample: failed to read WAL (%v)", err)
	}
	rc.raftStorage = raft.NewMemoryStorage()
	if snapshot != nil {
		rc.raftStorage.ApplySnapshot(*snapshot)
	}
	rc.raftStorage.SetHardState(st)

	// append to storage so raft starts at the right place in log
	rc.raftStorage.Append(ents)
	// send nil once lastIndex is published so client knows commit channel is current
	if len(ents) > 0 {
		rc.lastIndex = ents[len(ents)-1].Index
	} else {
		rc.commitC <- nil
	}
	return w
}

func (rc *raftNode) writeError(err error) {
	rc.stopHTTP()
	close(rc.commitC)
	rc.errorC <- err
	close(rc.errorC)
	rc.node.Stop()
}

func (rc *raftNode) startRaft() {
	if !fileutil.Exist(rc.snapdir) {
		if err := os.Mkdir(rc.snapdir, 0750); err != nil {
			log.Fatalf("raftexample: cannot create dir for snapshot (%v)", err)
		}
	}
	rc.snapshotter = snap.New(zap.NewExample(), rc.snapdir)
	rc.snapshotterReady <- rc.snapshotter

	oldwal := wal.Exist(rc.waldir)
	rc.wal = rc.replayWAL()

	rpeers := make([]raft.Peer, len(rc.peers))
	for i := range rpeers {
		rpeers[i] = raft.Peer{ID: uint64(i + 1)}
	}
	c := &raft.Config{
		ID:                        uint64(rc.id),
		ElectionTick:              10, // election timeout 时间
		HeartbeatTick:             1,
		Storage:                   rc.raftStorage, //持久化存储 与etcd-raft模块的raftLog.storage共享一个MemoryStorage实例
		MaxSizePerMsg:             1024 * 1024,    //每条消息最大长度
		MaxInflightMsgs:           256,            //已发送但是 没收到响应的消息的最大个数
		MaxUncommittedEntriesSize: 1 << 30,
	}

	// 初始化 并启动 etcd-raft模块
	if oldwal {
		rc.node = raft.RestartNode(c)
	} else {
		startPeers := rpeers
		if rc.join {
			startPeers = nil
		}
		rc.node = raft.StartNode(c, startPeers)
	}

	//创建Transport 实例并启动，它负责raft 节点之间通信的网络服务
	rc.transport = &rafthttp.Transport{
		Logger:      zap.NewExample(),
		ID:          types.ID(rc.id),
		ClusterID:   0x1000,
		Raft:        rc,
		ServerStats: stats.NewServerStats("", ""),
		LeaderStats: stats.NewLeaderStats(zap.NewExample(), strconv.Itoa(rc.id)),
		ErrorC:      make(chan error),
	}

	rc.transport.Start() //启动网络相关组件
	//建立与集群中其他各个节点的连接
	for i := range rc.peers {
		if i+1 != rc.id {
			rc.transport.AddPeer(types.ID(i+1), []string{rc.peers[i]})
		}
	}

	go rc.serveRaft()     //启动一个goroutine ，其中会监听当前节点与集群中其他节点之间的网络连接
	go rc.serveChannels() //启动后台goroutine 处理上层应用与底层eted-raft 模块的交互
}

// stop closes http, closes all channels, and stops raft.
func (rc *raftNode) stop() {
	rc.stopHTTP()
	close(rc.commitC)
	close(rc.errorC)
	rc.node.Stop()
}

func (rc *raftNode) stopHTTP() {
	rc.transport.Stop()
	close(rc.httpstopc)
	<-rc.httpdonec
}

//通知上层模块加载新生成的快照数据，并使用新快照的元数据更新raftNode 中的相应字段
func (rc *raftNode) publishSnapshot(snapshotToSave raftpb.Snapshot) {
	if raft.IsEmptySnap(snapshotToSave) {
		return
	}

	log.Printf("publishing snapshot at index %d", rc.snapshotIndex)
	defer log.Printf("finished publishing snapshot at index %d", rc.snapshotIndex)

	if snapshotToSave.Metadata.Index <= rc.appliedIndex {
		log.Fatalf("snapshot index [%d] should > progress.appliedIndex [%d]", snapshotToSave.Metadata.Index, rc.appliedIndex)
	}
	//使用commitC 通过知上层应用 加载新生成的快照数据
	rc.commitC <- nil // trigger kvstore to load snapshot

	//记录新快照的元数据
	rc.confState = snapshotToSave.Metadata.ConfState
	rc.snapshotIndex = snapshotToSave.Metadata.Index
	rc.appliedIndex = snapshotToSave.Metadata.Index
}

var snapshotCatchUpEntriesN uint64 = 10000

//为了释放底层etcd-raft模块中无用的Entry 记录，节点每处理指定条数（默认是10000 条）
//的记录之后，就会触发一次快照生成操作，相关实现在raftNode.maybeTriggerSnapshot()方法中，
func (rc *raftNode) maybeTriggerSnapshot() {
	if rc.appliedIndex-rc.snapshotIndex <= rc.snapCount {//检测处理的记录数是否足够，如果不足，则直接返回
		return
	}

	log.Printf("start snapshot [applied index: %d | last snapshot index: %d]", rc.appliedIndex, rc.snapshotIndex)
	//获取快照数据，在raftexample 示例中是获取kvstore 中记录的全部键值对数据，异常处理（略）
	data, err := rc.getSnapshot()
	if err != nil {
		log.Panic(err)
	}
	snap, err := rc.raftStorage.CreateSnapshot(rc.appliedIndex, &rc.confState, data)//创建Snapshot实例，同时也会将快照和元数据更新到raftLog .MemoryStorage中.
	if err != nil {
		panic(err)
	}
	if err := rc.saveSnap(snap); err != nil {  //保存快照数据
		panic(err)
	}

	compactIndex := uint64(1) //计算压缩的位置， 压缩之后，该位置之前的全部记录都会被抛弃
	if rc.appliedIndex > snapshotCatchUpEntriesN {
		compactIndex = rc.appliedIndex - snapshotCatchUpEntriesN
	}
	if err := rc.raftStorage.Compact(compactIndex); err != nil {  //压缩raftLog 中保存的Entry 记录
		panic(err)
	}

	log.Printf("compacted log at index %d", compactIndex)
	rc.snapshotIndex = rc.appliedIndex
}

func (rc *raftNode) serveChannels() {
	snap, err := rc.raftStorage.Snapshot()
	if err != nil {
		panic(err)
	}
	rc.confState = snap.Metadata.ConfState
	rc.snapshotIndex = snap.Metadata.Index
	rc.appliedIndex = snap.Metadata.Index

	defer rc.wal.Close()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	// send proposals over raft
	go func() {
		confChangeCount := uint64(0)

		for rc.proposeC != nil && rc.confChangeC != nil {
			select {
			case prop, ok := <-rc.proposeC:
				if !ok {
					rc.proposeC = nil
				} else {
					// blocks until accepted by raft state machine
					rc.node.Propose(context.TODO(), []byte(prop))
				}

			case cc, ok := <-rc.confChangeC:
				if !ok {
					rc.confChangeC = nil
				} else {
					confChangeCount++
					cc.ID = confChangeCount
					rc.node.ProposeConfChange(context.TODO(), cc)
				}
			}
		}
		// client closed channel; shutdown raft if not already
		close(rc.stopc)
	}()

	// event loop on raft state machine updates
	for {
		select {
		case <-ticker.C:
			rc.node.Tick()

		// store raft entries to wal, then publish over commit channel
		case rd := <-rc.node.Ready():
			//将当前etcd raft 组件的状态信息，以及待持久化的Entry 记录先记录到WAL 日志文件中，
			//即使之后宕机，这些信息也可以在节点下次启动时，通过前面回放WAL 日志的方式进行恢复。 WAL 记录日志的具体实现，
			rc.wal.Save(rd.HardState, rd.Entries)
			if !raft.IsEmptySnap(rd.Snapshot) {
				rc.saveSnap(rd.Snapshot)
				rc.raftStorage.ApplySnapshot(rd.Snapshot)
				rc.publishSnapshot(rd.Snapshot)
			}
			rc.raftStorage.Append(rd.Entries)
			rc.transport.Send(rd.Messages)
			if ok := rc.publishEntries(rc.entriesToApply(rd.CommittedEntries)); !ok {
				rc.stop()
				return
			}
			rc.maybeTriggerSnapshot()
			rc.node.Advance()

		case err := <-rc.transport.ErrorC:
			rc.writeError(err)
			return

		case <-rc.stopc:
			rc.stop()
			return
		}
	}
}

func (rc *raftNode) serveRaft() {
	url, err := url.Parse(rc.peers[rc.id-1])
	if err != nil {
		log.Fatalf("raftexample: Failed parsing URL (%v)", err)
	}

	ln, err := newStoppableListener(url.Host, rc.httpstopc)
	if err != nil {
		log.Fatalf("raftexample: Failed to listen rafthttp (%v)", err)
	}

	err = (&http.Server{Handler: rc.transport.Handler()}).Serve(ln)
	select {
	case <-rc.httpstopc:
	default:
		log.Fatalf("raftexample: Failed to serve rafthttp (%v)", err)
	}
	close(rc.httpdonec)
}

func (rc *raftNode) Process(ctx context.Context, m raftpb.Message) error {
	return rc.node.Step(ctx, m)
}
func (rc *raftNode) IsIDRemoved(id uint64) bool                           { return false }
func (rc *raftNode) ReportUnreachable(id uint64)                          {}
func (rc *raftNode) ReportSnapshot(id uint64, status raft.SnapshotStatus) {}
