package raft

import (
	"src/github.com/tecbot/gorocksdb"
	"sync"
)

type Persistence struct {
	mu sync.Mutex

	db *gorocksdb.DB
	ro *gorocksdb.ReadOptions
	wo *gorocksdb.WriteOptions

	raftstate []byte
	snapshot  []byte
}

func MakePersistence() *Persistence {

	bbto := gorocksdb.NewDefaultBlockBasedTableOptions()
	bbto.SetBlockCache(gorocksdb.NewLRUCache(3 << 30))
	opts := gorocksdb.NewDefaultOptions()
	opts.SetBlockBasedTableFactory(bbto)
	opts.SetCreateIfMissing(true)

	persister := &Persistence{}
	if db, err := gorocksdb.OpenDb(opts, "./db"); err == nil {
		persister.db = db
		persister.ro = gorocksdb.NewDefaultReadOptions()
		persister.wo = gorocksdb.NewDefaultWriteOptions()
	} else {
		panic("error when open db.")
	}
	return persister
}

func (ps *Persistence) Copy() *Persistence {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	np := MakePersistence()
	np.raftstate = ps.raftstate
	np.snapshot = ps.snapshot
	return np
}

func (ps *Persistence) SaveRaftState(state []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	if err := ps.db.Put(ps.wo, []byte("raft_state"), state); err == nil {
		ps.raftstate = state
	} else {
		panic("error when write raft state to db.")
	}
}

func (ps *Persistence) ReadRaftState() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	if value, err := ps.db.Get(ps.ro, []byte("raft_state")); err == nil {
		ps.raftstate = value.Data()
		return ps.raftstate
	} else {
		ps.ro.Destroy()
		ps.wo.Destroy()
		panic("error when read raft state to db.")
	}
}

func (ps *Persistence) RaftStateSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.raftstate)
}

// Save both Raft state and K/V snapshot as a single atomic action,
// to help avoid them getting out of sync.
func (ps *Persistence) SaveStateAndSnapshot(state []byte, snapshot []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	if err := ps.db.Put(ps.wo, []byte("raft_state"), state); err == nil {
		ps.raftstate = state
	} else {
		panic("error when write raft state to db.")
	}

	if err := ps.db.Put(ps.wo, []byte("snapshot"), snapshot); err == nil {
		ps.snapshot = snapshot
	} else {
		panic("error when write snapshot to db.")
	}
}

func (ps *Persistence) ReadSnapshot() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	if value, err := ps.db.Get(ps.ro, []byte("snapshot")); err == nil {
		ps.snapshot = value.Data()
		return ps.snapshot
	} else {
		ps.ro.Destroy()
		ps.wo.Destroy()
		panic("error when read snapshot to db.")
	}
}

func (ps *Persistence) SnapshotSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.snapshot)
}
