package shardctrler

import "sort"

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

func copyConfig(config Config) Config {
	newConfig := Config{
		Num:    config.Num,
		Shards: config.Shards,
		Groups: make(map[int][]string),
	}
	for gid, servers := range config.Groups {
		newSlice := make([]string, len(servers))
		for i, server := range servers {
			newSlice[i] = server
		}
		newConfig.Groups[gid] = newSlice
	}
	// DPrintf("copy config, newConfig have %d group", len(newConfig.Groups))
	return newConfig
}

func getMaxShardGid(gidShardMap map[int][]int) int {
	if shard, ok := gidShardMap[0]; ok && len(shard) > 0 {
		return 0
	}
	// make iteration deterministic
	gids := make([]int, 0)
	for gid := range gidShardMap {
		gids = append(gids, gid)
	}
	sort.Ints(gids)
	res, max := -1, -1
	for _, gid := range gids {
		if len(gidShardMap[gid]) > max {
			res = gid
			max = len(gidShardMap[gid])
		}
	}
	return res
}

func getMinShardGid(gidShardMap map[int][]int) int {
	// make iteration deterministic
	gids := make([]int, 0)
	for gid := range gidShardMap {
		gids = append(gids, gid)
	}
	sort.Ints(gids)
	res, min := -1, NShards+1
	for _, gid := range gids {
		if gid != 0 && len(gidShardMap[gid]) < min {
			res = gid
			min = len(gidShardMap[gid])
		}
	}
	return res
}

const (
	OK         = "OK"
	ErrTimeout = "ErrTimeout"
	ErrFailed  = "ErrFailed"
)

type Err string

type JoinArgs struct {
	Servers   map[int][]string // new GID -> servers mappings
	ClerkId   int64
	RequestNo int64
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	GIDs      []int
	ClerkId   int64
	RequestNo int64
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	Shard     int
	GID       int
	ClerkId   int64
	RequestNo int64
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	Num       int // desired config number
	ClerkId   int64
	RequestNo int64
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}
