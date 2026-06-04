package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"time"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/tikv/client-go/v2/config/retry"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/txnkv"
	"github.com/tikv/client-go/v2/txnkv/transaction"
)

const (
	defaultPDAddr        = "127.0.0.1:2379"
	defaultStatusAddress = "127.0.0.1:20180"
	fixedKeyspaceName    = "SYSTEM"
	childTableID         = math.MaxInt64 - 10
	parentTableID        = childTableID - 1
	lockTTLms            = 500
	sleepAfterTTL        = 1200 * time.Millisecond
)

// main reproduces the duplicated-keyspace-prefix rollback path in a
// foreign-key-like transaction shape:
//
//   - child row: exclusive lock + normal write
//   - parent row: shared lock
//
// The key point is that API v2 has two different key views:
//
//   - client-go logical key: parent
//   - storage/RPC key:       <keyspace-prefix> + parent
//
// The bug requires the same parent-row entry inside SharedLocks to first move
// from a shared pessimistic lock into a shared prewrite lock. Then a later
// conflicting prewrite asks storage for the conflicting lock. Storage returns a
// SharedLock wrapper whose outer key and inner SharedLockInfos keys are both
// storage/RPC keys. client-go's API-v2 response codec decodes the outer
// LockInfo key, but does not recursively decode SharedLockInfos. The inner key
// is therefore still <keyspace-prefix>+parent when the lock resolver consumes
// it. The resolver then builds a ResolveLock request from that inner key, and
// the API-v2 request codec adds <keyspace-prefix> again. The rollback is written
// under <keyspace-prefix>+<keyspace-prefix>+parent.
//
// ASCII flow:
//
//	T1: child-row writer with FK check              T2: conflicting parent-row writer
//	----------------------------------              ---------------------------------
//	LockKeys(child) exclusive
//	  client-go API-v2 request codec:
//	    child -> P+child
//
//	LockKeys(parent) in share mode
//	  client-go API-v2 request codec:
//	    parent -> P+parent
//	  storage stores SharedLocks at key:
//	    P+parent
//
//	prewrite child Put + parent SharedLock
//	  storage updates SharedLocks[P+parent]:
//	    shared pessimistic holder -> shared prewrite Lock holder
//
//	wait for TTL to expire
//
//	                                                prewrite parent Put
//	                                                  client-go API-v2 request codec:
//	                                                    parent -> P+parent
//	                                                  storage returns KeyIsLocked:
//	                                                    outer.key = P+parent
//	                                                    inner[0].key = P+parent
//
//	                                                client-go API-v2 response codec:
//	                                                  outer.key: P+parent -> parent
//	                                                  inner[0].key: unchanged P+parent
//	                                                    (SharedLockInfos not decoded)
//
//	                                                lock resolver resolves inner[0]
//	                                                  ResolveLockRequest.keys = P+parent
//	                                                  API-v2 request codec adds prefix:
//	                                                    P+parent -> P+P+parent
//
//	                                                storage rolls back missing lock at:
//	                                                  P+P+parent
//
//	                                                /flush sees malformed ordering:
//	                                                  normal table keys and P+P+parent
//	                                                  -> OutOfOrder(...)
//
// Modes:
//   - warmup: use table-row-shaped child/parent keys, but do not flush yet.
//     This seeds the target shard with a realistic FK-like pattern.
//   - repro: use raw x... child/parent keys to generate the malformed
//     rollback, then call /flush and wait for OutOfOrder.
func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	pdAddr := flag.String("pd", envOr("PD_ADDR", defaultPDAddr), "PD address")
	statusAddr := flag.String("status-addr", envOr("STATUS_ADDR", defaultStatusAddress), "TiKV status server address host:port")
	flushTimeout := flag.Duration("flush-timeout", 10*time.Second, "timeout for the final /flush request")
	mode := flag.String("mode", "repro", "run mode: warmup or repro")
	flag.Parse()

	ctx := context.Background()

	// Connect through API v2 and the SYSTEM keyspace because the bug requires the
	// keyspace prefix codec to participate in both decode and re-encode paths.
	client, err := txnkv.NewClient(
		[]string{*pdAddr},
		txnkv.WithAPIVersion(kvrpcpb.APIVersion_V2),
		txnkv.WithKeyspace(fixedKeyspaceName),
	)
	must(err)
	defer func() {
		must(client.Close())
	}()

	store := tikv.StoreProbe{KVStore: client.KVStore}
	ksMeta, err := client.GetPDClient().LoadKeyspace(ctx, fixedKeyspaceName)
	must(err)

	codec, err := tikv.NewCodecV2(tikv.ModeTxn, ksMeta)
	must(err)

	tag := fmt.Sprintf("%d", time.Now().UnixNano())
	// The two modes only change the concrete encoding of the child/parent key
	// pair. The transaction shape itself stays aligned with the FK path.
	plan, err := keysForMode(*mode, tag)
	must(err)
	childValue := []byte("txn1-child-row")
	parentRewriteValue := []byte("v2-nextgen-parent-rewrite")

	log.Printf("mode=%s keyspace=%s id=%d prefix=%x", *mode, fixedKeyspaceName, ksMeta.GetId(), codec.GetKeyspace())
	log.Printf(
		"keys child=%x parent=%x flush=%v child-table-id=%d parent-table-id=%d status=%s timeout=%s",
		plan.childKey,
		plan.parentKey,
		plan.shouldFlush,
		childTableID,
		parentTableID,
		*statusAddr,
		*flushTimeout,
	)

	// T1 does the child-row write and the parent-row FK check. The important
	// transition is that the same parent-row entry inside SharedLocks later
	// moves from the pessimistic-lock stage into the prewrite-lock stage.
	txn1, err := store.Begin()
	must(err)
	txn1.SetPessimistic(true)
	defer func() {
		_ = txn1.Rollback()
	}()

	forUpdateTS := getTS(&store)
	// Lock the child row exclusively, matching the FK child-table write path.
	must(txn1.LockKeys(ctx, kv.NewLockCtx(forUpdateTS, 1000, time.Now()), plan.childKey))
	// Acquire the parent-row shared lock through the FK check path.
	sharedLockCtx := kv.NewLockCtx(forUpdateTS, 1000, time.Now())
	sharedLockCtx.InShareMode = true
	must(txn1.LockKeys(ctx, sharedLockCtx, plan.parentKey))
	// Write the child row so the transaction has a regular primary/write key in
	// addition to the shared parent-row lock.
	must(txn1.Set(plan.childKey, childValue))

	committer1, err := txn1.NewCommitter(1001)
	must(err)
	// The probe-style setup does not always carry forUpdateTS into the committer.
	// Patch it explicitly so PrewriteAllMutations behaves like a real pessimistic
	// transaction entering 2PC prewrite.
	if committer1.GetForUpdateTS() == 0 {
		committer1.SetForUpdateTS(forUpdateTS)
		log.Printf("patched txn1 committer forUpdateTS=%d", forUpdateTS)
	}
	committer1.SetLockTTL(lockTTLms)
	txn1.SetCommitter(committer1)
	// Force T1 into the exact window we care about: the child-row write is
	// prewritten, and the same parent-row entry inside SharedLocks has been
	// updated from a pessimistic owner into a prewrite Lock owner.
	must(committer1.PrewriteAllMutations(ctx))
	log.Printf("txn1 prewrite done: start_ts=%d ttl=%d", txn1.StartTS(), committer1.GetLockTTL())

	// Let the prewrite lock expire so the later resolver path treats the inner
	// holder as a stale lock that should be rolled back.
	log.Printf("sleeping %s to let prewrite lock expire", sleepAfterTTL)
	time.Sleep(sleepAfterTTL)

	// T2 models another transaction that now wants to rewrite the parent row. It
	// does not need to commit; we only need its prewrite request to bounce on
	// T1's shared-prewrite state and return the nested SharedLockInfos payload.
	txn2, err := store.Begin()
	must(err)
	must(txn2.Set(plan.parentKey, parentRewriteValue))
	committer2, err := txn2.NewCommitter(1002)
	must(err)
	txn2.SetCommitter(committer2)

	// Send prewrite manually so we can inspect the raw Locked response instead of
	// letting the normal client retry/resolve path hide the interesting details.
	bo := retry.NewBackofferWithVars(ctx, int(transaction.PrewriteMaxBackoff.Load()), nil)
	loc, err := store.GetRegionCache().LocateKey(bo, plan.parentKey)
	must(err)
	req := committer2.BuildPrewriteRequest(
		loc.Region.GetID(),
		loc.Region.GetConfVer(),
		loc.Region.GetVer(),
		committer2.GetMutations(),
		uint64(committer2.GetMutations().Len()),
	)
	resp, err := store.SendReq(bo, req, loc.Region, 10*time.Second)
	must(err)
	prewriteResp := resp.Resp.(*kvrpcpb.PrewriteResponse)
	if len(prewriteResp.Errors) == 0 {
		log.Fatalf("expected prewrite conflict, got no key errors")
	}
	locked := prewriteResp.Errors[0].GetLocked()
	if locked == nil {
		log.Fatalf("expected locked error, got: %+v", prewriteResp.Errors[0])
	}
	log.Printf("prewrite locked wrapper: type=%s key=%x primary=%x shared_infos=%d", opString(locked.GetLockType()), locked.GetKey(), locked.GetPrimaryLock(), len(locked.GetSharedLockInfos()))
	if len(locked.GetSharedLockInfos()) == 0 {
		log.Fatalf("expected shared_lock_infos in locked error")
	}
	inner := locked.GetSharedLockInfos()[0]
	// This is the heart of the bug:
	//   - outer wrapper key is decoded back to the raw user key
	//   - inner shared_lock_infos[0].key is still API-v2-prefixed
	// Re-encoding it through the same codec yields prefix+prefix+raw.
	log.Printf("inner shared lock: type=%s key=%x primary=%x", opString(inner.GetLockType()), inner.GetKey(), inner.GetPrimaryLock())
	log.Printf("inner key has raw shared suffix=%v", bytes.HasSuffix(inner.GetKey(), plan.parentKey))
	log.Printf("codec.EncodeKey(inner.Key)=%x", codec.EncodeKey(inner.GetKey()))

	// Rebuild the resolver's lock object from the nested inner lock and force the
	// same rollback-cleanup path that production client-go would take.
	badLock := txnkv.NewLock(inner)
	log.Printf("constructed lock for resolver: type=%s key=%x primary=%x", opString(badLock.LockType), badLock.Key, badLock.Primary)

	resolver := store.NewLockResolver()
	must(resolver.ForceResolveLock(ctx, badLock))
	log.Printf("force resolve lock finished")

	if plan.shouldFlush {
		// Flush is only needed in repro mode. The duplicated-prefix rollback is the
		// immediate bug, but the production symptom is that nextgen memtable flush
		// later fails with OutOfOrder once malformed and normal keys are ordered
		// together inside the same shard.
		flushCtx, cancel := context.WithTimeout(ctx, *flushTimeout)
		err := flushTable(flushCtx, *statusAddr, ksMeta.GetId())
		cancel()
		must(err)
		log.Printf("manual flush request finished")
	} else {
		log.Printf("warmup mode finished without flush")
	}

	log.Println("reproduction program completed")
}

type modePlan struct {
	childKey    []byte
	parentKey   []byte
	shouldFlush bool
}

// keysForMode returns the child/parent key pair used in each phase:
//   - warmup uses table-row-shaped child/parent keys
//   - repro uses raw x... child/parent keys
func keysForMode(mode, tag string) (modePlan, error) {
	switch mode {
	case "warmup":
		return modePlan{
			childKey:    rowKey(childTableID, 1),
			parentKey:   rowKey(parentTableID, 1),
			shouldFlush: false,
		}, nil
	case "repro":
		return modePlan{
			childKey:    []byte("xchild-" + tag),
			parentKey:   []byte("xparent-" + tag),
			shouldFlush: true,
		}, nil
	default:
		return modePlan{}, fmt.Errorf("unsupported mode: %s", mode)
	}
}

func getTS(store *tikv.StoreProbe) uint64 {
	ts, err := store.GetOracle().GetTimestamp(context.Background(), &oracle.Option{})
	must(err)
	return ts
}

// rowKey builds a TiDB row key by hand so warmup mode can place ordinary table
// writes into the same target shard without needing a SQL layer.
func rowKey(tableID, handle int64) []byte {
	key := make([]byte, 0, 1+8+2+8)
	key = append(key, 't')
	key = append(key, encodeInt(tableID)...)
	key = append(key, '_', 'r')
	key = append(key, encodeInt(handle)...)
	return key
}

func encodeInt(v int64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(v)^uint64(1<<63))
	return buf
}

// flushTable asks the TiKV status server to flush the shard selected by the
// fixed keyspace/table combination. The wrapper script terminates as soon as the
// first OutOfOrder log line appears, so this call only needs to start the flush
// work, not wait for retries forever.
func flushTable(ctx context.Context, statusAddr string, keyspaceID uint32) error {
	url := fmt.Sprintf("http://%s/flush?keyspace_id=%d&table_id=%d", statusAddr, keyspaceID, childTableID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	log.Printf("flush response status=%s body=%s", resp.Status, bytes.TrimSpace(body))
	if resp.StatusCode >= 300 {
		return fmt.Errorf("flush failed: %s", string(body))
	}
	return nil
}

func opString(op kvrpcpb.Op) string {
	if s, ok := kvrpcpb.Op_name[int32(op)]; ok {
		return s
	}
	return fmt.Sprintf("Op(%d)", op)
}

func must(err error) {
	if err != nil {
		log.Printf("fatal: %v", err)
		os.Exit(1)
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
