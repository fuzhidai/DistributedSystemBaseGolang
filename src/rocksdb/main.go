package main

import (
	"fmt"
	"src/github.com/tecbot/gorocksdb"
)

func main() {

	bbto := gorocksdb.NewDefaultBlockBasedTableOptions()
	bbto.SetBlockCache(gorocksdb.NewLRUCache(3 << 30))
	opts := gorocksdb.NewDefaultOptions()
	opts.SetBlockBasedTableFactory(bbto)
	opts.SetCreateIfMissing(true)
	db, _ := gorocksdb.OpenDb(opts, "./db")

	ro := gorocksdb.NewDefaultReadOptions()
	wo := gorocksdb.NewDefaultWriteOptions()
	// if ro and wo are not used again, be sure to Close them.
	_ = db.Put(wo, []byte("foo"), []byte("bar"))
	value, _ := db.Get(ro, []byte("foo"))
	defer value.Free()
	fmt.Println("value: ", string(value.Data()[:]))
	_ = db.Delete(wo, []byte("foo"))
}
