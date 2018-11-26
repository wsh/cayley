package datastore

import (
	"context"
	"fmt"
	"log"
	"reflect"
	"testing"

	"cloud.google.com/go/datastore"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/nosql"
	"github.com/cayleygraph/cayley/graph/nosql/nosqltest"
)

func makeDatastore(t testing.TB) (nosql.Database, *nosql.Options, graph.Options, func()) {
	ctx := context.Background()
	succ, err := makeDS(ctx)
	if err != nil {
		log.Fatal(err)
	}
	return succ, nil, nil, func() {
	}
}

var conf = &nosqltest.Config{
	TimeInMs: true,
}

func TestType(t *testing.T) {
	doc := make(Doc)
	foo := reflect.TypeOf(doc)
	fmt.Printf("typeof: %v", foo)
	type2 := reflect.TypeOf((*datastore.PropertyLoadSaver)(nil)).Elem()
	fmt.Println(reflect.PtrTo(foo).Implements(type2))
}

func TestScratch(t *testing.T) {
	ctx := context.Background()
	succ, err := makeDS(ctx)
	if err != nil {
		t.Error(err)
	}
	doc := &nosql.Document{"foo": nosql.String("bar")}
	_, err = succ.Insert(ctx, "Kind", []string{"EG"}, *doc)
	if err != nil {
		t.Error(err)
	}
	gotten := make(Doc)
	err = succ.client.Get(ctx, datastore.NameKey("Kind", "EG", nil), gotten)
	if err != nil {
		t.Error(err)
	}
	t.Log(&gotten)
	err = succ.client.Delete(ctx, datastore.NameKey("Kind", "EG2", nil))
	if err != nil {
		t.Error(err)
	}
	incKey := datastore.IncompleteKey("Fart", nil)
	foo := []*datastore.Key{incKey}
	keys, err := succ.client.AllocateIDs(ctx, foo)
	if err != nil {
		t.Error(err)
	}
	t.Log(keys)
	t.Log("here")
	succ.Close()
}

func TestDatastore(t *testing.T) {
	nosqltest.TestAll(t, makeDatastore, conf)
}

func BenchmarkDatastore(t *testing.B) {
	nosqltest.BenchmarkAll(t, makeDatastore, conf)
}
