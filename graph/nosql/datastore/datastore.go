package datastore

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/datastore"
	"github.com/cayleygraph/cayley/graph/nosql"
	"google.golang.org/api/iterator"
)

const Type = "datastore"

type DS struct {
	client *datastore.Client
}

type Doc nosql.Document

func makeDS(ctx context.Context) (*DS, error) {
	client, err := datastore.NewClient(ctx, os.Getenv("DATASTORE_PROJECT_ID"))
	if err != nil {
		log.Fatal(err)
	}
	return &DS{client: client}, err
}

func (ds *DS) Insert(ctx context.Context, col string, key nosql.Key, d nosql.Document) (nosql.Key, error) {
	dsKey := convNoSQLKeyToDatastoreKey(col, key)
	allocatedKey, err := ds.client.Put(ctx, dsKey, Doc(d))
	if err != nil {
		return nil, err
	}
	if key == nil {
		_, key = convDatastoreKeyToColAndNoSQLKey(*allocatedKey)
	}
	return key, err
}

func (d Doc) Load(ps []datastore.Property) error {
	for _, p := range ps {
		val, err := convertDatastoreValueToNoSQLValue(p.Value)
		if err != nil {
			return err
		}
		d[p.Name] = val
	}
	return nil
}

func convertDatastoreValueToNoSQLValue(dsv interface{}) (nosql.Value, error) {
	switch tv := dsv.(type) {
	case nil:
		return nil, nil
	case bool:
		return nosql.Bool(tv), nil
	case []byte:
		return nosql.Bytes(tv), nil
	case float64:
		return nosql.Float(tv), nil
	case int64:
		return nosql.Int(tv), nil
	case string:
		return nosql.String(tv), nil
	case []interface{}:
		res := make([]string, len(tv))
		for i, v := range tv {
			c, err := convertDatastoreValueToNoSQLValue(v)
			if err != nil {
				return nil, err
			}
			switch tc := c.(type) {
			case nosql.String:
				res[i] = string(tc)
				continue
			default:
				panic("Unexpected, cayley/nosql does not have heterogeneous array Value types")
			}
		}
		return nosql.Strings(res), nil
	case time.Time:
		return nosql.Time(tv), nil
	case *datastore.Entity:
		properties := tv.Properties
		doc := make(Doc, len(properties))
		doc.Load(properties)
		return nosql.Document(doc), nil
	// case *datastore.Key:
	// 	col, key := convDatastoreKeyToColAndNoSQLKey(*tv)
	// 	segs := append(make([]string, len(key)+1), col)
	// 	segs = append(segs, key...)
	// 	return nosql.Strings(segs), nil
	default:
		return nil, fmt.Errorf("Unexpected type for value: %v", dsv)
	}
}

func (d Doc) Save() ([]datastore.Property, error) {
	props := make([]datastore.Property, len(d))
	i := 0
	for k, v := range d {
		val, err := convertNoSQLValueToDatastoreValue(v)
		if err != nil {
			return nil, err
		}
		props[i] = datastore.Property{
			Name:  k,
			Value: val,
		}
		i++
	}
	return props, nil
}

func convertNoSQLValueToDatastoreValue(nsv nosql.Value) (interface{}, error) {
	switch vt := nsv.(type) {
	case nosql.Bool:
		return bool(vt), nil
	case nosql.Bytes:
		return []byte(vt), nil
	case nosql.Float:
		return float64(vt), nil
	case nosql.Int:
		return int64(vt), nil
	case nosql.String:
		return string(vt), nil
	case nosql.Time:
		return time.Time(vt), nil
	case nosql.Strings:
		s := make([]interface{}, len(vt))
		for i, ns := range vt {
			s[i] = ns
		}
		return s, nil
	case nosql.Document:
		pl, err := Doc(vt).Save()
		if err != nil {
			return nil, err
		}
		return &datastore.Entity{Properties: pl}, nil
	default:
		return nil, fmt.Errorf("Unknown type for value %v", vt)
	}
}

func convNoSQLKeyToDatastoreKey(col string, key nosql.Key) *datastore.Key {
	if key == nil {
		return datastore.IncompleteKey(col, nil)
	}
	if len(key) == 1 {
		id, err := strconv.Atoi(key[0])
		if err == nil {
			// This looks like an automatically assigned key.
			return datastore.IDKey(col, int64(id), nil)
		}
	}
	return datastore.NameKey(col, strings.Join(key, "|"), nil)
}

func convDatastoreKeyToColAndNoSQLKey(key datastore.Key) (string, nosql.Key) {
	if key.ID != 0 {
		return key.Kind, []string{fmt.Sprintf("%v", key.ID)}
	}
	return key.Kind, strings.Split(key.Name, "|")
}

func (ds *DS) FindByKey(ctx context.Context, col string, key nosql.Key) (nosql.Document, error) {
	dsKey := convNoSQLKeyToDatastoreKey(col, key)
	doc := make(Doc)
	err := ds.client.Get(ctx, dsKey, doc)
	if err != nil {
		switch err.Error() {
		case "datastore: no such entity":
			return nil, fmt.Errorf("not found")
		default:
			return nil, err
		}
	}
	nd := nosql.Document(doc)
	// TODO: nosql.Strings? eliminate?
	if dsKey.ID != 0 {
		nd["id"] = nosql.String(fmt.Sprintf("%v", dsKey.ID))
	} else if dsKey.Name != "" {
		nd["id"] = nosql.String(strings.Split(dsKey.Name, "|")[0])
	} else {
		// This is an 'incomplete' key. We never expect this.
		panic("Incomplete key in Get result?!")
	}
	return nd, nil
}

func (ds *DS) Query(col string) nosql.Query {
	return &Query{client: ds.client, col: col, dsq: datastore.NewQuery(col)}
}

type Query struct {
	client  *datastore.Client
	col     string
	dsq     *datastore.Query
	dit     *datastore.Iterator
	cursor  datastore.Cursor
	nextKey *nosql.Key
	nextDoc *nosql.Document
	err     error
}

func (q *Query) WithFields(filters ...nosql.FieldFilter) nosql.Query {
	for _, f := range filters {
		op, err := convertFilterOpToDatastore(f.Filter)
		if err != nil {
			panic(err)
		}
		// TODO: properly convert field path?
		fexp := fmt.Sprintf("%s%s", f.Path[0], op)
		val, err := convertNoSQLValueToDatastoreValue(f.Value)
		if err != nil {
			panic(err)
		}
		q.dsq = q.dsq.Filter(fexp, val)
	}

	return q
}

func convertFilterOpToDatastore(op nosql.FilterOp) (string, error) {
	switch op {
	case nosql.Equal:
		return "=", nil
	case nosql.NotEqual:
		return "", fmt.Errorf("!= not supported by Datastore")
	case nosql.GT:
		return ">", nil
	case nosql.GTE:
		return ">=", nil
	case nosql.LT:
		return "<", nil
	case nosql.LTE:
		return "<=", nil
	}
	return "", fmt.Errorf("Unrecognized operator %v", op)
}

func (q *Query) Limit(n int) nosql.Query {
	q.dsq = q.dsq.Limit(n)
	return q
}

func (q *Query) Count(ctx context.Context) (int64, error) {
	i64, err := q.client.Count(ctx, q.dsq)
	return int64(i64), err
}

func (q *Query) One(ctx context.Context) (nosql.Document, error) {
	err := q.vitalize(ctx)
	if err != nil {
		q.err = err
		return nil, err
	}
	hasNext := q.Next(ctx)
	if !hasNext {
		return nil, fmt.Errorf("no docs")
	}
	return *q.nextDoc, nil
}

func (q *Query) Iterate() nosql.DocIterator {
	return q
}

func (q *Query) Next(ctx context.Context) bool {
	err := q.vitalize(ctx)
	if err != nil {
		q.err = err
		return false
	}
	doc := make(Doc)
	key, err := q.dit.Next(&doc)
	if err != nil {
		if err == iterator.Done {
			return false
		}
		q.err = err
		panic(fmt.Errorf("Not sure what to do: %v", err))
	}
	_, k := convDatastoreKeyToColAndNoSQLKey(*key)
	q.nextKey = &k
	nd := nosql.Document(doc)
	// TODO: remove this if possible
	if key.ID != 0 {
		nd["id"] = nosql.String(fmt.Sprintf("%v", key.ID))
	} else if key.Name != "" {
		nd["id"] = nosql.String(strings.Split(key.Name, "|")[0])
	} else {
		// This is an 'incomplete' key. We never expect this.
		panic("Incomplete key in Get result?!")
	}
	q.nextDoc = &nd
	return true
}

func (q *Query) Key() nosql.Key {
	return *q.nextKey
}

func (q *Query) Doc() nosql.Document {
	return *q.nextDoc
}

func (q *Query) Close() error {
	return nil
}

func (q *Query) Err() error {
	return q.err
}

func (q *Query) vitalize(ctx context.Context) error {
	if q.dit == nil {
		q.dsq = q.dsq.Start(q.cursor)
		q.dit = q.client.Run(ctx, q.dsq)
		cur, err := q.dit.Cursor()
		if err != nil {
			return err
		}
		q.cursor = cur
	}
	return nil
}

func (ds *DS) Update(col string, key nosql.Key) nosql.Update {
	return &Update{client: ds.client, col: col, key: key}
}

type Update struct {
	client *datastore.Client
	col    string
	key    nosql.Key
	inc    map[string]int
	upsert nosql.Document
}

func (upd Update) Do(ctx context.Context) error {
	key := convNoSQLKeyToDatastoreKey(upd.col, upd.key)
	upd.client.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
		ex := make(Doc)
		tx.Get(key, ex)
		update := upd.upsert
		if update == nil {
			update = make(nosql.Document)
		}
		for k, v := range upd.inc {
			if update[k] != nil {
				return fmt.Errorf("Increment/upsert clash for path %s", k)
			}
			existing := ex[k]
			switch tv := existing.(type) {
			case nosql.Float:
				update[k] = nosql.Float(float64(tv) + float64(v))
			case nosql.Int:
				update[k] = nosql.Int(int64(tv) + int64(v))
			default:
				return fmt.Errorf("can't handle type %v", tv)
			}
		}
		_, err := tx.Put(key, Doc(update))
		return err
	})
	return nil
}

func (upd Update) Inc(field string, n int) nosql.Update {
	if upd.inc == nil {
		upd.inc = make(map[string]int)
	}
	upd.inc[field] = n
	return upd
}

func (upd Update) Upsert(doc nosql.Document) nosql.Update {
	upd.upsert = doc
	return upd
}

func (ds *DS) Delete(col string) nosql.Delete {
	return &Delete{client: ds.client, col: col}
}

type Delete struct {
	client  *datastore.Client
	col     string
	keys    []nosql.Key
	filters []nosql.FieldFilter
}

func (del Delete) WithFields(filters ...nosql.FieldFilter) nosql.Delete {
	del.filters = append(del.filters, filters...)
	return del
}

func (del Delete) Keys(keys ...nosql.Key) nosql.Delete {
	del.keys = append(del.keys, keys...)
	return del
}

func (del Delete) Do(ctx context.Context) error {
	if del.filters != nil {
		return fmt.Errorf("Datastore doesn't support field filtering yet")
	}
	var keys []*datastore.Key
	for _, key := range del.keys {
		keys = append(keys, convNoSQLKeyToDatastoreKey(del.col, key))
	}
	return del.client.DeleteMulti(ctx, keys)
}

func (ds *DS) EnsureIndex(ctx context.Context, col string, primary nosql.Index, secondary []nosql.Index) error {
	return nil
}

func (ds *DS) Close() error {
	return ds.client.Close()
}
