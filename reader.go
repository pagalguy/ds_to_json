package ds_to_json

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"reflect"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/pagalguy/ds_to_json/google"
	pb "github.com/pagalguy/ds_to_json/google/pb"
	"github.com/syndtr/goleveldb/leveldb/journal"
	"google.golang.org/appengine"
)

type JSONChan chan *map[string]interface{}
type ErrorChan chan ReadError

type ReadError struct {
	Message string
	File    string
	Line    int
}

func NewReadError(msg, file string, line int) ReadError {
	return ReadError{
		Message: msg,
		File:    file,
		Line:    line,
	}
}

func (e *ReadError) Error() string {
	return fmt.Sprintf("File: %s. Line: #%d | %s", e.File, e.Line, e.Message)
}

// reads datastore backup file from path converts each entity into a JSON and outputs
// to a channels
func ReadDatastoreFile(path string, jsonChan JSONChan, errChan ErrorChan) error {

	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	// not sure why to use a journal reader. picked this up from a datastore to BQ repo for GO
	journals := journal.NewReader(f, nil, false, true)
	lineNo := 0

	for {
		j, err := journals.Next()
		if err != nil {
			// These are usually EOF errors, just break the loop.
			// no need to return
			break
		}
		b, err := ioutil.ReadAll(j)
		if err != nil {
			// These are usually EOF errors, just break the loop.
			// no need to return
			break
		}

		protoObj := &pb.EntityProto{}
		if err := proto.Unmarshal(b, protoObj); err != nil {
			errChan <- NewReadError("Error while reading protobuf entity", path, lineNo)
			lineNo += 1
			continue
		}

		jsonObj, err := convertProtoToJSON(protoObj)
		if err != nil {
			errChan <- NewReadError("Error while parsing protobuf entity", path, lineNo)
			lineNo += 1
			continue
		}

		jsonChan <- jsonObj
		lineNo += 1

	}

	return nil
}

// converts protobuf entity to a generic json struct
func convertProtoToJSON(protoObj *pb.EntityProto) (*map[string]interface{}, error) {

	// extract the key first
	key, err := google.ProtoToKey(protoObj.GetKey())

	if err != nil {
		return nil, err
	}

	destJSON := make(map[string]interface{})

	destJSON["key"] = mkKeyJson(*key)

	allProperties, err := google.ProtoToProperties(protoObj)

	if err != nil {
		return nil, err
	}

	for _, prop := range allProperties {
		jsonVal := protoValueToJsonValue(&prop)

		// pass a pointer and update the JSON object
		// TODO check memory perf of passing destJSON as value vs passing a pointer
		setJSONValue(&destJSON, prop.Name, &jsonVal, prop.Multiple)
	}

	return &destJSON, nil

}

// Datastore keys are converted to JSON objects with "id" and "kind" fields
func mkKeyJson(key google.Key) *map[string]interface{} {
	keyJson := make(map[string]interface{})
	if key.IntID() != 0 {
		keyJson["id"] = key.IntID()
	} else if key.StringID() != "" {
		keyJson["id"] = key.StringID()
	}
	keyJson["kind"] = key.Kind()
	return &keyJson
}

// converts a protobuf property to a serializable value
func protoValueToJsonValue(prop *google.Property) interface{} {

	switch prop.Value.(type) {
	case int, int64, float64, bool:
		return prop.Value
	case string:
		nestedStruct := &pb.EntityProto{}
		err := proto.Unmarshal([]byte(prop.Value.(string)), nestedStruct)
		if err != nil {
			return prop.Value
		} else {
			nestedJson, err := convertProtoToJSON(nestedStruct)
			if err != nil {
				return prop.Value
			} else {
				return nestedJson
			}
		}

	case *google.Key:
		return mkKeyJson(*prop.Value.(*google.Key))
	case []byte:
		nestedJson, err := parseJson(string(prop.Value.([]byte)))
		if err != nil {
			return nil
		}
		return nestedJson
	case time.Time:
		timeVal := prop.Value.(time.Time)
		return timeVal.UnixNano() / int64(time.Millisecond)
	case appengine.GeoPoint:
		geo := prop.Value.(appengine.GeoPoint)
		geoJson := map[string]float64{
			"lat": geo.Lat,
			"lon": geo.Lng,
		}
		return geoJson
	case nil:
		return nil
	default:
		log.Printf("YET TO HANDLE key: %s type: %s %v", prop.Name, reflect.TypeOf(prop.Value), prop.Value)
		return nil
	}

	return nil
}

// parses a JSON string into a generic struct or list of generic structs
func parseJson(jsonStr string) (interface{}, error) {

	if jsonStr[0] == '[' {
		nestedJson := make([]map[string]interface{}, 0)
		err := json.Unmarshal([]byte(jsonStr), &nestedJson)
		if err != nil {
			return nil, err
		}
		return nestedJson, nil
	} else {
		nestedJson := make(map[string]interface{})
		err := json.Unmarshal([]byte(jsonStr), &nestedJson)
		if err != nil {
			return nil, err
		}
		return nestedJson, nil
	}
}

// modifies a dict inplace to update JSON values
func setJSONValue(ptr *map[string]interface{}, key string, value interface{}, multiple bool) map[string]interface{} {
	data := *ptr
	existing, ok := data[key]

	if !ok && multiple {
		// create a new list, if it's a repeated field
		data[key] = []interface{}{value}

	} else if ok && multiple {
		// append to existing list
		data[key] = append(existing.([]interface{}), value)
	} else {
		// just set for all other cases
		data[key] = value
	}

	return data
}
