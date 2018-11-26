# Datastore Reader

Utility written in Go to read & convert Google Datastore backups to JSON files.

## Build

To create an executable run:

```
make build
```

This will create `ds_to_json` executable at `$GOPATH/bin`

## Usage

To run the script:

```
ds_to_json {entity backup folder path} {output folder path}
```

## Assumptions
- If your output folder does not exist, the script will create it. Ensure that the script has write access to the path
- CAUTION If the output folder already has content, this script will delete all files in it. This will be changed in future, to make it an optional flag
- The script uses workers to parallely convert files to JSON.
- The JSON output files will be of format `converted-{x}.json`
- Any errors in converting will be written to `errors-{x}.json` files

## TODO
- [ ] Add ability to pass args as named cli args
- [ ] Make clearing/deleting an optional flag
- [ ] Show a progress bar instead of showing logs
- [ ] Make logging optional/verbose
- [ ] Make number of workers configurable


### External Code
- `google` - this is subset of App Engine's Go library that can read backup files, which store entities in protobuf format - https://github.com/golang/appengine
- `leveldb/journal` - This part of the code is used to read the backfile. https://github.com/syndtr/goleveldb/blob/master/leveldb/journal/journal.go
